# dashboard/routes/settings.py

import logging
import re
import math, json, pytz, datetime
from flask import (
    Blueprint, render_template, request, flash, redirect, url_for
)
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError
from bson import ObjectId # For converting string IDs back to ObjectId
from bson.errors import InvalidId
from ..extensions import build_rules_collection, course_config_collection, trainer_email_collection

# Define Blueprint
bp = Blueprint('settings', __name__, url_prefix='/settings')
logger = logging.getLogger('dashboard.routes.settings')


@bp.route('/trainer-emails')
def view_trainer_emails():
    """Displays the trainer emails management page."""
    current_theme = request.cookies.get('theme', 'light')
    trainers = []
    if trainer_email_collection is not None:
        try:
            trainers_cursor = trainer_email_collection.find().sort("trainer_name", ASCENDING)
            trainers = list(trainers_cursor)
            for trainer in trainers:
                trainer['_id'] = str(trainer['_id']) # For template usage
        except PyMongoError as e:
            logger.error(f"Error fetching trainer emails: {e}")
            flash("Error fetching trainer emails from the database.", "danger")
    else:
        flash("Trainer emails collection is unavailable.", "danger")
    
    return render_template(
        'settings_trainer_emails.html',
        trainers=trainers,
        current_theme=current_theme
    )

@bp.route('/trainer-emails/add', methods=['POST'])
def add_trainer_email():
    """Adds a new trainer email record."""
    if trainer_email_collection is None:
        flash("Database collection for trainer emails is unavailable.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    trainer_name = request.form.get('trainer_name', '').strip()
    email_address = request.form.get('email_address', '').strip()
    # 'active' will be 'on' if checked, or not present if unchecked
    active = 'active' in request.form

    if not trainer_name or not email_address:
        flash("Trainer Name and Email Address are required.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    try:
        # Check for duplicate trainer name
        if trainer_email_collection.count_documents({"trainer_name": trainer_name}) > 0:
            flash(f"A trainer with the name '{trainer_name}' already exists.", "warning")
            return redirect(url_for('.view_trainer_emails'))
        
        new_trainer_doc = {
            "trainer_name": trainer_name,
            "email_address": email_address,
            "active": active
        }
        trainer_email_collection.insert_one(new_trainer_doc)
        flash(f"Successfully added trainer '{trainer_name}'.", "success")
    except PyMongoError as e:
        logger.error(f"Database error adding trainer email: {e}")
        flash("A database error occurred while adding the trainer.", "danger")

    return redirect(url_for('.view_trainer_emails'))

@bp.route('/trainer-emails/update/<trainer_id>', methods=['POST'])
def update_trainer_email(trainer_id):
    """Updates an existing trainer email record."""
    if trainer_email_collection is None:
        flash("Database collection for trainer emails is unavailable.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    try:
        trainer_oid = ObjectId(trainer_id)
    except InvalidId:
        flash("Invalid trainer ID provided.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    trainer_name = request.form.get('trainer_name', '').strip()
    email_address = request.form.get('email_address', '').strip()
    active = 'active' in request.form

    if not trainer_name or not email_address:
        flash("Trainer Name and Email Address cannot be empty.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    try:
        # Check if the new name conflicts with another existing trainer
        existing_trainer = trainer_email_collection.find_one({
            "trainer_name": trainer_name,
            "_id": {"$ne": trainer_oid} # Look for a different document with the same name
        })
        if existing_trainer:
            flash(f"Cannot rename to '{trainer_name}' as another trainer with that name already exists.", "danger")
            return redirect(url_for('.view_trainer_emails'))

        update_doc = {
            "$set": {
                "trainer_name": trainer_name,
                "email_address": email_address,
                "active": active
            }
        }
        result = trainer_email_collection.update_one({"_id": trainer_oid}, update_doc)

        if result.modified_count > 0:
            flash(f"Successfully updated trainer '{trainer_name}'.", "success")
        else:
            flash(f"No changes detected for trainer '{trainer_name}'.", "info")
            
    except PyMongoError as e:
        logger.error(f"Database error updating trainer email {trainer_id}: {e}")
        flash("A database error occurred while updating the trainer.", "danger")
        
    return redirect(url_for('.view_trainer_emails'))

@bp.route('/trainer-emails/delete/<trainer_id>', methods=['POST'])
def delete_trainer_email(trainer_id):
    """Deletes a trainer email record."""
    if trainer_email_collection is None:
        flash("Database collection for trainer emails is unavailable.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    try:
        trainer_oid = ObjectId(trainer_id)
    except InvalidId:
        flash("Invalid trainer ID provided for deletion.", "danger")
        return redirect(url_for('.view_trainer_emails'))

    try:
        result = trainer_email_collection.delete_one({"_id": trainer_oid})
        if result.deleted_count == 1:
            flash("Trainer email record deleted successfully.", "success")
        else:
            flash("Trainer record not found for deletion.", "warning")
    except PyMongoError as e:
        logger.error(f"Database error deleting trainer email {trainer_id}: {e}")
        flash("A database error occurred while deleting the trainer.", "danger")

    return redirect(url_for('.view_trainer_emails'))

# --- Route to Display Build Rules ---
@bp.route('/build-rules')
def view_build_rules():
    """Displays the build rules management page."""
    current_theme = request.cookies.get('theme', 'light')
    rules = []
    if build_rules_collection is not None:
        try:
            # Fetch rules sorted by priority
            rules_cursor = build_rules_collection.find().sort("priority", ASCENDING)
            rules = list(rules_cursor)
            # Convert ObjectId to string for template if needed (or use custom JSON encoder)
            for rule in rules:
                rule['_id'] = str(rule['_id'])
        except PyMongoError as e:
            logger.error(f"Error fetching build rules: {e}")
            flash("Error fetching build rules from database.", "danger")
        except Exception as e:
             logger.error(f"Unexpected error fetching rules: {e}", exc_info=True)
             flash("An unexpected error occurred.", "danger")
    else:
        flash("Build rules collection is unavailable.", "danger")

    return render_template(
        'settings_build_rules.html',
        rules=rules,
        current_theme=current_theme
    )

@bp.route('/course-configs')
def view_course_configs():
    """Displays the course configurations management page with filtering."""
    current_theme = request.cookies.get('theme', 'light')
    configs = []

    # --- Get Filter & Pagination Parameters ---
    filter_vendor = request.args.get('filter_vendor', '').strip()
    filter_course_name = request.args.get('filter_course_name', '').strip()

    try:
        page = int(request.args.get('page', 1))
        if page < 1: page = 1
    except ValueError:
        page = 1

    try:
        # Allow user to select results per page, default to 10
        # Ensure it's a reasonable number
        per_page = int(request.args.get('per_page', 10))
        if per_page not in [10, 25, 50, 100]: per_page = 10
    except ValueError:
        per_page = 10

    # --- Build MongoDB Query ---
    mongo_query = {}
    if filter_vendor:
        # Exact match for vendor (case-insensitive)
        mongo_query['vendor_shortcode'] = {'$regex': f'^{re.escape(filter_vendor)}$', '$options': 'i'}
    if filter_course_name:
        # Contains match for course name (case-insensitive)
        mongo_query['course_name'] = {'$regex': re.escape(filter_course_name), '$options': 'i'}

    # --- Store current filters for template ---
    current_filters = {
        'filter_vendor': filter_vendor,
        'filter_course_name': filter_course_name
    }

    # --- Store current filters and per_page for template ---
    current_params = {
        'filter_vendor': filter_vendor,
        'filter_course_name': filter_course_name,
        'per_page': per_page
        # 'page' will be added by pagination macro/logic in template
    }
    total_configs = 0

    if course_config_collection is not None:
        try:
            # --- Get Total Count for Pagination (before applying skip/limit) ---
            total_configs = course_config_collection.count_documents(mongo_query)

            # --- Calculate Skip and Fetch Paginated Results ---
            skip_amount = (page - 1) * per_page
            configs_cursor = course_config_collection.find(mongo_query).sort(
                [("vendor_shortcode", ASCENDING), ("course_name", ASCENDING)]
            ).skip(skip_amount).limit(per_page)

            configs = list(configs_cursor)
            for config in configs:
                config['_id'] = str(config['_id'])
        except PyMongoError as e:
            logger.error(f"Error fetching course configurations: {e}")
            flash("Error fetching course configurations from database.", "danger")
        except Exception as e:
             logger.error(f"Unexpected error fetching course configs: {e}", exc_info=True)
             flash("An unexpected error occurred.", "danger")
    else:
        flash("Course configurations collection is unavailable.", "danger")

    # Calculate total pages
    total_pages = math.ceil(total_configs / per_page) if per_page > 0 else 1
    # Ensure current page is not out of bounds after calculation
    page = max(1, min(page, total_pages))


    return render_template(
        'settings_course_configs.html',
        configs=configs,
        current_params=current_params, # Pass all current params for filter form and pagination links
        current_page=page,
        total_pages=total_pages,
        total_configs=total_configs,
        per_page_options=[10, 25, 50, 100], # Options for dropdown
        current_theme=current_theme
    )

@bp.route('/build-rules/add', methods=['POST'])
def add_build_rule():
    """Adds a new build rule to the collection."""
    if build_rules_collection is None:
        flash("Build rules database collection is unavailable.", "danger")
        return redirect(url_for('settings.view_build_rules')) # Redirect to settings blueprint to view rules

    try:
        # Extract data from the form
        rule_name = request.form.get('rule_name', '').strip()
        priority_str = request.form.get('priority', '').strip()
        conditions_json = request.form.get('conditions', '{}').strip() # Default to empty JSON string
        actions_json = request.form.get('actions', '{}').strip()   # Default to empty JSON string

        # --- Basic Validation ---
        if not rule_name:
            flash("Rule Name is required.", "danger")
            return redirect(url_for('settings.view_build_rules'))
        
        try:
            priority = int(priority_str)
            if priority < 1: # Priority should be positive
                raise ValueError("Priority must be a positive integer.")
        except (ValueError, TypeError):
             flash("Priority must be a positive integer.", "danger")
             return redirect(url_for('settings.view_build_rules'))
        
        try:
            conditions = json.loads(conditions_json)
            if not isinstance(conditions, dict):
                raise ValueError("Conditions field must contain a valid JSON object.")
        except (json.JSONDecodeError, ValueError) as e:
             flash(f"Conditions field contains invalid JSON: {e}", "danger")
             return redirect(url_for('settings.view_build_rules'))
        
        try:
            actions = json.loads(actions_json)
            if not isinstance(actions, dict):
                raise ValueError("Actions field must contain a valid JSON object.")
        except (json.JSONDecodeError, ValueError) as e:
             flash(f"Actions field contains invalid JSON: {e}", "danger")
             return redirect(url_for('settings.view_build_rules'))

        # Prepare the document to be inserted
        new_rule = {
            "rule_name": rule_name,
            "priority": priority,
            "conditions": conditions,
            "actions": actions,
            "created_at": datetime.datetime.now(pytz.utc) # Add a creation timestamp in UTC
            # "updated_at": datetime.datetime.now(pytz.utc) # Optionally add updated_at too
        }

        # Insert into the database
        result = build_rules_collection.insert_one(new_rule)
        
        # Log success and flash message
        logger.info(f"Added new build rule '{rule_name}' (Priority: {priority}) with ID: {result.inserted_id}")
        flash(f"Successfully added build rule '{rule_name}'.", "success")

    except PyMongoError as e:
        logger.error(f"Database error occurred while adding build rule: {e}")
        flash("A database error occurred while adding the rule. Please try again.", "danger")
    except Exception as e:
        logger.error(f"An unexpected error occurred while adding build rule: {e}", exc_info=True)
        flash("An unexpected error occurred. Please check the logs.", "danger")

    return redirect(url_for('settings.view_build_rules')) # Redirect back to the rules

@bp.route('/build-rules/update', methods=['POST'])
def update_build_rule():
    """Updates an existing build rule."""
    if build_rules_collection is None:
        flash("Build rules database collection is unavailable.", "danger")
        return redirect(url_for('settings.view_build_rules'))

    rule_id_str = request.form.get('rule_id')
    if not rule_id_str:
        flash("Rule ID missing for update.", "danger")
        return redirect(url_for('settings.view_build_rules'))

    try:
        rule_oid = ObjectId(rule_id_str) # Convert string ID to ObjectId
    except InvalidId:
        flash("Invalid Rule ID format.", "danger")
        return redirect(url_for('settings.view_build_rules'))

    try:
        rule_name = request.form.get('rule_name', '').strip()
        priority_str = request.form.get('priority', '').strip()
        conditions_json = request.form.get('conditions', '{}').strip()
        actions_json = request.form.get('actions', '{}').strip()

        # Validation (similar to add)
        if not rule_name: raise ValueError("Rule Name is required.")
        try: priority = int(priority_str); assert priority >= 1
        except: raise ValueError("Priority must be a positive integer.")
        try: conditions = json.loads(conditions_json); assert isinstance(conditions, dict)
        except: raise ValueError("Conditions field contains invalid JSON.")
        try: actions = json.loads(actions_json); assert isinstance(actions, dict)
        except: raise ValueError("Actions field contains invalid JSON.")

        # Prepare update document
        update_doc = {
            "$set": {
                "rule_name": rule_name,
                "priority": priority,
                "conditions": conditions,
                "actions": actions,
                "updated_at": datetime.datetime.now(pytz.utc) # Add update timestamp
            }
        }

        # Perform update
        result = build_rules_collection.update_one({"_id": rule_oid}, update_doc)

        if result.matched_count == 0:
            flash(f"Rule with ID {rule_id_str} not found.", "warning")
        elif result.modified_count == 0:
             flash(f"Rule '{rule_name}' was not modified (no changes detected).", "info")
        else:
            logger.info(f"Updated build rule '{rule_name}' (ID: {rule_id_str})")
            flash(f"Successfully updated build rule '{rule_name}'.", "success")

    except ValueError as ve: # Catch validation errors
        flash(f"Invalid input: {ve}", "danger")
    except PyMongoError as e:
        logger.error(f"Database error updating build rule {rule_id_str}: {e}")
        flash("Database error updating rule.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error updating build rule {rule_id_str}: {e}", exc_info=True)
        flash("An unexpected error occurred while updating.", "danger")

    return redirect(url_for('settings.view_build_rules'))

@bp.route('/build-rules/delete/<rule_id>', methods=['POST'])
def delete_build_rule(rule_id):
    """Deletes a build rule by its ID."""
    if build_rules_collection is None:
        flash("Build rules database collection is unavailable.", "danger")
        return redirect(url_for('settings.view_build_rules'))

    try:
        rule_oid = ObjectId(rule_id) # Convert string ID from URL
    except InvalidId:
        flash("Invalid Rule ID format for deletion.", "danger")
        return redirect(url_for('settings.view_build_rules'))

    try:
        result = build_rules_collection.delete_one({"_id": rule_oid})

        if result.deleted_count == 1:
            logger.info(f"Deleted build rule with ID: {rule_id}")
            flash("Successfully deleted build rule.", "success")
        else:
            flash(f"Rule with ID {rule_id} not found for deletion.", "warning")

    except PyMongoError as e:
        logger.error(f"Database error deleting build rule {rule_id}: {e}")
        flash("Database error deleting rule.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error deleting build rule {rule_id}: {e}", exc_info=True)
        flash("An unexpected error occurred during deletion.", "danger")

    return redirect(url_for('settings.view_build_rules'))

@bp.route('/course-configs/add', methods=['POST'])
def add_course_config():
    """Adds a new course configuration."""
    if course_config_collection is None:
        flash("Course config collection unavailable.", "danger")
        return redirect(url_for('settings.view_course_configs')) # Redirect to settings blueprint

    config_json_str = request.form.get('config_json', '{}').strip()
    try:
        new_config_data = json.loads(config_json_str)
        if not isinstance(new_config_data, dict):
            raise ValueError("Configuration must be a valid JSON object.")

        # Validate required fields
        course_name = new_config_data.get('course_name')
        vendor_shortcode = new_config_data.get('vendor_shortcode')
        if not course_name or not isinstance(course_name, str) or not course_name.strip():
            flash("Valid 'course_name' (string) is required in the JSON.", "danger")
            return redirect(url_for('settings.view_course_configs'))
        if not vendor_shortcode or not isinstance(vendor_shortcode, str) or not vendor_shortcode.strip():
            flash("Valid 'vendor_shortcode' (string) is required in the JSON.", "danger")
            return redirect(url_for('settings.view_course_configs'))

        new_config_data['course_name'] = course_name.strip() # Ensure trimmed
        new_config_data['vendor_shortcode'] = vendor_shortcode.strip().lower() # Trim & lowercase vendor

        # Add created_at timestamp
        new_config_data['created_at'] = datetime.datetime.now(pytz.utc)

        # Attempt to insert
        # Consider adding a unique index on (course_name, vendor_shortcode) in MongoDB
        # to prevent exact duplicates if course_name itself isn't globally unique.
        # For now, let's assume course_name should be unique.
        if course_config_collection.count_documents({"course_name": new_config_data['course_name']}) > 0:
            flash(f"Course configuration with name '{new_config_data['course_name']}' already exists.", "warning")
            return redirect(url_for('settings.view_course_configs'))

        result = course_config_collection.insert_one(new_config_data)
        logger.info(f"Added new course config '{new_config_data['course_name']}' with ID: {result.inserted_id}")
        flash(f"Successfully added course configuration '{new_config_data['course_name']}'.", "success")

    except json.JSONDecodeError:
        flash("Invalid JSON format for configuration.", "danger")
    except ValueError as ve: # Catch our custom validation errors
        flash(str(ve), "danger")
    except PyMongoError as e:
        logger.error(f"Database error adding course config: {e}")
        if e.code == 11000: # Duplicate key error
             flash(f"A course configuration with that name or key combination already exists.", "danger")
        else:
             flash("Database error adding configuration.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error adding course config: {e}", exc_info=True)
        flash("An unexpected error occurred.", "danger")

    return redirect(url_for('settings.view_course_configs'))

@bp.route('/course-configs/update', methods=['POST'])
def update_course_config():
    """Updates an existing course configuration."""
    if course_config_collection is None:
        flash("Course config collection unavailable.", "danger")
        return redirect(url_for('settings.view_course_configs'))

    config_id_str = request.form.get('config_id')
    config_json_str = request.form.get('config_json', '{}').strip()

    if not config_id_str:
        flash("Configuration ID missing for update.", "danger")
        return redirect(url_for('settings.view_course_configs'))
    try:
        config_oid = ObjectId(config_id_str)
    except InvalidId:
        flash("Invalid Configuration ID format.", "danger")
        return redirect(url_for('settings.view_course_configs'))

    try:
        updated_config_data = json.loads(config_json_str)
        if not isinstance(updated_config_data, dict):
            raise ValueError("Configuration must be a valid JSON object.")

        # Validate required fields are still present and valid
        course_name = updated_config_data.get('course_name')
        vendor_shortcode = updated_config_data.get('vendor_shortcode')
        if not course_name or not isinstance(course_name, str) or not course_name.strip():
            raise ValueError("Valid 'course_name' (string) is required.")
        if not vendor_shortcode or not isinstance(vendor_shortcode, str) or not vendor_shortcode.strip():
            raise ValueError("Valid 'vendor_shortcode' (string) is required.")

        updated_config_data['course_name'] = course_name.strip()
        updated_config_data['vendor_shortcode'] = vendor_shortcode.strip().lower()

        # Add updated_at timestamp
        updated_config_data['updated_at'] = datetime.datetime.now(pytz.utc)

        # Remove _id from update data if it was accidentally included from textarea
        updated_config_data.pop('_id', None)

        # Check for name collision if course_name is being changed to an existing one
        # (and it's not the current document being edited)
        existing_with_new_name = course_config_collection.find_one({
            "course_name": updated_config_data['course_name'],
            "_id": {"$ne": config_oid}
        })
        if existing_with_new_name:
             flash(f"Another course configuration with the name '{updated_config_data['course_name']}' already exists.", "danger")
             return redirect(url_for('settings.view_course_configs'))


        result = course_config_collection.update_one(
            {"_id": config_oid},
            {"$set": updated_config_data}
        )

        if result.matched_count == 0:
            flash(f"Course configuration with ID {config_id_str} not found.", "warning")
        elif result.modified_count == 0:
             flash(f"Course configuration '{updated_config_data['course_name']}' was not modified (no changes detected or attempt to change to existing name).", "info")
        else:
            logger.info(f"Updated course config '{updated_config_data['course_name']}' (ID: {config_id_str})")
            flash(f"Successfully updated course configuration '{updated_config_data['course_name']}'.", "success")

    except json.JSONDecodeError:
        flash("Invalid JSON format for configuration.", "danger")
    except ValueError as ve:
        flash(str(ve), "danger")
    except PyMongoError as e:
        logger.error(f"Database error updating course config {config_id_str}: {e}")
        if e.code == 11000: # Duplicate key error
             flash(f"Update failed: A course configuration with the new name or key combination may already exist.", "danger")
        else:
            flash("Database error updating configuration.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error updating course config {config_id_str}: {e}", exc_info=True)
        flash("An unexpected error occurred.", "danger")

    return redirect(url_for('settings.view_course_configs'))

@bp.route('/course-configs/delete/<config_id>', methods=['POST'])
def delete_course_config(config_id):
    """Deletes a course configuration by its ID."""
    if course_config_collection is None:
        flash("Course config collection unavailable.", "danger")
        return redirect(url_for('settings.view_course_configs'))

    try:
        config_oid = ObjectId(config_id)
    except InvalidId:
        flash("Invalid Configuration ID format for deletion.", "danger")
        return redirect(url_for('settings.view_course_configs'))

    try:
        result = course_config_collection.delete_one({"_id": config_oid})
        if result.deleted_count == 1:
            logger.info(f"Deleted course configuration with ID: {config_id}")
            flash("Successfully deleted course configuration.", "success")
        else:
            flash(f"Course configuration with ID {config_id} not found for deletion.", "warning")
    except PyMongoError as e:
        logger.error(f"Database error deleting course config {config_id}: {e}")
        flash("Database error deleting configuration.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error deleting course config {config_id}: {e}", exc_info=True)
        flash("An unexpected error occurred during deletion.", "danger")

    return redirect(url_for('settings.view_course_configs'))