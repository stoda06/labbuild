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

def _reconstruct_rule_from_form(form_data):
    """Parses flat form data from the build rule form into a nested Python dictionary."""
    rule = {
        "conditions": {},
        "actions": {}
    }

    # Simple top-level fields
    rule['rule_name'] = form_data.get('rule_name', '').strip()
    try:
        rule['priority'] = int(form_data.get('priority'))
    except (ValueError, TypeError):
        rule['priority'] = 99 # Default priority on error
        
    # --- Conditions ---
    if form_data.get('conditions.vendor'):
        rule['conditions']['vendor'] = form_data.get('conditions.vendor').strip().lower()
    
    # Handle comma-separated string lists
    for key in ['course_code_contains', 'course_type_contains', 'course_code_not_contains']:
        value = form_data.get(f'conditions.{key}')
        if value:
            rule['conditions'][key] = [item.strip() for item in value.split(',') if item.strip()]

    # --- Actions ---
    if form_data.get('actions.set_labbuild_course'):
        rule['actions']['set_labbuild_course'] = form_data.get('actions.set_labbuild_course').strip()
    
    rule['actions']['allow_spillover'] = 'actions.allow_spillover' in form_data

    if form_data.get('actions.start_pod_number'):
        try: rule['actions']['start_pod_number'] = int(form_data.get('actions.start_pod_number'))
        except (ValueError, TypeError): pass
        
    if form_data.get('actions.set_max_pods'):
        try: rule['actions']['set_max_pods'] = int(form_data.get('actions.set_max_pods'))
        except (ValueError, TypeError): pass

    if form_data.get('actions.host_priority'):
        rule['actions']['host_priority'] = [item.strip() for item in form_data.get('actions.host_priority').split(',') if item.strip()]

    # --- Handle nested objects (calculate_pods_from_pax and maestro_split_build) ---
    if form_data.get('actions.calculate_pods_from_pax.enabled') == 'on':
        calc_pods = {}
        try:
            calc_pods['divisor'] = int(form_data.get('actions.calculate_pods_from_pax.divisor'))
            calc_pods['min_pods'] = int(form_data.get('actions.calculate_pods_from_pax.min_pods'))
            calc_pods['use_field'] = form_data.get('actions.calculate_pods_from_pax.use_field')
            if all(k in calc_pods for k in ['divisor', 'min_pods', 'use_field']):
                 rule['actions']['calculate_pods_from_pax'] = calc_pods
        except (ValueError, TypeError):
            logger.warning("Could not parse 'calculate_pods_from_pax' fields.")

    if form_data.get('actions.maestro_split_build.enabled') == 'on':
        maestro = {}
        maestro['main_course'] = form_data.get('actions.maestro_split_build.main_course')
        maestro['rack1_course'] = form_data.get('actions.maestro_split_build.rack1_course')
        maestro['rack2_course'] = form_data.get('actions.maestro_split_build.rack2_course')
        rack_host_str = form_data.get('actions.maestro_split_build.rack_host')
        if rack_host_str:
            maestro['rack_host'] = [h.strip() for h in rack_host_str.split(',') if h.strip()]
        
        if all(k in maestro for k in ['main_course', 'rack1_course', 'rack2_course', 'rack_host']):
            rule['actions']['maestro_split_build'] = maestro

    # Clean up empty sub-dictionaries
    if not rule['conditions']: del rule['conditions']
    if not rule['actions']: del rule['actions']
    
    return rule

# --- Helper to reconstruct nested dict from form data ---
def _reconstruct_config_from_form(form_data):
    """
    Parses flat form data with names like 'components[0][name]'
    into a nested Python dictionary that matches the MongoDB schema.
    """
    config = {}
    
    # Regex to parse field names like: main_key[index][sub_key]
    # e.g., components[0][component_name] or networks[0][port_groups][0][port_group_name]
    pattern = re.compile(r'(\w+)(?:\[(\d+)\])?(?:\[(\w+)\])?(?:\[(\d+)\])?(?:\[(\w+)\])?')

    # Separate simple fields from structured (array/object) fields
    for key, value in form_data.items():
        if not value: continue # Skip empty fields

        match = pattern.match(key)
        if not match:
            if key not in ['config_id']: # Ignore hidden fields for the main doc
                config[key] = value
            continue

        parts = [p for p in match.groups() if p is not None]
        
        # This part handles building the nested structure
        # It's complex because it supports up to two levels of nested arrays
        d = config
        for i, part in enumerate(parts[:-1]):
            # if part is a number, it's an index for a list
            if part.isdigit():
                idx = int(part)
                # Ensure the list is long enough
                while len(d) <= idx:
                    d.append({})
                # Move deeper into the list
                d = d[idx]
            else: # It's a key for a dictionary
                # For lists, d is the list itself. we need to operate on its dict items.
                if isinstance(d, list):
                    # This case handles something like networks[0][port_groups]
                    # The parent is a list, and the key is a new list inside the dict item
                    parent_dict = d[-1] if d else {}
                    d = parent_dict.setdefault(part, [])
                else:
                    d = d.setdefault(part, [])

        # The last part is the final key and its value
        final_key = parts[-1]
        
        # If the parent is a list, we need to add a new dictionary to it
        if isinstance(d, list):
             # This is for the deepest level, e.g., a port_group item
            if len(d) <= int(parts[-2]) if len(parts) > 1 and parts[-2].isdigit() else 0:
                 d.append({})
            
            # Try to convert value to int if it's a numeric key
            try:
                if final_key in ['vlan_id', 'memory']:
                    value = int(value)
            except (ValueError, TypeError):
                pass # Keep as string if conversion fails
                
            d[-1][final_key] = value
        else:
            d[final_key] = value

    # Clean up any empty lists that might have been created
    for key in list(config.keys()):
        if isinstance(config[key], list) and not any(config[key]):
            del config[key]
            
    return config

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
    """Adds a new build rule from the structured form."""
    if build_rules_collection is None:
        flash("Build rules collection is unavailable.", "danger")
        return redirect(url_for('.view_build_rules'))

    try:
        new_rule = _reconstruct_rule_from_form(request.form)
        if not new_rule.get('rule_name'): raise ValueError("Rule Name is required.")
        
        new_rule["created_at"] = datetime.datetime.now(pytz.utc)
        result = build_rules_collection.insert_one(new_rule)
        
        logger.info(f"Added new build rule '{new_rule['rule_name']}' with ID: {result.inserted_id}")
        flash(f"Successfully added build rule '{new_rule['rule_name']}'.", "success")

    except ValueError as ve:
        flash(f"Validation Error: {ve}", "danger")
    except PyMongoError as e:
        logger.error(f"DB error adding build rule: {e}"); flash("DB error adding rule.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error adding rule: {e}", exc_info=True); flash("An unexpected error occurred.", "danger")

    return redirect(url_for('.view_build_rules'))


@bp.route('/build-rules/update', methods=['POST'])
def update_build_rule():
    """Updates an existing build rule from the structured form."""
    if build_rules_collection is None:
        flash("Build rules collection is unavailable.", "danger")
        return redirect(url_for('.view_build_rules'))

    rule_id_str = request.form.get('rule_id')
    try: rule_oid = ObjectId(rule_id_str)
    except InvalidId: flash("Invalid Rule ID.", "danger"); return redirect(url_for('.view_build_rules'))

    try:
        updated_rule = _reconstruct_rule_from_form(request.form)
        if not updated_rule.get('rule_name'): raise ValueError("Rule Name is required.")
        
        updated_rule["updated_at"] = datetime.datetime.now(pytz.utc)
        
        # We replace the document to handle removed keys correctly
        # Preserve created_at timestamp
        original_doc = build_rules_collection.find_one({"_id": rule_oid}, {"created_at": 1})
        if original_doc and original_doc.get('created_at'):
            updated_rule['created_at'] = original_doc['created_at']
            
        result = build_rules_collection.replace_one({"_id": rule_oid}, updated_rule)

        if result.modified_count > 0:
            flash(f"Successfully updated rule '{updated_rule['rule_name']}'.", "success")
        else:
            flash(f"No changes detected for rule '{updated_rule['rule_name']}'.", "info")

    except ValueError as ve:
        flash(f"Validation Error: {ve}", "danger")
    except PyMongoError as e:
        logger.error(f"DB error updating rule {rule_id_str}: {e}"); flash("DB error updating rule.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error updating rule {rule_id_str}: {e}", exc_info=True); flash("An unexpected error occurred.", "danger")

    return redirect(url_for('.view_build_rules'))

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
    """Adds a new course configuration from the structured form."""
    if course_config_collection is None:
        flash("Course config collection unavailable.", "danger")
        return redirect(url_for('.view_course_configs'))

    try:
        # Reconstruct the nested dictionary from the form data
        new_config_data = _reconstruct_config_from_form(request.form)

        course_name = new_config_data.get('course_name')
        if not course_name: raise ValueError("'Course Name' is required.")
        if not new_config_data.get('vendor_shortcode'): raise ValueError("'Vendor Shortcode' is required.")

        # Check for duplicates
        if course_config_collection.count_documents({"course_name": course_name}) > 0:
            flash(f"Course configuration with name '{course_name}' already exists.", "warning")
            return redirect(url_for('.view_course_configs'))

        new_config_data['created_at'] = datetime.datetime.now(pytz.utc)
        result = course_config_collection.insert_one(new_config_data)
        
        logger.info(f"Added new course config '{course_name}' with ID: {result.inserted_id}")
        flash(f"Successfully added course config '{course_name}'.", "success")

    except ValueError as ve:
        flash(f"Validation Error: {ve}", "danger")
    except PyMongoError as e:
        logger.error(f"DB error adding course config: {e}"); flash("Database error adding configuration.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error adding course config: {e}", exc_info=True); flash("An unexpected error occurred.", "danger")

    return redirect(url_for('.view_course_configs'))

@bp.route('/course-configs/update', methods=['POST'])
def update_course_config():
    """Updates an existing course configuration from the structured form."""
    if course_config_collection is None:
        flash("Course config collection unavailable.", "danger")
        return redirect(url_for('.view_course_configs'))

    config_id_str = request.form.get('config_id')
    try: config_oid = ObjectId(config_id_str)
    except InvalidId: flash("Invalid Configuration ID format.", "danger"); return redirect(url_for('.view_course_configs'))

    try:
        updated_config_data = _reconstruct_config_from_form(request.form)
        course_name = updated_config_data.get('course_name')
        if not course_name: raise ValueError("'Course Name' is required.")
        if not updated_config_data.get('vendor_shortcode'): raise ValueError("'Vendor Shortcode' is required.")
        
        # Check for name collision
        if course_config_collection.count_documents({"course_name": course_name, "_id": {"$ne": config_oid}}) > 0:
            raise ValueError(f"Another course with the name '{course_name}' already exists.")

        updated_config_data['updated_at'] = datetime.datetime.now(pytz.utc)
        
        # Replace the entire document except for fields that should be preserved (like _id, created_at)
        original_doc = course_config_collection.find_one({"_id": config_oid})
        if original_doc and 'created_at' in original_doc:
            updated_config_data['created_at'] = original_doc['created_at']
            
        result = course_config_collection.replace_one({"_id": config_oid}, updated_config_data)

        if result.modified_count > 0:
            flash(f"Successfully updated '{course_name}'.", "success")
        else:
            flash(f"No changes detected for '{course_name}'.", "info")

    except ValueError as ve:
        flash(f"Validation Error: {ve}", "danger")
    except PyMongoError as e:
        logger.error(f"DB error updating course config {config_id_str}: {e}"); flash("Database error updating.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error updating course config {config_id_str}: {e}", exc_info=True); flash("An unexpected error occurred.", "danger")

    return redirect(url_for('.view_course_configs'))

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