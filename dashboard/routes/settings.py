# dashboard/routes/settings.py

import logging
import re
from flask import (
    Blueprint, render_template, request, flash, redirect, url_for
)
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError
from bson import ObjectId # For converting string IDs back to ObjectId
from ..extensions import build_rules_collection, course_config_collection

# Define Blueprint
bp = Blueprint('settings', __name__, url_prefix='/settings')
logger = logging.getLogger('dashboard.routes.settings')

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

    # --- Get Filter Parameters ---
    filter_vendor = request.args.get('filter_vendor', '').strip()
    filter_course_name = request.args.get('filter_course_name', '').strip()

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

    if course_config_collection is not None:
        try:
            configs_cursor = course_config_collection.find(mongo_query).sort(
                [("vendor_shortcode", ASCENDING), ("course_name", ASCENDING)]
            )
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

    return render_template(
        'settings_course_configs.html',
        configs=configs,
        current_filters=current_filters, # Pass filters to template
        current_theme=current_theme
    )