# dashboard/routes/settings.py

import logging
from flask import (
    Blueprint, render_template, request, flash, redirect, url_for
)
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError
from bson import ObjectId # For converting string IDs back to ObjectId
from ..extensions import build_rules_collection # Import the collection

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

# Action routes for add/edit/delete will be in actions.py or here later.
# Let's keep display logic separate first.

# --- END OF dashboard/routes/settings.py ---