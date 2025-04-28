# dashboard/routes/api.py

import logging
import re
import threading
from flask import Blueprint, request, jsonify

# Import extensions and utils from dashboard package
from ..extensions import db, course_config_collection
from ..utils import build_args_from_dict # Helper to build args from JSON
from ..tasks import run_labbuild_task # Task runner

# Define Blueprint
bp = Blueprint('api', __name__, url_prefix='/api') # Prefix all routes with /api
logger = logging.getLogger('dashboard.routes.api')

@bp.route('/courses') # Route becomes /api/courses
def api_courses():
    """Returns course name suggestions based on query and optional vendor."""
    # Logic moved from original app.py
    query = request.args.get('q', '').strip()
    vendor = request.args.get('vendor', '').strip()
    suggestions = []
    if not query or db is None or course_config_collection is None:
        return jsonify(suggestions)
    try:
        mongo_filter = {'course_name': {'$regex': f'^{re.escape(query)}', '$options': 'i'}}
        if vendor: mongo_filter['vendor_shortcode'] = {'$regex': f'^{re.escape(vendor)}$', '$options': 'i'}
        cursor = course_config_collection.find(mongo_filter, {'course_name': 1, '_id': 0}).limit(15)
        suggestions = [doc['course_name'] for doc in cursor if 'course_name' in doc]
    except Exception as e:
        logger.error(f"Error fetching course suggestions: {e}", exc_info=True)
    return jsonify(suggestions)


@bp.route('/v1/labbuild', methods=['POST']) # Route becomes /api/v1/labbuild
def api_run_labbuild():
    """API endpoint to trigger labbuild commands asynchronously."""
    # Logic moved from original app.py
    # TODO: Add Authentication/Authorization
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 415
    data = request.get_json();
    if not data: return jsonify({"error": "No JSON data received"}), 400
    logger.info(f"API request received: {data}")

    # Build and validate arguments using the dict helper
    args_list, error_msg = build_args_from_dict(data)

    if error_msg: logger.error(f"API validation failed: {error_msg}"); return jsonify({"error": f"Invalid input: {error_msg}"}), 400
    if not args_list: logger.error("API argument building failed silently."); return jsonify({"error": "Failed to process arguments."}), 500

    # Execute asynchronously
    try:
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True); thread.start()
        logger.info(f"Submitted API task: {' '.join(args_list)}")
        # TODO: Generate and return a unique run_id here for tracking
        # run_id = ...
        return jsonify({ "status": "submitted", "message": "LabBuild command submitted.", "submitted_command": args_list, #"run_id": run_id
                         }), 202
    except Exception as e: logger.error(f"Failed to start API task thread: {e}", exc_info=True); return jsonify({"error": "Failed to start background task."}), 500

# --- END OF dashboard/routes/api.py ---