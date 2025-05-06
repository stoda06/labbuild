# dashboard/routes/main.py

import re
import math
import logging
from flask import (
    Blueprint, render_template, request, flash, current_app, jsonify, redirect, url_for
)
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import PyMongoError

# Import extensions and utils from the dashboard package
from ..extensions import (
    op_logs_collection, std_logs_collection, course_config_collection,
    host_collection, alloc_collection, scheduler, db, # Need db/collections
    build_rules_collection # NEED RULES COLLECTION AGAIN
)
from ..utils import build_log_filter_query, format_datetime

# Import the Salesforce util that APPLIES RULES
from ..salesforce_utils import get_upcoming_courses_data # USE THIS ONE


# Define Blueprint
bp = Blueprint('main', __name__)
logger = logging.getLogger('dashboard.routes.main')

@bp.route('/')
def index():
    """Main dashboard page."""
    # Logic moved from original app.py
    current_theme = request.cookies.get('theme', 'light')
    vendors, hosts, recent_runs, jobs_display = [], [], [], []

    if db is None:
        flash("Database connection unavailable.", "danger")
        return render_template(
            'index.html', recent_runs=[], jobs=[], vendors=[], hosts=[],
            current_theme=current_theme
        )

    # Fetch Vendors/Hosts
    try:
        if course_config_collection is not None:
            distinct_vendors = course_config_collection.distinct("vendor_shortcode")
            vendors = sorted([v for v in distinct_vendors if v])
        else: flash("Course config collection unavailable.", "warning")
    except PyMongoError as e: logger.error(f"Error fetching vendors: {e}"); flash("Error fetching vendor list.", "danger")
    try:
        if host_collection is not None:
            distinct_hosts = host_collection.distinct("host_name")
            hosts = sorted([h for h in distinct_hosts if h])
        else: flash("Host collection unavailable.", "warning")
    except PyMongoError as e: logger.error(f"Error fetching hosts: {e}"); flash("Error fetching host list.", "danger")

    # Fetch Recent Runs
    try:
        if op_logs_collection is not None:
            cursor = op_logs_collection.find().sort('start_time', DESCENDING).limit(5)
            docs = list(cursor)
            for run in docs:
                run['_id'] = str(run['_id'])
                run['start_time_iso'] = format_datetime(run.get('start_time'))
                run['end_time_iso'] = format_datetime(run.get('end_time'))
                run['args_display'] = { k: str(v) for k, v in run.get('args', {}).items() }
                recent_runs.append(run)
        else: flash("Operation logs collection unavailable.", "warning")
    except PyMongoError as e: logger.error(f"Error fetching operation logs: {e}"); flash("Error fetching recent runs.", "danger")
    except Exception as e: logger.error(f"Error processing recent runs: {e}", exc_info=True); flash("Server error processing recent runs.", "danger")

    # Fetch Scheduled Jobs
    try:
        if scheduler and scheduler.running:
            # Import format_job_args here as it's only used for display in this route
            from ..utils import format_job_args
            import datetime
            import pytz # Need pytz for timezone display formatting

            jobs = scheduler.get_jobs()
            # Ensure sorting handles None next_run_time
            jobs.sort(key=lambda j: j.next_run_time or datetime.datetime.max.replace(tzinfo=pytz.utc))

            for job in jobs:
                trigger_info = 'Unknown'
                next_run_iso = format_datetime(job.next_run_time) # Format for JS

                # Check trigger type (import necessary trigger types)
                from apscheduler.triggers.date import DateTrigger
                from apscheduler.triggers.cron import CronTrigger
                from apscheduler.triggers.interval import IntervalTrigger

                if isinstance(job.trigger, DateTrigger):
                    # Format run_date for display (it should be timezone-aware)
                    run_date_display = format_datetime(job.trigger.run_date) if job.trigger.run_date else 'N/A'
                    trigger_info = f"Once @ {run_date_display}"
                elif isinstance(job.trigger, CronTrigger):
                    # CronTrigger's __str__ method provides a good representation
                    trigger_info = f"Cron: {job.trigger}"
                elif isinstance(job.trigger, IntervalTrigger):
                    trigger_info = f"Every {job.trigger.interval}"
                else:
                     trigger_info = str(job.trigger) # Fallback

                args_display_str = format_job_args(job.args)

                jobs_display.append({
                    'id': job.id, 'name': job.name,
                    'next_run_time_iso': next_run_iso, # Use ISO for JS
                    'args_display_str': args_display_str,
                    'trigger_info': trigger_info
                })
        elif not scheduler:
            flash("Scheduler not initialized.", "warning")
        else: # scheduler exists but not running
             flash("Scheduler is not running.", "warning")
    except Exception as e: logger.error(f"Error fetching jobs: {e}", exc_info=True); flash("Error fetching scheduled jobs.", "danger")

    return render_template(
        'index.html', recent_runs=recent_runs, jobs=jobs_display,
        vendors=vendors, hosts=hosts, current_theme=current_theme
    )


@bp.route('/allocations')
def view_allocations():
    """Displays current allocations with filtering and server-side pagination."""
    # Logic moved from original app.py
    from collections import defaultdict # Needed for grouping

    current_theme = request.cookies.get('theme', 'light')
    current_filters = {k: v for k, v in request.args.items() if k != 'page'}
    try: page = int(request.args.get('page', 1)); page = max(1, page)
    except ValueError: page = 1
    per_page = 50

    mongo_filter = {}
    flat_display_list_all = []
    total_data_items = 0

    # Build Mongo Filter (moved from original app.py)
    if current_filters.get('filter_tag'): mongo_filter['tag'] = {'$regex': re.escape(current_filters["filter_tag"]), '$options': 'i'}
    if current_filters.get('filter_vendor'): mongo_filter['courses.vendor'] = {'$regex': f'^{re.escape(current_filters["filter_vendor"])}$', '$options': 'i'}
    if current_filters.get('filter_course'): mongo_filter['courses.course_name'] = {'$regex': re.escape(current_filters['filter_course']), '$options': 'i'}

    try:
        if db is None or alloc_collection is None:
            flash("DB connection or allocation collection unavailable.", "danger")
            raise ConnectionError("Database not available")

        temp_grouped_allocations = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        alloc_cursor = alloc_collection.find(mongo_filter).sort("tag", 1)

        for tag_doc in alloc_cursor:
            # ... (rest of the data fetching and processing logic from original app.py) ...
            # ... This includes host filter, number filter, grouping, power status ...
             tag = tag_doc.get("tag", "Unknown Tag")
             courses_in_tag = tag_doc.get("courses", [])
             if not isinstance(courses_in_tag, list): continue

             for course in courses_in_tag:
                 if not isinstance(course, dict): continue
                 course_name = course.get("course_name", "Unknown Course")
                 vendor = course.get("vendor", "N/A")
                 is_f5 = vendor.lower() == 'f5'
                 pod_details_list = course.get("pod_details", [])
                 if not isinstance(pod_details_list, list): continue

                 for pod_detail in pod_details_list:
                     if not isinstance(pod_detail, dict): continue
                     host = pod_detail.get("host", pod_detail.get("pod_host", "Unknown Host"))
                     pod_num = pod_detail.get("pod_number")
                     class_num = pod_detail.get("class_number")
                     prtg_url = pod_detail.get("prtg_url")
                     power_on_status = str(pod_detail.get("poweron", False)).lower() == 'true'

                     # Post-processing Host Filter
                     filter_host_val = current_filters.get('filter_host', '').strip().lower()
                     if filter_host_val and filter_host_val not in host.lower(): continue

                     # Post-processing Number Filter
                     num_filter_str = current_filters.get('filter_number', '').strip()
                     num_filter_int = None
                     is_num_filter_active = False
                     if num_filter_str:
                         try: num_filter_int = int(num_filter_str); is_num_filter_active = True
                         except ValueError: is_num_filter_active = False

                     item_data = { "pod_number": pod_num, "class_number": class_num, "prtg_url": prtg_url, "vendor": vendor, "course_name": course_name, "tag": tag, "host": host, "type": "unknown", "number": None, "poweron": power_on_status }

                     if is_f5 and class_num is not None:
                         item_data["type"] = "f5_class"; item_data["number"] = class_num
                         nested_pods_filtered = []
                         nested_power_states = {}
                         for np in pod_detail.get("pods", []):
                              if isinstance(np, dict):
                                   np_num = np.get("pod_number"); np_power = str(np.get("poweron", False)).lower() == 'true'
                                   if np_num is not None:
                                       nested_power_states[np_num] = np_power
                                       if not is_num_filter_active or np_num == num_filter_int: nested_pods_filtered.append(np_num)
                         class_matches_num = (not is_num_filter_active) or (class_num == num_filter_int)
                         if class_matches_num or nested_pods_filtered:
                              item_data["nested_pods"] = sorted(list(set(nested_pods_filtered))); item_data["nested_power_states"] = nested_power_states
                              group_list = temp_grouped_allocations[tag][course_name][host]
                              if not any(d.get("type") == "f5_class" and d.get("number") == class_num for d in group_list): group_list.append(item_data)
                     elif pod_num is not None:
                          item_data["type"] = "pod"; item_data["number"] = pod_num
                          pod_matches_num = (not is_num_filter_active) or (pod_num == num_filter_int)
                          if pod_matches_num: temp_grouped_allocations[tag][course_name][host].append(item_data)

        # Flatten the Grouped Data with Headers
        current_tag, current_course, current_host = None, None, None
        for tag, courses in sorted(temp_grouped_allocations.items()):
            if tag != current_tag: flat_display_list_all.append({"row_type": "tag_header", "tag": tag}); current_tag, current_course, current_host = tag, None, None
            for course_name, hosts_data in sorted(courses.items()):
                if course_name != current_course: flat_display_list_all.append({"row_type": "course_header", "course_name": course_name, "tag": tag}); current_course, current_host = course_name, None
                for host, items in sorted(hosts_data.items()):
                    if host != current_host: flat_display_list_all.append({"row_type": "host_header", "host": host, "course_name": course_name, "tag": tag}); current_host = host
                    items.sort(key=lambda x: (x.get("type", ""), x.get("number", 0)))
                    for item in items: item["row_type"] = "data"; flat_display_list_all.append(item); total_data_items += 1

    except (PyMongoError, ConnectionError) as db_e: logger.error(f"Database error fetching allocations: {db_e}"); flash("Error fetching allocation data from database.", "danger"); flat_display_list_all = []; total_data_items = 0
    except Exception as e: logger.error(f"Error processing allocation data: {e}", exc_info=True); flash("An error occurred while processing allocation data.", "danger"); flat_display_list_all = []; total_data_items = 0

    # Server-Side Pagination Logic
    total_rows = len(flat_display_list_all)
    total_pages = math.ceil(total_rows / per_page) if per_page > 0 and total_rows > 0 else 1
    page = max(1, min(page, total_pages))
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    paginated_list = flat_display_list_all[start_index:end_index]

    return render_template(
        'allocations.html',
        allocation_list=paginated_list, total_data_items=total_data_items,
        current_page=page, total_pages=total_pages,
        pagination_args=current_filters, current_filters=current_filters,
        current_theme=current_theme
    )

@bp.route('/logs/<run_id>')
def log_detail(run_id):
    """Display details and logs for a specific run."""
    # Logic moved from original app.py
    redis_url_in_route = current_app.config.get("REDIS_URL")
    logger.info(f"--- DEBUG [/logs/{run_id}]: app.config['REDIS_URL'] = {redis_url_in_route} ---")
    current_theme = request.cookies.get('theme', 'light')
    op_log_data, historical_log_messages, std_log_count = None, [], 0
    sse_available = redis_url_in_route is not None

    if db is None:
        flash("DB unavailable.", "danger")
        return render_template( 'log_detail.html', log=None, historical_log_messages=[], std_log_count=0, run_id=run_id, current_theme=current_theme, sse_enabled=sse_available )

    # Fetch Op Log Summary
    try:
        if op_logs_collection is not None:
            op_log_data = op_logs_collection.find_one({'run_id': run_id})
            if op_log_data:
                op_log_data['_id'] = str(op_log_data['_id'])
                op_log_data['start_time_iso'] = format_datetime(op_log_data.get('start_time'))
                op_log_data['end_time_iso'] = format_datetime(op_log_data.get('end_time'))
                op_log_data['args_display'] = { k: str(v) for k, v in op_log_data.get('args', {}).items() }
                for p_log in op_log_data.get('pod_statuses', []): p_log['timestamp_iso'] = format_datetime(p_log.get('timestamp'))
        else: flash("Op logs collection unavailable.", "warning")
    except PyMongoError as e: logger.error(f"Error fetch op log {run_id}: {e}"); flash("DB error fetch op log.", "danger")
    except Exception as e: logger.error(f"Unexpected error fetch op log {run_id}: {e}", exc_info=True); flash("Server error.", "danger")

    # Fetch Detailed Logs
    try:
        if std_logs_collection is not None:
            log_doc = std_logs_collection.find_one({'run_id': run_id})
            if log_doc:
                messages = log_doc.get('messages', [])
                std_log_count = len(messages)
                for msg in messages:
                    if isinstance(msg, dict): historical_log_messages.append({ 'level': msg.get('level', 'N/A'), 'logger_name': msg.get('logger_name', 'N/A'), 'message': msg.get('message', ''), 'timestamp_iso': format_datetime(msg.get('timestamp')) })
            elif op_log_data: flash(f"Detailed log document not found for run {run_id}.", "warning")
            elif not op_log_data: flash(f"No logs found for run ID {run_id}.", 'warning')
        else: flash("Detailed logs collection unavailable.", "warning")
    except PyMongoError as e: logger.error(f"Error fetch detailed logs {run_id}: {e}"); flash("DB error fetch detailed logs.", "danger")
    except Exception as e: logger.error(f"Unexpected error fetch detailed logs {run_id}: {e}", exc_info=True); flash("Server error.", "danger")

    return render_template( 'log_detail.html', log=op_log_data, historical_log_messages=historical_log_messages, std_log_count=std_log_count, run_id=run_id, current_theme=current_theme, sse_enabled=sse_available )


@bp.route('/logs/all')
def all_logs():
    """Displays all operation logs with filtering and pagination."""
    # Logic moved from original app.py
    current_theme = request.cookies.get('theme', 'light')
    page = request.args.get('page', 1, type=int)
    per_page = 50
    current_filters = {k: v for k, v in request.args.items() if k != 'page'}
    mongo_filter = build_log_filter_query(request.args)
    logs, total_logs = [], 0

    if db is None or op_logs_collection is None:
        flash("DB or op_logs collection unavailable.", "danger")
        return render_template( 'all_logs.html', logs=[], current_page=1, total_pages=0, total_logs=0, pagination_args={}, current_filters={}, current_theme=current_theme )

    try:
        total_logs = op_logs_collection.count_documents(mongo_filter)
        skip = (page - 1) * per_page
        cursor = op_logs_collection.find(mongo_filter).sort('start_time', DESCENDING).skip(skip).limit(per_page)
        docs = list(cursor)
        for log in docs:
            log['_id'] = str(log['_id']); log['start_time_iso'] = format_datetime(log.get('start_time')); log['end_time_iso'] = format_datetime(log.get('end_time'))
            log['args_display'] = { k: str(v) for k, v in log.get('args', {}).items() }; logs.append(log)
    except PyMongoError as e: logger.error(f"Error fetching all logs page {page}: {e}"); flash("Error fetching logs.", "danger")
    except Exception as e: logger.error(f"Unexpected error processing all logs: {e}", exc_info=True); flash("Server error.", "danger")

    total_pages = math.ceil(total_logs / per_page) if per_page > 0 else 1
    pagination_args = current_filters.copy()

    return render_template( 'all_logs.html', logs=logs, current_page=page, total_pages=total_pages, total_logs=total_logs, pagination_args=pagination_args, current_filters=current_filters, current_theme=current_theme )

@bp.route('/terminal')
def terminal():
    """Renders the pseudo-terminal page."""
    # Logic moved from original app.py
    current_theme = request.cookies.get('theme', 'light')
    return render_template('terminal.html', current_theme=current_theme)

# --- Upcoming Courses Route ---
@bp.route('/upcoming-courses')
def view_upcoming_courses():
    current_theme = request.cookies.get('theme', 'light')
    # Initialize lists
    hosts_list = []
    course_configs_list = []
    build_rules = [] # Use build_rules instead of mapping_rules
    courses_with_preselects = [] # Store final augmented data
    error_message = None

    # Fetch Hosts (Needed by get_upcoming_courses_data)
    if host_collection is not None:
        try:
            hosts_cursor = host_collection.find({}, {"host_name": 1, "_id": 0}).sort("host_name", 1)
            hosts_list = [host['host_name'] for host in hosts_cursor if 'host_name' in host]
        except PyMongoError as e: logger.error(f"Hosts fetch error: {e}"); flash("Error fetching hosts.", "warning")
    else: flash("Host collection unavailable.", "warning")

    # Fetch Course Configs (Needed for dropdown AND rule application)
    if course_config_collection is not None:
        try:
            configs_cursor = course_config_collection.find({},{"course_name": 1, "vendor_shortcode": 1, "_id": 0}).sort([("vendor_shortcode", 1), ("course_name", 1)])
            course_configs_list = list(configs_cursor)
        except PyMongoError as e: logger.error(f"Configs fetch error: {e}"); flash("Error fetching lab build course list.", "warning")
    else: flash("Course config collection unavailable.", "warning")

    # Fetch Build Rules (NEEDED for auto-selection)
    if build_rules_collection is not None: # Use build_rules_collection
        try:
            rules_cursor = build_rules_collection.find().sort("priority", ASCENDING) # Sort by priority
            build_rules = list(rules_cursor)
            if not build_rules: flash("No build rules found.", "warning")
        except PyMongoError as e: logger.error(f"Error fetching build rules: {e}"); flash("Error fetching build rules.", "danger")
    else: flash("Build rules collection unavailable.", "danger")

    # --- Fetch SF data AND Apply Rules ---
    try:
        # *** Call the function that applies rules ***
        # Pass the build_rules list to the function
        courses_with_preselects = get_upcoming_courses_data(
            build_rules, course_configs_list, hosts_list
        )

        if courses_with_preselects is None:
            flash("Failed to fetch or process Salesforce data. Check server logs.", "danger")
            courses_with_preselects = []
            error_message = "Error retrieving data."
        # No need for elif not courses... here, empty list is handled by template

    except Exception as e:
        logger.error(f"Unexpected error in /upcoming-courses route: {e}", exc_info=True)
        flash("An unexpected error occurred while loading upcoming courses.", "danger")
        error_message = "Server error."
        courses_with_preselects = []

    # Pass augmented data (courses) and configs (for dropdowns) to template
    return render_template(
        'upcoming_courses.html',
        courses=courses_with_preselects, # This list now HAS preselect_* fields
        error_message=error_message,
        course_configs_list=course_configs_list, # Still needed for dropdown options
        current_theme=current_theme
    )

# --- Add Status Endpoint ---
@bp.route('/status/<run_id>')
def run_status(run_id):
    """API endpoint to get the current status of a run."""
    if op_logs_collection is not None:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        log_data = op_logs_collection.find_one(
            {'run_id': run_id},
            { # Projection to get only needed fields
                'overall_status': 1,
                'end_time': 1,
                'duration_seconds': 1,
                'summary': 1,
                'pod_statuses': {'$slice': -1} # Get only the last pod status update
            }
        )
        if not log_data:
            return jsonify({"error": "Run ID not found"}), 404

        # Prepare response data
        status_info = {
            'run_id': run_id,
            'overall_status': log_data.get('overall_status', 'unknown'),
            'end_time_iso': format_datetime(log_data.get('end_time')),
            'duration_seconds': log_data.get('duration_seconds'),
            'success_count': log_data.get('summary', {}).get('success_count'),
            'failure_count': log_data.get('summary', {}).get('failure_count'),
            'last_pod_status': None
        }
        # Add last pod status if available
        if log_data.get('pod_statuses'):
            last_status = log_data['pod_statuses'][0]
            status_info['last_pod_status'] = {
                 'identifier': last_status.get('identifier'),
                 'status': last_status.get('status'),
                 'step': last_status.get('failed_step'),
                 'timestamp_iso': format_datetime(last_status.get('timestamp'))
            }

        return jsonify(status_info)

    except PyMongoError as e:
        logger.error(f"DB error fetching status for {run_id}: {e}")
        return jsonify({"error": "Database query error"}), 500
    except Exception as e:
         logger.error(f"Error fetching status for {run_id}: {e}", exc_info=True)
         return jsonify({"error": "Internal server error"}), 500

# --- END OF dashboard/routes/main.py ---