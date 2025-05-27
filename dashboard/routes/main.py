# dashboard/routes/main.py

import re
import math
import logging
from flask import (
    Blueprint, render_template, request, flash, current_app, jsonify, redirect, url_for
)
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import PyMongoError
from typing import Dict, List, Optional, Any, OrderedDict, Tuple
import datetime


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
    """
    Displays current allocations, paginated by a fixed number of unique tags,
    with cells merged for items under the same tag.
    Items are sorted by Tag, then by Course Start Date, then by Pod/Class Number.
    """
    current_theme = request.cookies.get('theme', 'light')

    # --- 1. Get Filter & Pagination Parameters ---
    filter_tag_input = request.args.get('filter_tag', '').strip()
    filter_vendor = request.args.get('filter_vendor', '').strip()
    filter_course = request.args.get('filter_course', '').strip()
    filter_host_str = request.args.get('filter_host', '').strip().lower()
    filter_number_str = request.args.get('filter_number', '').strip()

    try:
        page = int(request.args.get('page', 1))
        page = max(1, page)
    except ValueError:
        page = 1
    
    tags_per_page = 5  # Configurable: Number of unique tags to display per page

    # --- 2. Build Initial MongoDB Query ---
    mongo_query: Dict[str, Any] = {}
    if filter_tag_input: # If specific tag is filtered, narrow down DB query
        mongo_query['tag'] = {'$regex': re.escape(filter_tag_input), '$options': 'i'}
    # Other primary filters could be added to mongo_query if they map directly to top-level fields

    current_filter_params = {
        'filter_tag': filter_tag_input,
        'filter_vendor': filter_vendor,
        'filter_course': filter_course,
        'filter_host': filter_host_str,
        'filter_number': filter_number_str
    }
    pagination_link_args = current_filter_params.copy()

    # --- 3. Fetch Data & Build Intermediate Flat List with Python-side Filters ---
    all_items_matching_filters: List[Dict] = []
    db_error = False

    if alloc_collection is None:
        flash("Database allocation collection is unavailable.", "danger")
        logger.error("view_allocations: alloc_collection is None.")
        db_error = True
    else:
        try:
            logger.debug(f"Fetching allocations with initial DB query: {mongo_query}")
            # Fetch all documents matching the primary tag filter (if any).
            # We will sort comprehensively in Python later.
            alloc_cursor = alloc_collection.find(mongo_query) # No sort here yet

            number_filter_int: Optional[int] = None
            if filter_number_str:
                try: number_filter_int = int(filter_number_str)
                except ValueError: logger.debug(f"Invalid number filter: '{filter_number_str}'")

            for tag_doc in alloc_cursor:
                tag_name = tag_doc.get("tag", "Unknown Tag")
                is_extended_tag = str(tag_doc.get("extend", "false")).lower() == "true"
                
                items_for_this_tag_doc = []
                tag_passes_all_item_filters = False # Does this tag have at least one item passing all filters?

                for course_alloc in tag_doc.get("courses", []):
                    vendor = course_alloc.get("vendor")
                    course_name = course_alloc.get("course_name")
                    # Get course-level start_date and end_date
                    course_start_date_str = str(course_alloc.get("start_date", "N/A"))
                    course_end_date_str = str(course_alloc.get("end_date", "N/A"))


                    if not vendor or not course_name: continue

                    if filter_vendor and not re.match(f'^{re.escape(filter_vendor)}$', vendor, re.I):
                        continue
                    if filter_course and not re.search(re.escape(filter_course), course_name, re.I):
                        continue

                    for pod_detail in course_alloc.get("pod_details", []):
                        host = pod_detail.get("host", pod_detail.get("pod_host", "Unknown Host"))
                        pod_num = pod_detail.get("pod_number")
                        class_num = pod_detail.get("class_number")
                        is_f5 = vendor.lower() == 'f5'

                        if filter_host_str and filter_host_str not in host.lower():
                            continue

                        item_type = "unknown"
                        item_number_display: Any = None
                        nested_pods_f5: List[Any] = []

                        if is_f5 and class_num is not None:
                            item_type = "f5_class"; item_number_display = class_num
                            raw_n = pod_detail.get("pods", [])
                            if isinstance(raw_n, list):
                                nested_pods_f5 = [p.get("pod_number") for p in raw_n if isinstance(p, dict) and p.get("pod_number") is not None]
                        elif pod_num is not None:
                            item_type = "pod"; item_number_display = pod_num
                        
                        if number_filter_int is not None:
                            passes_num_filter = False
                            if item_type == "f5_class":
                                if item_number_display == number_filter_int or number_filter_int in nested_pods_f5:
                                    passes_num_filter = True
                            elif item_type == "pod" and item_number_display == number_filter_int:
                                    passes_num_filter = True
                            if not passes_num_filter and item_type != "unknown": continue
                            elif item_type == "unknown": continue 
                        
                        if item_type != 'unknown':
                            tag_passes_all_item_filters = True
                            all_items_matching_filters.append({ # Add directly to the main list
                                "tag": tag_name, "is_extended": is_extended_tag,
                                "vendor": vendor, "course_name": course_name,
                                "course_start_date": course_start_date_str, # Store course start date
                                "course_end_date": course_end_date_str, # Store course end date
                                "host": host, "type": item_type,
                                "number": item_number_display,
                                "pod_number": pod_num, "class_number": class_num,
                                "nested_pods_str": ", ".join(map(str, sorted(list(set(nested_pods_f5))))) if nested_pods_f5 else "-",
                                "prtg_url": pod_detail.get("prtg_url"),
                                "poweron": str(pod_detail.get("poweron", "false")).lower() == 'true'
                            })
        except PyMongoError as e_mongo:
            logger.error(f"DB error fetching allocations: {e_mongo}", exc_info=True)
            flash("Error fetching data from database.", "danger")
            db_error = True
        except Exception as e_proc_alloc:
            logger.error(f"Error processing allocation data: {e_proc_alloc}", exc_info=True)
            flash("Server error while processing allocation data.", "danger")
            db_error = True

    # --- 4. Multi-level Python Sort on the fully filtered list ---
    if not db_error and all_items_matching_filters:
        def get_sort_keys_for_item(item: Dict) -> Tuple[str, datetime.date, int]:
            tag_key = item.get('tag', '').lower()
            
            date_str = item.get('course_start_date', '')
            sort_date = datetime.date.max # Default for unparseable/N/A dates
            if date_str and date_str != "N/A": # Check for "N/A"
                try: # Expect YYYY-MM-DD from your DB or prior processing
                    sort_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError: # Try other common formats if primary parse fails
                    for fmt in ("%d/%m/%Y", "%m/%d/%Y"):
                        try: sort_date = datetime.datetime.strptime(date_str, fmt).date(); break
                        except ValueError: pass
                    if sort_date == datetime.date.max: # Log if still unparsed
                        logger.debug(f"SortKey: Could not parse date '{date_str}' for tag '{tag_key}'. Sorting as latest.")
            
            number_key_val = item.get('number') # This is pod_number or class_number
            number_key = 999999 # Default for non-numeric or missing, sorts last
            if number_key_val is not None:
                try: number_key = int(number_key_val)
                except (ValueError, TypeError): pass 
            
            return (tag_key, sort_date, number_key)

        try:
            all_items_matching_filters.sort(key=get_sort_keys_for_item)
            logger.info("All filtered items sorted by tag, course_start_date, then item number.")
        except Exception as e_sort:
            logger.error(f"Error during multi-level sort: {e_sort}", exc_info=True)
            flash("Error organizing items. Display may be unsorted.", "warning")

    # --- 5. Paginate by Unique Tags & Calculate Rowspans (after multi-level sort) ---
    paginated_allocation_list: List[Dict] = []
    total_unique_tags_matching_filters = 0
    total_pages = 1

    if not db_error and all_items_matching_filters:
        unique_tags_ordered = list(OrderedDict.fromkeys(
            [item['tag'] for item in all_items_matching_filters]
        ))
        total_unique_tags_matching_filters = len(unique_tags_ordered)
        
        if total_unique_tags_matching_filters > 0:
            total_pages = math.ceil(total_unique_tags_matching_filters / tags_per_page) \
                          if tags_per_page > 0 else 1
            page = max(1, min(page, total_pages))

            start_tag_idx = (page - 1) * tags_per_page
            end_tag_idx = start_tag_idx + tags_per_page
            tags_to_display_on_page = set(unique_tags_ordered[start_tag_idx:end_tag_idx]) # Use set for faster lookups
            
            logger.debug(f"Page {page}: Displaying tags {tags_to_display_on_page}")

            current_tag_for_rowspan_calc: Optional[str] = None
            tag_item_counter_for_rowspan = 0
            
            # Iterate the already sorted all_items_matching_filters
            for item_data in all_items_matching_filters:
                if item_data.get('tag') in tags_to_display_on_page:
                    item_data_copy = item_data.copy()
                    
                    if item_data_copy.get('tag') != current_tag_for_rowspan_calc:
                        # This is the first item of a new tag group *on this page*
                        if current_tag_for_rowspan_calc is not None and paginated_allocation_list:
                            # Set rowspan for the first item of the PREVIOUS tag group *on this page*
                            for prev_item_idx in range(len(paginated_allocation_list) - 1, -1, -1):
                                if paginated_allocation_list[prev_item_idx].get('_is_first_in_tag_group') and \
                                   paginated_allocation_list[prev_item_idx].get('tag') == current_tag_for_rowspan_calc:
                                    paginated_allocation_list[prev_item_idx]['tag_rowspan'] = tag_item_counter_for_rowspan
                                    break
                        
                        current_tag_for_rowspan_calc = item_data_copy.get('tag')
                        tag_item_counter_for_rowspan = 1
                        item_data_copy['_is_first_in_tag_group'] = True
                    else:
                        tag_item_counter_for_rowspan += 1
                        item_data_copy['_is_first_in_tag_group'] = False
                    paginated_allocation_list.append(item_data_copy)
            
            # Set rowspan for the very last tag group processed on the page
            if current_tag_for_rowspan_calc is not None and paginated_allocation_list:
                for last_grp_item_idx in range(len(paginated_allocation_list) - 1, -1, -1):
                    if paginated_allocation_list[last_grp_item_idx].get('_is_first_in_tag_group') and \
                       paginated_allocation_list[last_grp_item_idx].get('tag') == current_tag_for_rowspan_calc:
                        paginated_allocation_list[last_grp_item_idx]['tag_rowspan'] = tag_item_counter_for_rowspan
                        break
    
    total_items_in_filtered_tags = len(all_items_matching_filters)

    logger.debug(f"Allocations: Total Matching Tags={total_unique_tags_matching_filters}, Page={page}, "
                 f"TotalPages={total_pages}, TagsPerPage={tags_per_page}, "
                 f"Items on this page (for render)={len(paginated_allocation_list)}")

    return render_template(
        'allocations.html',
        allocation_list=paginated_allocation_list,
        total_display_count=total_unique_tags_matching_filters, # Total unique tags matching filters
        total_items_overall=total_items_in_filtered_tags,    # Total items across all matching tags
        current_page=page,
        total_pages=total_pages, # Total pages of TAGS
        pagination_args=pagination_link_args,
        current_filters=current_filter_params,
        current_theme=current_theme,
        pagination_by_tags=True # Flag for template display logic
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