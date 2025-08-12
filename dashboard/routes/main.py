# dashboard/routes/main.py

import re
import math
import logging
from flask import (
    Blueprint, render_template, request, flash, current_app, jsonify, redirect, url_for, g, session
)
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import PyMongoError
from typing import Dict, List, Optional, Any, OrderedDict, Tuple
import datetime
import pytz

# Import extensions and utils from the dashboard package
from ..extensions import (
    op_logs_collection, std_logs_collection, course_config_collection,
    host_collection, alloc_collection, scheduler, db, # Need db/collections
    build_rules_collection,
    interim_alloc_collection
)
from ..utils import build_log_filter_query, format_datetime
from collections import defaultdict

# Import the Salesforce util that APPLIES RULES
from ..salesforce_utils import get_upcoming_courses_data # USE THIS ONE

import pytz
from ..extensions import SERVER_TIMEZONE


# Define Blueprint
bp = Blueprint('main', __name__)
logger = logging.getLogger('dashboard.routes.main')

# --- NEW: Timezone Helper ---
def get_user_timezone():
    """Gets the user's timezone from the session, falling back to the server's default."""
    try:
        # Get the IANA timezone name from the session
        tz_name = session.get('user_timezone', SERVER_TIMEZONE.zone)
        # Return a pytz timezone object
        return pytz.timezone(tz_name)
    except pytz.UnknownTimeZoneError:
        # If the session contains an invalid timezone, fall back to the server default
        logger.warning(f"Invalid timezone in session: {session.get('user_timezone')}. Defaulting to {SERVER_TIMEZONE.zone}")
        session['user_timezone'] = SERVER_TIMEZONE.zone # Correct the session
        return SERVER_TIMEZONE
    except Exception as e:
        logger.error(f"Error getting user timezone: {e}")
        return SERVER_TIMEZONE


@bp.app_context_processor
def inject_global_vars():
    """Injects global variables into all templates."""
    if 'past_due_notifications' not in g:
        g.past_due_notifications = get_past_due_allocations()
    
    # --- NEW: Inject timezone information ---
    if 'all_timezones' not in g:
        g.all_timezones = pytz.all_timezones
    
    user_tz = get_user_timezone()
    # --- END NEW ---
    
    return dict(
        past_due_notifications=g.past_due_notifications,
        all_timezones=g.all_timezones,
        current_user_timezone=user_tz.zone # Pass the string name
    )

# --- NEW: Route to update timezone ---
@bp.route('/set-timezone', methods=['POST'])
def set_timezone():
    """API endpoint to set the user's timezone in the session."""
    data = request.json
    tz_name = data.get('timezone')
    
    if tz_name in pytz.all_timezones:
        session['user_timezone'] = tz_name
        logger.info(f"User timezone updated to: {tz_name}")
        return jsonify({"success": True, "message": f"Timezone set to {tz_name}."})
    else:
        logger.warning(f"Attempted to set invalid timezone: {tz_name}")
        return jsonify({"success": False, "error": "Invalid timezone specified."}), 400

def _prepare_date_for_display_iso(date_str: Optional[str], context_info: str = "") -> Optional[str]:
    if not date_str or date_str == "N/A":
        logger.debug(f"Date Prep ({context_info}): Input date string is None, empty, or 'N/A': '{date_str}'. Returning None.")
        return None
    
    parsed_date = None
    # Prioritize the expected "YYYY-MM-DD" format
    expected_formats = ["%Y-%m-%d"]
    # Add other common formats as fallbacks
    fallback_formats = ["%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%d-%B-%Y", "%Y/%m/%d"]
    
    all_formats_to_try = expected_formats + fallback_formats

    for fmt in all_formats_to_try:
        try:
            parsed_date = datetime.datetime.strptime(date_str, fmt).date()
            logger.debug(f"Date Prep ({context_info}): Successfully parsed '{date_str}' with format '{fmt}' to {parsed_date}.")
            break # Parsed successfully
        except ValueError:
            logger.debug(f"Date Prep ({context_info}): Failed to parse '{date_str}' with format '{fmt}'.")
            continue # Try next format
    
    if parsed_date:
        # Convert to datetime at midnight UTC for ISO string
        # Ensure correct UTC localization
        dt_obj_utc = datetime.datetime.combine(parsed_date, datetime.time.min)
        dt_obj_utc = pytz.utc.localize(dt_obj_utc) # Make it timezone-aware (UTC)
        
        iso_string = dt_obj_utc.isoformat() # This should produce 'YYYY-MM-DDTHH:MM:SS+00:00'
        
        # Ensure it ends with 'Z' for JavaScript compatibility, if not already +00:00
        if iso_string.endswith('+00:00'):
            iso_string = iso_string.replace('+00:00', 'Z')
        elif not iso_string.endswith('Z'): # Should not happen if localized to UTC correctly
            logger.warning(f"Date Prep ({context_info}): ISO string for {dt_obj_utc} did not end with +00:00 or Z: {iso_string}. Appending Z.")
            iso_string += 'Z'

        logger.debug(f"Date Prep ({context_info}): Converted to ISO string: {iso_string}")
        return iso_string
    
    logger.warning(f"Date Prep ({context_info}): Could not parse date string '{date_str}' into any known format. Returning None.")
    return None

def get_past_due_allocations(ignore_session_cleared=False):
    """
    Fetches allocations where the course end date is in the past
    and the tag is not marked with extend: "true".
    Now uses 'Australia/Sydney' for consistent date comparison.

    Args:
        ignore_session_cleared (bool): If True, returns all past-due items
                                       regardless of the session's cleared list.
    """
    past_due_items = []
    if alloc_collection is None:
        logger.warning("get_past_due_allocations: alloc_collection is None.")
        return past_due_items

    cleared_in_session = set()
    if not ignore_session_cleared:
        cleared_in_session = session.get('cleared_notifications', set())

    try:
        # --- THIS IS THE FIX ---
        # Define the target timezone
        sydney_tz = pytz.timezone('Australia/Sydney')
        # Get the current date in the specified timezone
        today_sydney = datetime.datetime.now(sydney_tz).date()
        logger.debug(f"Running past-due check against Australia/Sydney date: {today_sydney}")
        # --- END OF FIX ---

        relevant_tags_cursor = alloc_collection.find(
            {"extend": {"$ne": "true"}}
        )

        for tag_doc in relevant_tags_cursor:
            tag_name = tag_doc.get("tag")
            if not tag_name: continue

            latest_end_date_str = None
            latest_end_date_obj = None
            course_names_in_tag = set()

            for course_data in tag_doc.get("courses", []):
                course_names_in_tag.add(course_data.get("course_name", "Unknown Course"))
                end_date_str = str(course_data.get("end_date", ""))
                if not end_date_str or end_date_str == "N/A": continue
                
                try:
                    current_end_date_obj = None
                    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                        try:
                            current_end_date_obj = datetime.datetime.strptime(end_date_str, fmt).date()
                            break
                        except ValueError: continue
                    
                    if current_end_date_obj:
                        if latest_end_date_obj is None or current_end_date_obj > latest_end_date_obj:
                            latest_end_date_obj = current_end_date_obj
                            latest_end_date_str = end_date_str
                except ValueError:
                    logger.warning(f"Could not parse end_date '{end_date_str}' in tag '{tag_name}'.")

            # Compare the course's end date with today's Sydney date
            if latest_end_date_obj and latest_end_date_obj < today_sydney:
                notification_id = f"past_due_{tag_name}".replace(" ", "_").replace("/", "_")
                
                if not ignore_session_cleared and notification_id in cleared_in_session:
                    continue

                past_due_items.append({
                    "id": notification_id,
                    "tag": tag_name,
                    "course_names": ", ".join(sorted(list(course_names_in_tag))),
                    "end_date": latest_end_date_str, 
                    "days_past": (today_sydney - latest_end_date_obj).days
                })

    except PyMongoError as e:
        logger.error(f"Error fetching past due allocations: {e}", exc_info=True)
    
    past_due_items.sort(key=lambda x: x.get("days_past", 0), reverse=True)
    return past_due_items

# --- NEW: Route for the "All Notifications" page ---
@bp.route('/notifications/all')
def all_notifications():
    """Renders the page showing all past-due allocations."""
    current_theme = request.cookies.get('theme', 'light')
    
    # Fetch all past-due allocations, ignoring the session cleared list
    all_past_due_tags = get_past_due_allocations(ignore_session_cleared=True)
    
    return render_template(
        'notifications.html',
        past_due_tags=all_past_due_tags,
        current_theme=current_theme
    )

# --- NEW: App Context Processor to make notifications available globally ---
@bp.app_context_processor
def inject_notifications():
    """Injects past_due_notifications into all templates."""
    if 'past_due_notifications' not in g:
        g.past_due_notifications = get_past_due_allocations()
    return dict(past_due_notifications=g.past_due_notifications)

@bp.route('/allocations')
def view_allocations():
    """
    Displays current allocations in a collapsible group view,
    paginated by a fixed number of groups.
    """
    current_theme = request.cookies.get('theme', 'light')

    filter_tag_input = request.args.get('filter_tag', '').strip()
    filter_vendor = request.args.get('filter_vendor', '').strip()
    filter_course = request.args.get('filter_course', '').strip()
    filter_host_str = request.args.get('filter_host', '').strip().lower()
    filter_number_str = request.args.get('filter_number', '').strip()
    filter_extend = request.args.get('filter_extend', '')
    try:
        page = int(request.args.get('page', 1))
        page = max(1, page)
    except ValueError:
        page = 1
    groups_per_page = 20

    mongo_query: Dict[str, Any] = {}
    if filter_tag_input: 
        mongo_query['tag'] = {'$regex': re.escape(filter_tag_input), '$options': 'i'}

    if filter_extend == 'yes':
        mongo_query['extend'] = 'true'
    elif filter_extend == 'no':
        mongo_query['extend'] = {'$ne': 'true'}

    current_filter_params = {
        'filter_tag': filter_tag_input, 'filter_vendor': filter_vendor, 'filter_course': filter_course,
        'filter_host': filter_host_str, 'filter_number': filter_number_str,
        'filter_extend': filter_extend
    }
    pagination_link_args = current_filter_params.copy()

    all_items_matching_filters: List[Dict] = []
    if alloc_collection is None:
        flash("Database allocation collection is unavailable.", "danger")
    else:
        try:
            alloc_cursor = alloc_collection.find(mongo_query) 
            number_filter_int = int(filter_number_str) if filter_number_str else None

            for tag_doc in alloc_cursor:
                tag_name = tag_doc.get("tag", "Unknown Tag")
                is_extended_tag = str(tag_doc.get("extend", "false")).lower() == "true"
                
                for course_alloc in tag_doc.get("courses", []):
                    vendor = course_alloc.get("vendor", "")
                    if filter_vendor and not re.match(f'^{re.escape(filter_vendor)}$', vendor, re.I): continue
                    if filter_course and not re.search(re.escape(filter_course), course_alloc.get("course_name", ""), re.I): continue
                    
                    for pod_detail in course_alloc.get("pod_details", []):
                        if filter_host_str and filter_host_str not in pod_detail.get("host", "").lower(): continue
                        
                        item = {
                            "tag": tag_name, "is_extended": is_extended_tag,
                            "start_date": course_alloc.get("start_date"), "end_date": course_alloc.get("end_date"),
                            "trainer_name": course_alloc.get("trainer_name"), "apm_username": course_alloc.get("apm_username"),
                            "apm_password": course_alloc.get("apm_password"), "vendor": vendor,
                            "course_name": course_alloc.get("course_name"), "host": pod_detail.get("host", "Unknown Host"),
                            "poweron": str(pod_detail.get("poweron", "false")).lower() == 'true', "prtg_url": pod_detail.get("prtg_url")
                        }
                        
                        is_f5_class_entry = vendor.lower() == 'f5' and "class_number" in pod_detail
                        if is_f5_class_entry:
                            item.update({"type": "f5_class", "number": pod_detail.get("class_number"), "class_number": pod_detail.get("class_number"), "nested_pods": pod_detail.get("pods", [])})
                        else:
                            item.update({"type": "pod", "number": pod_detail.get("pod_number"), "pod_number": pod_detail.get("pod_number")})
                        
                        if number_filter_int is not None:
                            nested_pod_numbers = set()
                            for p in item.get("nested_pods", []):
                                try:
                                    if p.get("pod_number") is not None: nested_pod_numbers.add(int(p.get("pod_number")))
                                except (ValueError, TypeError):
                                    pass
                            
                            item_number_val = None
                            try:
                                if item.get("number") is not None: item_number_val = int(item.get("number"))
                            except (ValueError, TypeError):
                                pass

                            if item_number_val != number_filter_int and not (is_f5_class_entry and number_filter_int in nested_pod_numbers):
                                continue
                        
                        all_items_matching_filters.append(item)
        except PyMongoError as e_mongo:
            flash("Error fetching data from database.", "danger")

    grouped_by_tag = defaultdict(list)
    for item in all_items_matching_filters:
        grouped_by_tag[item['tag']].append(item)

    final_groups = []
    for tag, items in grouped_by_tag.items():
        first_item = items[0]
        summary = {
            "tag": tag,
            "is_extended": first_item['is_extended'],
            "start_date": first_item.get('start_date', 'N/A'),
            "end_date": first_item.get('end_date', 'N/A'),
            "trainer_name": first_item.get('trainer_name', 'N/A'),
            "apm_username": first_item.get('apm_username', 'N/A'),
            "apm_password": first_item.get('apm_password', 'N/A'),
            "course_names": sorted(list({item['course_name'] for item in items}))
        }
        
        all_pod_numbers = set()
        all_class_numbers = set()
        
        for item in items:
            try:
                if item.get('pod_number') is not None:
                    all_pod_numbers.add(int(item['pod_number']))
            except (ValueError, TypeError):
                logger.warning(f"Could not convert pod_number '{item.get('pod_number')}' to int for tag '{tag}'.")
            
            try:
                if item.get('class_number') is not None:
                    all_class_numbers.add(int(item['class_number']))
            except (ValueError, TypeError):
                logger.warning(f"Could not convert class_number '{item.get('class_number')}' to int for tag '{tag}'.")
        
        pod_range_str = ""
        if all_pod_numbers:
            min_pod, max_pod = min(all_pod_numbers), max(all_pod_numbers)
            pod_range_str = f"Pods: {min_pod}-{max_pod}" if min_pod != max_pod else f"Pod: {min_pod}"
        
        class_range_str = ""
        if all_class_numbers:
            min_class, max_class = min(all_class_numbers), max(all_class_numbers)
            class_range_str = f"Classes: {min_class}-{max_class}" if min_class != max_class else f"Class: {min_class}"
        
        summary['pod_range_display'] = " | ".join(filter(None, [class_range_str, pod_range_str]))
        
        # --- THIS IS THE FIX ---
        # Helper function to safely convert values to integers for sorting
        def safe_int(value, default=9999):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default

        # Apply safe conversion in the sort key
        items.sort(key=lambda item: (
            safe_int(item.get('class_number'), 9999), 
            -1 if item.get('type') == 'f5_class' else safe_int(item.get('pod_number'), 9999)
        ))
        # --- END OF FIX ---

        final_groups.append({"summary": summary, "details": items})

    final_groups.sort(key=lambda g: g['summary']['tag'].lower())
        
    total_groups_matching_filters = len(final_groups)
    total_pages = math.ceil(total_groups_matching_filters / groups_per_page) if groups_per_page > 0 else 1
    page = max(1, min(page, total_pages))
    start_idx = (page - 1) * groups_per_page
    end_idx = start_idx + groups_per_page
    paginated_groups = final_groups[start_idx:end_idx]

    return render_template(
        'allocations.html',
        grouped_allocations=paginated_groups,
        total_display_count=total_groups_matching_filters, 
        current_page=page,
        total_pages=total_pages, 
        pagination_args=pagination_link_args,
        current_filters=current_filter_params,
        current_theme=current_theme
    )

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

@bp.route('/logs/<run_id>')
def log_detail(run_id):
    """Display details and logs for a specific run."""
    # --- HIGHLIGHTED MODIFICATION: Fetch historical logs and check SSE status ---
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
        if courses_with_preselects and interim_alloc_collection is not None:
            # Create a set of SF course codes from the upcoming courses for an efficient query
            upcoming_sf_codes = {c.get('Course Code') for c in courses_with_preselects if c.get('Course Code')}
            
            # Find all interim docs that match these SF codes
            interim_docs_cursor = interim_alloc_collection.find(
                {"sf_course_code": {"$in": list(upcoming_sf_codes)}},
                {"_id": 0, "sf_course_code": 1, "batch_review_id": 1}
            )
            
            # Create a mapping from sf_course_code to its batch_review_id
            sf_code_to_batch_id = {doc['sf_course_code']: doc['batch_review_id'] for doc in interim_docs_cursor}
            
            # Augment the course data with the batch_id if it exists
            for course in courses_with_preselects:
                sf_code = course.get('Course Code')
                course['existing_batch_id'] = sf_code_to_batch_id.get(sf_code)

    except Exception as e:
        logger.error(f"Unexpected error in /upcoming-courses route: {e}", exc_info=True)
        flash("An unexpected error occurred while loading upcoming courses.", "danger")
        error_message = "Server error."
        courses_with_preselects = []
    
    saved_plan_batch_id = None
    if interim_alloc_collection is not None:
        try:
            # Find the most recently created document that is saved for later
            latest_plan = interim_alloc_collection.find_one(
                {"status": "saved_for_later"},
                sort=[("created_at", DESCENDING)]
            )
            if latest_plan:
                saved_plan_batch_id = latest_plan.get('batch_review_id')
        except PyMongoError as e:
            logger.warning(f"Could not check for saved plans: {e}")


    # Pass augmented data (courses) and configs (for dropdowns) to template
    return render_template(
        'upcoming_courses.html',
        courses=courses_with_preselects, # This list now HAS preselect_* fields
        error_message=error_message,
        course_configs_list=course_configs_list, # Still needed for dropdown options
        saved_plan_batch_id=saved_plan_batch_id,
        current_theme=current_theme
    )
    
@bp.route('/notifications/clear/<notification_id>', methods=['POST'])
def clear_notification(notification_id: str):
    """
    Marks a specific notification as 'cleared' for the current user session.
    """
    logger.info(f"Attempting to clear notification ID: {notification_id}")
    if 'cleared_notifications' not in session:
        session['cleared_notifications'] = set()
    
    # Ensure session['cleared_notifications'] is a set
    if not isinstance(session['cleared_notifications'], set):
        try:
            session['cleared_notifications'] = set(session['cleared_notifications'])
        except TypeError: # If it's not iterable, reset it
            session['cleared_notifications'] = set()

    session['cleared_notifications'].add(notification_id)
    session.modified = True # Important to mark session as modified
    
    flash(f"Notification '{notification_id.split('_')[-1]}' (from tag '{notification_id.split('_')[-2]}') marked as read for this session.", "info")
    return redirect(request.referrer or url_for('main.index'))

@bp.route('/notifications/clear-all', methods=['POST'])
def clear_all_notifications():
    """
    Marks ALL currently past-due notifications as 'cleared' for the current user session.
    """
    logger.info("Attempting to clear all current past-due notifications for this session.")
    
    # Get current past-due items (before modifying session)
    # We call get_past_due_allocations() but it will use the *current* session state.
    # So, we need to get the list of IDs *before* we add them to the session's cleared set.
    # A simpler way is to just clear the session's set or add a special "clear_all_flag".
    # For now, let's get current notifications and add all their IDs.
    
    # To avoid re-querying and complex logic with g, let's fetch them once more
    # or rely on the idea that if this route is hit, it's after a page render where g was populated.
    # Safer to re-fetch IF the list in 'g' isn't guaranteed to be up-to-date for this exact request.
    # However, for session-based clearing, we just need to ensure future calls to get_past_due_allocations
    # will return an empty list if "clear all" was pressed.

    # A more robust way for "clear all" with session:
    # We can't easily add all *future* past_due IDs.
    # Instead, we can either:
    # 1. Store a "cleared_all_timestamp" in the session. `get_past_due_allocations` would then
    #    ignore any notification whose original end_date is before this timestamp. (More complex date logic)
    # 2. Simpler: Just add all *currently visible* past-due notification IDs to the session's cleared set.
    #    This means if new items become past-due later, they will appear.

    # Let's go with option 2 for simplicity for now:
    current_notifications_to_clear = get_past_due_allocations() # Gets uncleared items
    
    if 'cleared_notifications' not in session:
        session['cleared_notifications'] = set()
    
    if not isinstance(session['cleared_notifications'], set): # Ensure it's a set
        try: session['cleared_notifications'] = set(session['cleared_notifications'])
        except TypeError: session['cleared_notifications'] = set()

    cleared_count = 0
    for item in current_notifications_to_clear:
        if item['id'] not in session['cleared_notifications']:
            session['cleared_notifications'].add(item['id'])
            cleared_count +=1
    
    if cleared_count > 0:
        session.modified = True
        flash(f"{cleared_count} notification(s) marked as read for this session.", "success")
    else:
        flash("No active notifications to clear.", "info")
        
    return redirect(request.referrer or url_for('main.index'))