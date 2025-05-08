# dashboard/routes/actions.py

import logging
import threading
import datetime
import pytz
import json
from flask import (
    Blueprint, request, redirect, url_for, flash, current_app, jsonify, render_template
)
from pymongo.errors import PyMongoError, BulkWriteError
import pymongo
from pymongo import ASCENDING, UpdateOne
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
# Import extensions, utils, tasks from dashboard package
from ..extensions import (
    scheduler,
    db,
    interim_alloc_collection, # For saving review data
    alloc_collection,         # For toggle_power and teardown_tag
    host_collection,          # For getting host list in build_review
    course_config_collection,  # For getting memory in build_review
    build_rules_collection
)

from ..utils import (build_args_from_form, parse_command_line, 
                     update_power_state_in_db, get_hosts_available_memory_parallel)
from ..tasks import run_labbuild_task

# Import top-level utils if needed (e.g., delete_from_database)
from config_utils import get_host_by_name
from db_utils import delete_from_database
from bson.errors import InvalidId
from bson import ObjectId

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:
    # Fallback for Python < 3.9 or if zoneinfo data isn't available
    from datetime import timezone as ZoneInfo # Use basic UTC offset if zoneinfo missing

from constants import SUBSEQUENT_POD_MEMORY_FACTOR

# Define Blueprint
bp = Blueprint('actions', __name__)
logger = logging.getLogger('dashboard.routes.actions')

def _find_matching_build_rule(rules: List[Dict], vendor: str,
                             course_code: str, course_type: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Finds the highest priority build rule matching the course criteria.

    Iterates through rules (assumed pre-sorted by priority ascending) and returns
    the first rule whose conditions match the provided vendor, course code, and type.
    It does not currently support complex pattern extraction within conditions but checks
    for substring presence.

    Args:
        rules: List of build rule documents from MongoDB, sorted by priority ascending.
        vendor: The vendor shortcode (e.g., 'cp', 'pa') associated with the course.
        course_code: The Salesforce course code (e.g., 'EDU-210-W').
        course_type: The Salesforce course type string.

    Returns:
        A tuple containing:
            - The matching rule document dictionary (dict) if found, otherwise None.
            - Currently always None for the second element, as complex pattern
              extraction isn't implemented here (reserved for potential future use).
              Kept tuple structure for potential extensibility.
    """
    if not rules:
        logger.debug("No build rules provided to match against.")
        return None, None # No rules to check

    # Normalize inputs for case-insensitive matching
    cc_lower = course_code.lower()
    ct_lower = course_type.lower()
    vendor_lower = vendor.lower()

    for rule in rules:
        conditions = rule.get("conditions", {})
        # If conditions is empty, it's a potential fallback/default rule
        is_fallback = not conditions

        match = True # Assume match until a condition fails

        # --- Condition Checks ---
        # Only perform checks if conditions exist for the rule
        if not is_fallback:
            # 1. Vendor check (often the primary filter)
            # Rule condition must exist and match the course vendor
            rule_vendor = conditions.get("vendor")
            if rule_vendor and rule_vendor.lower() != vendor_lower:
                match = False
                continue # Skip rule if vendor doesn't match

            # 2. Course Code Contains check
            # If the condition exists in the rule...
            if match and "course_code_contains" in conditions:
                terms = conditions["course_code_contains"]
                # Ensure terms is a list, handle single string case
                if not isinstance(terms, list):
                    terms = [terms]
                # Check if *any* of the terms are present in the course code
                if not any(str(term).lower() in cc_lower for term in terms):
                    match = False
                    continue # Skip if none of the required terms are found

            # 3. Course Code NOT Contains check
            if match and "course_code_not_contains" in conditions:
                terms = conditions["course_code_not_contains"]
                if not isinstance(terms, list):
                    terms = [terms]
                # Check if *any* of the excluded terms are present
                if any(str(term).lower() in cc_lower for term in terms):
                    match = False
                    continue # Skip if an excluded term is found

            # 4. Course Type Contains check
            if match and "course_type_contains" in conditions:
                terms = conditions["course_type_contains"]
                if not isinstance(terms, list):
                    terms = [terms]
                # Check if *any* required terms are present in the course type
                if not any(str(term).lower() in ct_lower for term in terms):
                    match = False
                    continue

            # --- Add more condition checks here as needed (e.g., region) ---

        # If all checks passed (or it's a fallback rule with no conditions)
        if match:
            logger.info(f"Course '{vendor}/{course_code}' matched rule '{rule.get('rule_name', rule.get('_id'))}' (Priority: {rule.get('priority', 'N/A')})")
            # Return the rule (second element of tuple is None currently)
            return rule, None

    # If loop completes without finding a specific match
    logger.warning(f"No specific build rule found for Vendor '{vendor}', Course Code '{course_code}'.")
    return None, None # No specific rule matched

# --- HELPER FUNCTION (can be moved to utils if preferred) ---
def _get_memory_for_course(course_name: str) -> float:
    """Fetches memory_gb_per_pod for a given course name."""
    if course_config_collection is None: return 0.0
    try:
        config = course_config_collection.find_one({"course_name": course_name})
        if config:
            # --- Adjust this key based on your actual course config structure ---
            mem_gb = float(config.get('memory', 0))
            return mem_gb if mem_gb > 0 else 0.0
            # --- Or implement component summing logic here ---
        return 0.0
    except Exception as e:
        logger.error(f"Error getting memory for course '{course_name}': {e}")
        return 0.0

@bp.route('/run', methods=['POST'])
def run_now():
    """Handle immediate run request from the main form."""
    form_data = request.form.to_dict()
    args_list, error_msg = build_args_from_form(form_data) # Use form helper

    if error_msg:
        flash(f'Invalid form data: {error_msg}', 'danger')
        return redirect(url_for('main.index')) # Redirect to main blueprint index
    if not args_list:
         flash('Failed to build command arguments.', 'danger')
         return redirect(url_for('main.index'))

    try:
        # Use daemon thread for background execution
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True)
        thread.start()
        flash(f"Submitted immediate run: {' '.join(args_list)}", 'info')
    except Exception as e:
        logger.error(f"Failed start thread run_now: {e}", exc_info=True)
        flash("Error starting background task.", 'danger')
    return redirect(url_for('main.index'))


@bp.route('/schedule', methods=['POST'])
def schedule_run():
    """Handle schedule run request from the main form."""
    form_data = request.form.to_dict()
    args_list, error_msg = build_args_from_form(form_data) # Use form helper

    if error_msg:
        flash(f'Invalid form data: {error_msg}', 'danger')
        return redirect(url_for('main.index'))
    if not args_list:
         flash('Failed to build command arguments.', 'danger')
         return redirect(url_for('main.index'))

    if not scheduler or not scheduler.running:
        flash("Scheduler not running. Cannot schedule job.", "danger")
        return redirect(url_for('main.index'))

    # Extract schedule details
    schedule_type = form_data.get('schedule_type', 'date')
    schedule_time_str = form_data.get('schedule_time')
    cron_expression = form_data.get('cron_expression')
    interval_value = form_data.get('interval_value')
    interval_unit = form_data.get('interval_unit', 'minutes')

    try:
        job_name = f"{form_data.get('command')}_{form_data.get('vendor')}_{form_data.get('course', 'N/A')}"
        trigger = None
        flash_msg = ""
        log_time_str = "N/A"

        # Import trigger types here
        from apscheduler.triggers.date import DateTrigger
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        # Get scheduler's timezone
        scheduler_tz = scheduler.timezone

        if schedule_type == 'date' and schedule_time_str:
            naive_dt = datetime.datetime.fromisoformat(schedule_time_str)
            # *** MODIFIED TIMEZONE HANDLING ***
            try:
                # Make aware using the scheduler's timezone object
                aware_dt = naive_dt.replace(tzinfo=scheduler_tz)
            except TypeError:
                 # Fallback for older pytz versions if scheduler_tz is pytz
                 if hasattr(scheduler_tz, 'localize'):
                      aware_dt = scheduler_tz.localize(naive_dt)
                 else: # Could not make aware, proceed with naive (might be UTC if lucky)
                      aware_dt = naive_dt
                      logger.warning(f"Could not make datetime timezone-aware for scheduling: {naive_dt}. Assuming UTC or scheduler default.")

            run_date_utc = aware_dt.astimezone(datetime.timezone.utc) # Always convert to UTC for DateTrigger
            # *** END MODIFICATION ***
            trigger = DateTrigger(run_date=run_date_utc)
            flash_msg = f"for {aware_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')}" # Display in scheduler's TZ
            log_time_str = f"UTC: {run_date_utc.isoformat()}, SchedulerTZ: {aware_dt.isoformat()}"

        elif schedule_type == 'cron' and cron_expression:
            parts = cron_expression.split()
            if len(parts) != 5: raise ValueError("Invalid cron format (must have 5 parts).")
            trigger = CronTrigger.from_crontab(cron_expression, timezone=scheduler_tz)
            flash_msg = f"with cron: '{cron_expression}' ({scheduler_tz})"
            log_time_str = flash_msg

        elif schedule_type == 'interval' and interval_value:
             interval_val_int = int(interval_value)
             if interval_val_int < 1: raise ValueError("Interval must be >= 1.")
             kwargs = {interval_unit: interval_val_int}
             trigger = IntervalTrigger(**kwargs)
             flash_msg = f"every {interval_val_int} {interval_unit}"
             log_time_str = flash_msg
        else:
             flash('Invalid schedule details provided.', 'danger')
             return redirect(url_for('main.index'))

        # Add job
        job = scheduler.add_job(
            run_labbuild_task, trigger=trigger, args=[args_list],
            name=job_name, misfire_grace_time=3600, replace_existing=False
        )
        flash(f"Scheduled job '{job.id}' {flash_msg}", 'success')
        logger.info(f"Job '{job.id}' ({job_name}) scheduled. Trigger info: {log_time_str}")

    except ValueError as ve: logger.error(f"Invalid schedule input: {ve}", exc_info=True); flash(f"Invalid schedule input: {ve}", 'danger')
    except Exception as e: logger.error(f"Failed schedule job: {e}", exc_info=True); flash(f"Error scheduling job: {e}", 'danger')

    return redirect(url_for('main.index'))

@bp.route('/schedule-batch', methods=['POST'])
def schedule_batch():
    """Handle schedule batch run request from the main form."""
    # Logic moved from original app.py
    import io # Need io for TextIOWrapper
    import werkzeug.utils # For secure_filename

    # --- File Handling ---
    if 'batch_file' not in request.files: flash('No file part.', 'danger'); return redirect(url_for('main.index'))
    file = request.files['batch_file']
    if file.filename == '': flash('No file selected.', 'danger'); return redirect(url_for('main.index'))

    # --- Time/Delay Parsing ---
    start_time_str = request.form.get('start_time')
    delay_minutes_str = request.form.get('delay_minutes', '30')
    if not start_time_str: flash('Start time required for batch.', 'danger'); return redirect(url_for('main.index'))
    try: delay_minutes = int(delay_minutes_str); assert delay_minutes >= 1
    except (ValueError, AssertionError): flash('Invalid delay minutes (must be >= 1).', 'danger'); return redirect(url_for('main.index'))

    # --- Scheduler Check ---
    if not scheduler or not scheduler.running: flash("Scheduler not running.", "danger"); return redirect(url_for('main.index'))

    # --- Parse Start Time (using scheduler timezone) ---
    try:
        from apscheduler.triggers.date import DateTrigger # Import here
        scheduler_tz = scheduler.timezone
        naive_dt = datetime.datetime.fromisoformat(start_time_str)
        # *** MODIFIED TIMEZONE HANDLING ***
        try:
            # Make the naive datetime aware using the scheduler's timezone
            first_run_local = naive_dt.replace(tzinfo=scheduler_tz)
        except TypeError:
            # Fallback for older pytz versions if scheduler_tz is pytz
            if hasattr(scheduler_tz, 'localize'):
                 first_run_local = scheduler_tz.localize(naive_dt)
            else: # Could not make aware, proceed with naive (less ideal)
                 first_run_local = naive_dt
                 logger.warning(f"Could not make batch start time timezone-aware: {naive_dt}. Assuming UTC or scheduler default.")

        # Calculate subsequent run times in UTC for DateTrigger
        first_run_utc = first_run_local.astimezone(datetime.timezone.utc)
        # *** END MODIFICATION ***
        logger.info(f"Batch Schedule: First job UTC start: {first_run_utc} (from local input {first_run_local})")
    except Exception as e: logger.error(f"Err parse batch start: {e}", exc_info=True); flash("Error processing start time.", "danger"); return redirect(url_for('main.index'))

    # --- Process File ---
    scheduled_count, failed_lines = 0, 0
    current_run_time_utc = first_run_utc
    job_delay = datetime.timedelta(minutes=delay_minutes)
    filename = werkzeug.utils.secure_filename(file.filename)

    try:
        logger.info(f"Processing batch file: {filename}")
        stream = io.TextIOWrapper(file.stream, encoding='utf-8')
        lines = stream.readlines()

        for i, line in enumerate(lines):
            args_list = parse_command_line(line) # Use helper from utils
            if args_list:
                run_date_utc = current_run_time_utc
                job_name = f"batch_{filename}_{i+1}_{args_list[0]}" # Include command in name
                trigger = DateTrigger(run_date=run_date_utc)
                try:
                    job = scheduler.add_job( run_labbuild_task, trigger=trigger, args=[args_list], name=job_name, misfire_grace_time=3600, replace_existing=False )
                    run_date_local = run_date_utc.astimezone(scheduler_tz) # Format for logging
                    log_time_str = f"{run_date_local.strftime('%Y-%m-%d %H:%M:%S %Z%z')} / UTC: {run_date_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    logger.info(f"Scheduled batch job {i+1}: ID={job.id}, Name='{job_name}', RunAt={log_time_str}")
                    scheduled_count += 1; current_run_time_utc += job_delay
                except Exception as e_sched: logger.error(f"Fail schedule line {i+1}: {e_sched}", exc_info=True); failed_lines += 1
            elif line.strip() and not line.strip().startswith('#'): logger.warning(f"Skip invalid line {i+1}: {line.strip()}"); failed_lines += 1

        flash(f"Scheduled {scheduled_count} jobs from '{filename}'. {failed_lines} lines failed/skipped.", 'success' if scheduled_count > 0 else 'warning')
    except Exception as e: logger.error(f"Error processing batch file '{filename}': {e}", exc_info=True); flash(f"Error processing batch file: {e}", 'danger')

    return redirect(url_for('main.index'))


@bp.route('/jobs/delete/<job_id>', methods=['POST'])
def delete_job(job_id):
    """Delete a single scheduled job."""
    # Logic moved from original app.py
    if not scheduler or not scheduler.running: flash("Scheduler not running.", "danger"); return redirect(url_for('main.index'))
    try: scheduler.remove_job(job_id); flash(f"Job {job_id} deleted.", 'success')
    except Exception as e: logger.error(f"Failed delete job {job_id}: {e}", exc_info=True); flash(f"Error deleting job: {e}", 'danger')
    return redirect(url_for('main.index'))

@bp.route('/jobs/delete-bulk', methods=['POST'])
def delete_bulk_jobs():
    """Deletes multiple scheduled jobs based on selected IDs."""
    # Logic moved from original app.py
    if not scheduler or not scheduler.running: flash("Scheduler not running.", "danger"); return redirect(url_for('main.index'))
    job_ids_to_delete = request.form.getlist('job_ids')
    if not job_ids_to_delete: flash("No jobs selected for deletion.", "warning"); return redirect(url_for('main.index'))
    deleted_count, failed_count = 0, 0
    for job_id in job_ids_to_delete:
        try: scheduler.remove_job(job_id); deleted_count += 1
        except Exception as e: logger.error(f"Failed delete job {job_id} bulk: {e}", exc_info=True); failed_count += 1
    flash(f"Deleted {deleted_count} job(s). Failed: {failed_count} job(s).", 'success' if failed_count == 0 else 'warning')
    return redirect(url_for('main.index'))


@bp.route('/toggle-power', methods=['POST'])
def toggle_power():
    """Handles power toggle actions from the allocations page."""
    # Logic moved from original app.py
    scope = request.form.get('scope'); tag = request.form.get('tag')
    item_type = request.form.get('item_type'); item_number_str = request.form.get('item_number')
    host = request.form.get('host'); vendor = request.form.get('vendor'); course = request.form.get('course')
    item_class_number_str = request.form.get('item_class_number')

    if not scope or not tag: flash("Missing scope or tag for power toggle.", "danger"); return redirect(url_for('main.view_allocations', **request.args))
    logger.info(f"Power toggle request: Scope='{scope}', Tag='{tag}'")
    tasks_to_run = [] # List of tuples: (args_list, description, db_update_info)

    try:
        if db is None or alloc_collection is None: raise ConnectionError("Database unavailable.")
        if scope == 'tag':
            tag_doc = alloc_collection.find_one({"tag": tag});
            if not tag_doc: raise ValueError(f"Tag '{tag}' not found.")
            courses_in_tag = tag_doc.get("courses", [])
            if not isinstance(courses_in_tag, list): courses_in_tag = []
            for course_item in courses_in_tag:
                if not isinstance(course_item, dict): continue
                c_name, c_vendor, pod_details = course_item.get("course_name"), course_item.get("vendor"), course_item.get("pod_details", [])
                if not c_name or not c_vendor or not isinstance(pod_details, list): continue
                for pd in pod_details:
                    if not isinstance(pd, dict): continue
                    pd_host, pd_pod, pd_class = pd.get("host", pd.get("pod_host")), pd.get("pod_number"), pd.get("class_number")
                    current_power_on = str(pd.get("poweron", False)).lower() == 'true'
                    action = 'stop' if current_power_on else 'start'; new_power_state_bool = not current_power_on
                    if not pd_host: continue
                    is_f5 = c_vendor.lower() == 'f5'; item_detail = None
                    if is_f5 and pd_class is not None:
                        args = ['manage', '-v', c_vendor, '-g', c_name, '--host', pd_host, '-t', tag, '-o', action, '-cn', str(pd_class)]
                        desc = f"Tag '{tag}' - Manage {action} Class {pd_class}"; item_detail = (c_name, pd_host, pd_class, "f5_class", new_power_state_bool)
                    elif pd_pod is not None:
                        args = ['manage', '-v', c_vendor, '-g', c_name, '--host', pd_host, '-t', tag, '-o', action, '-s', str(pd_pod), '-e', str(pd_pod)]
                        if is_f5 and pd_class is not None: args.extend(['-cn', str(pd_class)])
                        desc = f"Tag '{tag}' - Manage {action} Pod {pd_pod}"; item_detail = (c_name, pd_host, pd_pod, "pod", new_power_state_bool)
                    if item_detail: tasks_to_run.append((args, desc, item_detail))
        elif scope == 'item':
            if not all([item_type, item_number_str, host, vendor, course]): raise ValueError("Missing item details.")
            item_number = int(item_number_str)
            current_state_is_on = request.form.get('current_state') == 'on'
            action = 'stop' if current_state_is_on else 'start'; new_power_state_bool = not current_state_is_on
            args = ['manage', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-o', action]
            item_detail = None; item_class_num = None
            if item_type == 'f5_class':
                args.extend(['-cn', str(item_number)]); desc = f"Item - Manage {action} Class {item_number}"
                item_detail = (course, host, item_number, "f5_class", new_power_state_bool)
            elif item_type == 'pod':
                args.extend(['-s', str(item_number), '-e', str(item_number)])
                if vendor.lower() == 'f5' and item_class_number_str: item_class_num = int(item_class_number_str); args.extend(['-cn', item_class_number_str])
                desc = f"Item - Manage {action} Pod {item_number}"; item_detail = (course, host, item_number, "pod", new_power_state_bool)
                if item_class_num is not None: item_detail = (course, host, item_number, "pod", new_power_state_bool, item_class_num)
            else: raise ValueError(f"Invalid item_type: {item_type}")
            if item_detail: tasks_to_run.append((args, desc, item_detail))
        else: raise ValueError(f"Invalid scope: {scope}")

        # --- Execute Tasks and Update DB ---
        if not tasks_to_run: flash(f"No valid manage tasks for Scope '{scope}', Tag '{tag}'.", "warning")
        else:
            logger.info(f"Submitting {len(tasks_to_run)} tasks for Scope '{scope}', Tag '{tag}'...")
            def run_sequential_manage_and_update(tasks):
                for i, (args, desc, db_info) in enumerate(tasks):
                    logger.info(f"Starting task {i+1}/{len(tasks)}: {desc}")
                    run_labbuild_task(args); logger.info(f"Finished task {i+1}/{len(tasks)}: {desc}")
                    if db_info:
                        try: update_power_state_in_db(tag, *db_info)
                        except Exception as db_err: logger.error(f"Failed DB update for {desc}: {db_err}")
                logger.info(f"All tasks submitted for Scope '{scope}', Tag '{tag}'.")
            thread = threading.Thread(target=run_sequential_manage_and_update, args=(tasks_to_run,), daemon=True)
            thread.start()
            flash(f"Submitted {len(tasks_to_run)} tasks for Scope '{scope}', Tag '{tag}'.", "info")

    except (ConnectionError, ValueError) as e: logger.error(f"Error during toggle for {tag}: {e}"); flash(f"Error: {e}", "danger")
    except Exception as e: logger.error(f"Unexpected error toggle for {tag}: {e}", exc_info=True); flash(f"Error submitting toggle: {e}", 'danger')

    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('main.view_allocations', **query_params)) # Redirect to main blueprint


@bp.route('/teardown-item', methods=['POST'])
def teardown_item():
    """Handles teardown/delete actions from the allocations page."""
    # Logic moved from original app.py
    try:
        tag = request.form.get('tag'); host = request.form.get('host'); vendor = request.form.get('vendor'); course = request.form.get('course')
        pod_num_str = request.form.get('pod_number'); class_num_str = request.form.get('class_number'); delete_level = request.form.get('delete_level')
        if not delete_level or not tag: flash("Missing delete level or tag.", "danger"); return redirect(url_for('main.view_allocations'))
        pod_num = int(pod_num_str) if pod_num_str else None; class_num = int(class_num_str) if class_num_str else None

        # --- Handle DB Deletion Levels ---
        if delete_level.endswith('_db'):
            logger.info(f"DB delete request: Level='{delete_level}', Tag='{tag}'")
            success, item_desc = False, "Unknown"
            try:
                if delete_level == 'tag_db': success = delete_from_database(tag=tag); item_desc = f"Tag '{tag}'"
                elif delete_level == 'course_db': success = delete_from_database(tag=tag, course_name=course); item_desc = f"Course '{course}' in Tag '{tag}'"
                elif delete_level == 'class_db': success = delete_from_database(tag=tag, course_name=course, class_number=class_num); item_desc = f"Class {class_num} in '{course}'/'{tag}'"
                elif delete_level == 'pod_db': success = delete_from_database(tag=tag, course_name=course, pod_number=pod_num, class_number=class_num); item_desc = f"Pod {pod_num}" + (f" (Class {class_num})" if class_num else "") + f" in '{course}'/'{tag}'"
                else: flash("Invalid DB delete level.", "warning"); return redirect(url_for('main.view_allocations'))
                flash(f"Removed DB entry for {item_desc}." if success else f"Failed DB removal for {item_desc}.", 'success' if success else 'danger')
            except Exception as e_db: flash(f"Error during DB delete: {e_db}", 'danger'); logger.error(f"DB delete error: {e_db}", exc_info=True)
            query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
            return redirect(url_for('main.view_allocations', **query_params))
        # --- Handle Full Teardown ---
        elif delete_level in ['class', 'pod']:
            if not all([vendor, course, host]): flash("Vendor, Course, Host required for infrastructure teardown.", "danger"); return redirect(url_for('main.view_allocations'))
            args_list, item_desc = [], ""
            if delete_level == 'class' and vendor.lower() == 'f5' and class_num is not None: args_list = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-cn', str(class_num)]; item_desc = f"F5 Class {class_num}"
            elif delete_level == 'pod' and pod_num is not None:
                args_list = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]
                item_desc = f"Pod {pod_num}"
                if vendor.lower() == 'f5' and class_num is not None: args_list.extend(['-cn', str(class_num)]); item_desc += f" (Class {class_num})"
            else: flash("Invalid teardown level or missing identifiers.", "danger"); return redirect(url_for('main.view_allocations'))
            if args_list:
                thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True); thread.start()
                flash(f"Submitted infrastructure teardown for {item_desc}.", 'info')
            else: flash("Failed build teardown command.", "danger")
        else: flash(f"Unsupported teardown level: '{delete_level}'.", "warning")
    except Exception as e: logger.error(f"Error processing teardown/delete request: {e}", exc_info=True); flash(f"Error processing request: {e}", 'danger')
    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('main.view_allocations', **query_params))


@bp.route('/teardown-tag', methods=['POST'])
def teardown_tag():
    """Handles teardown of an entire tag from the allocations page."""
    # Logic moved from original app.py
    from collections import defaultdict # Needed here
    tag = request.form.get('tag')
    if not tag: flash("Tag identifier is missing.", "danger"); return redirect(url_for('main.view_allocations'))
    logger.info(f"Initiating FULL teardown for Tag: '{tag}'")
    tasks_to_run = [] # List of ([args_list], description) tuples

    try:
        if db is None or alloc_collection is None: raise Exception("DB unavailable.")
        tag_doc = alloc_collection.find_one({"tag": tag})
        if not tag_doc: flash(f"Tag '{tag}' not found.", "warning"); return redirect(url_for('main.view_allocations'))

        courses_in_tag = tag_doc.get("courses", [])
        if not isinstance(courses_in_tag, list): courses_in_tag = []

        for course in courses_in_tag:
            if not isinstance(course, dict): continue
            course_name, vendor, pod_details = course.get("course_name"), course.get("vendor"), course.get("pod_details", [])
            if not course_name or not vendor or not isinstance(pod_details, list): continue
            items_by_host = defaultdict(lambda: {"pods": set(), "classes": set()})
            for pd in pod_details:
                if not isinstance(pd, dict): continue
                host, pod_num, class_num = pd.get("host", pd.get("pod_host")), pd.get("pod_number"), pd.get("class_number")
                if not host: continue
                is_f5 = vendor.lower() == 'f5'
                if is_f5 and class_num is not None: items_by_host[host]["classes"].add(class_num)
                elif pod_num is not None: items_by_host[host]["pods"].add(pod_num)

            for host, items in items_by_host.items():
                for class_num in sorted(list(items["classes"])): args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-cn', str(class_num)]; desc = f"Tag '{tag}', Course '{course_name}', Class '{class_num}'"; tasks_to_run.append((args, desc))
                if items["pods"]:
                    for pod_num in sorted(list(items["pods"])): args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]; desc = f"Tag '{tag}', Course '{course_name}', Pod '{pod_num}'"; tasks_to_run.append((args, desc))

        if not tasks_to_run: flash(f"No teardown tasks found for Tag '{tag}'.", "warning")
        else:
            logger.info(f"Submitting {len(tasks_to_run)} tasks for Tag '{tag}' sequentially...")
            def run_sequential_tasks(tasks):
                for i, (args, desc) in enumerate(tasks): logger.info(f"Starting task {i+1}/{len(tasks)}: {desc}"); run_labbuild_task(args); logger.info(f"Finished task {i+1}/{len(tasks)}: {desc}")
                logger.info(f"All teardown tasks submitted for Tag '{tag}'.")
            thread = threading.Thread(target=run_sequential_tasks, args=(tasks_to_run,), daemon=True); thread.start()
            flash(f"Submitted {len(tasks_to_run)} teardown tasks for Tag '{tag}'.", "info")
    except Exception as e: logger.error(f"Error processing teardown Tag '{tag}': {e}", exc_info=True); flash(f"Error submitting teardown Tag '{tag}': {e}", 'danger')

    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('main.view_allocations', **query_params))


@bp.route('/build-row', methods=['POST'])
def build_row():
    """Handles 'Build' action from the upcoming courses page."""
    # Logic moved from original app.py
    try:
        data = request.json
        if not data: return jsonify({"status": "error", "message": "No data received."}), 400
        labbuild_course, start_pod, end_pod, host, vendor = data.get('labbuild_course'), data.get('start_pod'), data.get('end_pod'), data.get('host'), data.get('vendor')
        sf_course_code = data.get('sf_course_code')
        if not all([labbuild_course, start_pod, end_pod, host, vendor]): return jsonify({"status": "error", "message": "Missing required fields."}), 400
        try: s_pod, e_pod = int(start_pod), int(end_pod); assert s_pod >= 0 and e_pod >= s_pod
        except (ValueError, AssertionError): return jsonify({"status": "error", "message": "Invalid Pod numbers."}), 400
        tag = f"uc_{sf_course_code}"[:50] if sf_course_code else "uc_dashboard"
        args_list = [ 'setup', '-v', vendor, '-g', labbuild_course, '--host', host, '-s', str(s_pod), '-e', str(e_pod), '-t', tag ]
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True); thread.start()
        # Use flash and redirect for UI feedback, or stick to JSON for pure AJAX
        flash(f"Submitted build for {labbuild_course} (Pods {s_pod}-{e_pod}) on {host}.", "info") # Example Flash
        return jsonify({"status": "success", "message": "Build submitted."}), 200
    except Exception as e: logger.error(f"Error in /build-row: {e}", exc_info=True); return jsonify({"status": "error", "message": "Internal server error."}), 500

@bp.route('/intermediate-build-review', methods=['POST'])
def intermediate_build_review():
    """
    Processes selected upcoming courses for build review and assignment.

    Handles the POST request from the upcoming courses page. It fetches build rules,
    current allocations, and host capacities to determine the optimal host and pod
    assignments for each selected course based on memory requirements, priority rules,
    and pod reusability. The results are saved (upserted) into the
    'interimallocation' collection and displayed on a review page.

    Workflow:
    1. Receives selected course data (POST request with JSON payload).
    2. Fetches build configuration rules from the 'build_rules' collection.
    3. Fetches non-extendable ('extend': 'false') current allocations to identify
       reusable pods and calculate the memory they would release.
    4. Fetches hosts marked for build inclusion from the 'host' collection.
    5. Fetches current available memory for these hosts using a parallel helper.
    6. Adjusts host available memory by adding back the calculated released memory.
    7. Iterates through each selected course from the input:
        a. Finds the highest-priority matching build rule based on vendor/course code/type.
        b. Determines the final LabBuild course name (user selection possibly overridden by rule).
        c. Calculates memory needed per pod for the final LabBuild course.
        d. Determines the prioritized list of potential hosts based on the matched rule or a default.
        e. Filters the host list based on available adjusted capacity.
        f. Assigns the required number of pods:
            i. Attempts to fill slots with reusable pods (matching vendor/host) first,
               checking memory and global availability (within this request).
            ii. If more pods are needed, assigns new pod numbers (starting from 1 or rule default),
               checking memory and global availability.
        g. Stores the detailed host-to-pod-list assignments or any assignment errors.
    8. Saves/Updates the processed course details (including assignments) into the
       'interimallocation' collection using a bulk upsert operation based on
       SF course code and final LabBuild course name.
    9. Renders the 'intermediate_build_review.html' template, passing the processed
       course data (including assignments or errors) for user review.

    Returns:
        Flask Response: Renders the review HTML page or redirects on error.
    """
    current_theme = request.cookies.get('theme', 'light')
    selected_courses_json = request.form.get('selected_courses')
    processed_courses_for_review = []
    save_error_flag = False
    # Track assignments globally within this request to avoid duplicates per vendor
    globally_assigned_pods_by_vendor = defaultdict(set)
    reusable_pods_by_vendor_host = defaultdict(lambda: defaultdict(set))
    released_memory_by_host = defaultdict(float)
    assignment_capacities_gb = {} # Adjusted capacity used during assignment
    build_rules = []
    available_hosts_db = []
    initial_host_capacities_gb = {}
    hosts_to_check_docs = []

    # --- Input Validation ---
    if not selected_courses_json:
        flash("No courses selected for review.", "warning")
        return redirect(url_for('main.view_upcoming_courses'))
    try:
        selected_courses_input = json.loads(selected_courses_json)
        if not isinstance(selected_courses_input, list) or not selected_courses_input:
             raise ValueError("Invalid course data format.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Invalid data received: {e}", "danger")
        logger.error(f"Failed to parse selected_courses JSON: {e}")
        return redirect(url_for('main.view_upcoming_courses'))

    # --- Main Processing Block ---
    try:
        # --- 1. Fetch Build Rules ---
        if build_rules_collection is not None:
            try:
                # Fetch rules sorted by priority (lower number = higher priority)
                rules_cursor = build_rules_collection.find().sort("priority", ASCENDING)
                build_rules = list(rules_cursor)
                if not build_rules: flash("No build rules defined.", "warning")
                logger.info(f"Loaded {len(build_rules)} build rules.")
            except PyMongoError as e:
                 logger.error(f"Error fetching build rules: {e}"); flash("Error loading build rules.", "danger")
        else: flash("Build rules collection unavailable.", "danger"); logger.error("build_rules_collection is None.")

        # --- 2. Fetch & Process Reusable Pods/Memory ---
        logger.info("Processing current allocations for reusable pods...")
        if alloc_collection is not None:
            try:
                logger.debug("Ensuring 'extend' field exists in currentallocation...")
                # Ensure 'extend' field exists (defaulting to false)
                alloc_collection.update_many({"extend": {"$exists": False}}, {"$set": {"extend": "false"}})
                logger.debug("Fetching allocations with 'extend': 'false'...")
                cursor = alloc_collection.find({"extend": "false"}, {"tag": 1,"courses": 1, "_id": 0}) # Fetch only courses for relevant tags

                for tag_doc in cursor:
                    tag_name = tag_doc.get("tag", "UNKNOWN")
                    logger.info(f"Processing reusable tag: '{tag_name}'")
                    if tag_name == "UNKNOWN":
                     logger.error(f"Found document with extend:false but missing 'tag' field! Doc: {tag_doc}")
                     continue
                    for course_alloc in tag_doc.get("courses", []):
                        vendor = course_alloc.get("vendor")
                        course_name = course_alloc.get("course_name")
                        if not vendor or not course_name: 
                            logger.warning(f"Skipping course in tag '{tag_name}': Missing vendor or course_name.")
                            continue
                        mem_per_pod = _get_memory_for_course(course_name)
                        if mem_per_pod <= 0: 
                            logger.warning(f"  Skipping course '{course_name}' in tag '{tag_name}': Cannot determine memory per pod ({mem_per_pod} GB). Check courseconfig.")
                            continue

                        for pod_detail in course_alloc.get("pod_details", []):
                            pod_num = pod_detail.get("pod_number"); host = pod_detail.get("host")
                            if pod_num is not None and host:
                                try:
                                    pod_num_int = int(pod_num)
                                    # Only add if not already marked as globally assigned (edge case protection)
                                    # 1. Check if pod is NOT already considered reusable for this host/vendor
                                    if pod_num_int not in reusable_pods_by_vendor_host[vendor][host]:
                                        # 2. If not present, THEN add it to the set
                                        reusable_pods_by_vendor_host[vendor][host].add(pod_num_int)
                                        # 3. Since it was newly added, increment the released memory
                                        released_memory_by_host[host] += mem_per_pod
                                except (ValueError, TypeError): pass # Ignore invalid pod numbers
                # Log summary
                for host, released_gb in released_memory_by_host.items():
                    print(f"Released: {released_gb}")
                    if released_gb > 0: logger.info(f"Host '{host}': Calculated {released_gb:.2f} GB releasable memory.")
            except PyMongoError as e: logger.error(f"DB Error processing current allocations: {e}"); flash("Error checking existing allocations.", "warning")
            except Exception as e: logger.error(f"Unexpected error calculating released memory: {e}", exc_info=True); flash("Error calculating released memory.", "warning")
        else: logger.warning("Current allocation collection unavailable.")

        # --- 3. Fetch Host Info & Initial Capacity ---
        if host_collection is not None:
             try:
                 hosts_cursor = host_collection.find({"include_for_build": "true"})
                 hosts_to_check_docs = list(hosts_cursor)
                 available_hosts_db = [h['host_name'] for h in hosts_to_check_docs if 'host_name' in h]
                 if not hosts_to_check_docs: flash("No hosts marked for build inclusion.", "warning")
             except PyMongoError as e: logger.error(f"Failed fetch host list: {e}"); flash("Could not retrieve host list.", "warning")
        else: flash("Host collection unavailable.", "danger"); logger.error("host_collection is None.")

        if hosts_to_check_docs: initial_host_capacities_gb = get_hosts_available_memory_parallel(hosts_to_check_docs)
        else: logger.warning("Skipping host memory check - no hosts marked for inclusion.")

        # --- 4. Adjust Host Capacity ---
        assignment_capacities_gb.clear()
        for host_name in available_hosts_db:
            initial_capacity = initial_host_capacities_gb.get(host_name)
            if initial_capacity is not None:
                released = released_memory_by_host.get(host_name, 0.0)
                adjusted_capacity = initial_capacity + released
                assignment_capacities_gb[host_name] = adjusted_capacity
                logger.info(f"Host '{host_name}': Initial={initial_capacity:.2f}GB + Released={released:.2f}GB = Adjusted={adjusted_capacity:.2f}GB")
            else: logger.warning(f"Excluding host '{host_name}' from assignment: capacity fetch failed.")

        # --- 5. Process Each Selected Course for Assignment ---
        logger.info("Starting host/pod assignment loop with deduplication factor: %.2f", SUBSEQUENT_POD_MEMORY_FACTOR)
        for course_input_data in selected_courses_input:
            # Extract data from the input posted from the previous page
            sf_course_code = course_input_data.get('sf_course_code', '')
            labbuild_course_selected = course_input_data.get('labbuild_course') # User's selection from dropdown
            vendor = course_input_data.get('vendor', '').lower()
            sf_course_type = course_input_data.get('sf_course_type', '') # Assuming this is passed now
            pods_req = max(1, int(course_input_data.get('sf_pods_req', 1))) # Assuming passed

            # Initialize for this course
            host_assignments_detail = defaultdict(list)
            error_determining_hosts = None
            pods_assigned_for_this_course = 0
            final_labbuild_course = labbuild_course_selected # Default to user selection

            # Find the best matching rule
            matched_rule, _ = _find_matching_build_rule(build_rules, vendor, sf_course_code, sf_course_type) # Ignore second return value for now

            # Determine final LabBuild course (rule overrides user selection)
            if matched_rule:
                rule_action_course = matched_rule.get("actions", {}).get("set_labbuild_course")
                if rule_action_course:
                    if rule_action_course != labbuild_course_selected:
                        logger.warning(f"Rule '{matched_rule.get('rule_name')}' overrides user selection: '{rule_action_course}' used for SF '{sf_course_code}'.")
                    final_labbuild_course = rule_action_course

            if not final_labbuild_course: # Check if we have a course name now
                 error_determining_hosts = "No LabBuild Course determined"
                 logger.error(f"Cannot assign pods for SF '{sf_course_code}': {error_determining_hosts}")

            # Get memory requirement for the FINAL course
            memory_gb_per_pod = 0
            if not error_determining_hosts:
                memory_gb_per_pod = _get_memory_for_course(final_labbuild_course)
                if memory_gb_per_pod <= 0:
                    error_determining_hosts = "Memory requirement unknown/invalid"
                    logger.warning(f"Cannot assign pods for '{final_labbuild_course}': {error_determining_hosts}")

            # --- Assign Hosts/Pods if possible ---
            if error_determining_hosts is None:
                total_memory_needed_gb = pods_req * memory_gb_per_pod
                logger.info(f"Course '{final_labbuild_course}' ({vendor}): Needs {pods_req} pods ({total_memory_needed_gb:.2f} GB total).")

                # Determine host priority from the matched rule or default
                priority_hosts = []
                allow_spillover = True
                start_pod_num_rule = 1
                if matched_rule and matched_rule.get("actions", {}).get("host_priority"):
                    priority_hosts = matched_rule["actions"]["host_priority"]
                    allow_spillover = matched_rule["actions"].get("allow_spillover", True)
                    start_pod_num_rule = matched_rule.get("actions",{}).get("start_pod_number", 1)
                else:
                    # Default fallback host pool logic
                    default_priority = ['unicron', 'ultramagnus', 'cliffjumper', 'nightbird', 'hotshot', 'apollo']
                    priority_hosts = [h for h in default_priority if h in assignment_capacities_gb] # Use adjusted capacities map
                    logger.warning(f"No specific host rule for '{final_labbuild_course}'. Using default pool: {priority_hosts}")

                # Filter hosts based on adjusted capacity available >= memory_gb_per_pod
                hosts_to_try_primary = [ h for h in priority_hosts if assignment_capacities_gb.get(h, 0.0) >= memory_gb_per_pod ]
                hosts_to_try = list(hosts_to_try_primary)

                # Add spillover if allowed and needed
                if allow_spillover:
                    spillover_hosts = sorted([ h for h in assignment_capacities_gb if h not in hosts_to_try and assignment_capacities_gb.get(h, 0.0) >= memory_gb_per_pod ])
                    if spillover_hosts: hosts_to_try.extend(spillover_hosts)
                logger.debug(f"Final assignment order for '{final_labbuild_course}': {hosts_to_try}")

                # --- Assignment Loop ---
                pods_to_assign = pods_req
                current_course_capacities = assignment_capacities_gb.copy() # Temp copy for this course

                for host_prio in hosts_to_try:
                    if pods_to_assign <= 0: break
                    available_mem_host = current_course_capacities.get(host_prio, 0.0)
                    pods_assigned_to_this_host_list = []

                    # 5a. Try REUSABLE pods
                    # Get reusable pods for this vendor/host, excluding globally assigned ones
                    candidate_reusable = sorted(list(
                        reusable_pods_by_vendor_host.get(vendor, {}).get(host_prio, set()) -
                        globally_assigned_pods_by_vendor.get(vendor, set())
                    ))
                    for reusable_pod_num in candidate_reusable:
                        if pods_to_assign <= 0: break
                        # *** Calculate memory needed considering deduplication ***
                        is_first_pod_on_host = not host_assignments_detail[host_prio]
                        memory_needed = memory_gb_per_pod if is_first_pod_on_host else (memory_gb_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR)
                        if available_mem_host >= memory_needed:
                            available_mem_host -= memory_needed
                            host_assignments_detail[host_prio].append(reusable_pod_num)
                            pods_to_assign -= 1
                            globally_assigned_pods_by_vendor[vendor].add(reusable_pod_num) # Mark globally
                        else: 
                            logger.debug(f"Host {host_prio} insufficient memory ({available_mem_host:.2f}GB) for reusable pod {reusable_pod_num} (needs ~{memory_needed:.2f}GB).")
                            break # No memory for reusable

                    # 5b. Assign NEW pods
                    if pods_to_assign > 0:
                        pod_num_candidate = start_pod_num_rule # Start from rule default or 1
                        while pods_to_assign > 0:
                            # Find next globally available new pod number for this vendor
                            while pod_num_candidate in globally_assigned_pods_by_vendor.get(vendor, set()):
                                pod_num_candidate += 1
                            
                            # *** Calculate memory needed considering deduplication ***
                            is_first_pod_on_host = not host_assignments_detail[host_prio]
                            memory_needed = memory_gb_per_pod if is_first_pod_on_host else (memory_gb_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR)

                            if available_mem_host >= memory_needed:
                                available_mem_host -= memory_needed
                                host_assignments_detail[host_prio].append(pod_num_candidate)
                                pods_to_assign -= 1
                                globally_assigned_pods_by_vendor[vendor].add(pod_num_candidate) # Mark globally
                                pod_num_candidate += 1
                            else: 
                                logger.debug(f"Host {host_prio} insufficient memory ({available_mem_host:.2f}GB) for new pod {pod_num_candidate} (needs ~{memory_needed:.2f}GB).")
                                break # No memory for new

                    # Record assignment for this host and update *master* capacity map
                    if host_prio in host_assignments_detail and host_prio in assignment_capacities_gb:
                     # Calculate total memory consumed on this host BY THIS COURSE (with dedupe)
                     pods_on_this_host_count = len(host_assignments_detail[host_prio])
                     mem_consumed_this_host_this_course = 0
                     if pods_on_this_host_count > 0:
                          mem_consumed_this_host_this_course = memory_gb_per_pod + (pods_on_this_host_count - 1) * memory_gb_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR

                     # Calculate the NEW remaining capacity by subtracting consumed from ORIGINAL adjusted capacity before this course started
                     original_adjusted_capacity = assignment_capacities_gb[host_prio] # Get capacity before this course touched it
                     new_remaining_capacity = original_adjusted_capacity - mem_consumed_this_host_this_course
                     assignment_capacities_gb[host_prio] = max(0, new_remaining_capacity) # Update master map

                     logger.info(f"Host '{host_prio}' capacity updated after assigning {pods_on_this_host_count} pods of '{final_labbuild_course}'. New tracked capacity: {assignment_capacities_gb[host_prio]:.2f} GB")
                # --- End Host Loop for Course ---

                # Check assignment success
                total_assigned_count = sum(len(pods) for pods in host_assignments_detail.values())
                if total_assigned_count < pods_req:
                    error_determining_hosts = f"Insufficient capacity/pods (assigned {total_assigned_count}/{pods_req})"
                    logger.error(f"Assignment failed for '{final_labbuild_course}': {error_determining_hosts}")
            # --- End Host/Pod Assignment Logic ---

            # --- Add processed course data for review ---
            processed_course = course_input_data.copy() # Use original input as base
            processed_course['labbuild_course'] = final_labbuild_course # Store final name
            # Format display string and store detailed pod map
            if error_determining_hosts:
                processed_course['host_assignments_display'] = f"Error: {error_determining_hosts}"
                processed_course['host_assignments_pods'] = {}
                processed_course['total_memory_gb'] = 0.0 
            elif not host_assignments_detail:
                processed_course['host_assignments_display'] = "Error: No suitable hosts found"
                processed_course['host_assignments_pods'] = {}
                processed_course['total_memory_gb'] = 0.0
            else:
                 display_parts = []
                 calculated_total_deduped_memory = 0.0
                 for h, nums in sorted(host_assignments_detail.items()):
                     nums.sort()
                     ranges = []
                     pods_on_this_host = len(nums)
                     memory_on_this_host = 0
                     if pods_on_this_host > 0:
                         memory_on_this_host = memory_gb_per_pod + max(0, pods_on_this_host - 1) * memory_gb_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR
                     calculated_total_deduped_memory += memory_on_this_host
                     if nums: # Generate compact ranges e.g., 1-3, 5
                         start = end = nums[0]
                         for i in range(1, len(nums)):
                             if nums[i] == end + 1: end = nums[i]
                             else: ranges.append(str(start) if start == end else f"{start}-{end}"); start = end = nums[i]
                         ranges.append(str(start) if start == end else f"{start}-{end}")
                     display_parts.append(f"{h}: Pods {', '.join(ranges)} ({memory_on_this_host:.1f} GB)")
                 processed_course['host_assignments_display'] = "; ".join(display_parts)
                 processed_course['host_assignments_pods'] = dict(host_assignments_detail) # Convert defaultdict back to dict for saving
                 processed_course['total_memory_gb'] = round(calculated_total_deduped_memory, 2)

            processed_courses_for_review.append(processed_course)
            # --- End Course Loop ---


        # --- 6. Upsert into interimallocation collection ---
        if interim_alloc_collection is None:
            flash("Interim DB unavailable.", "danger"); logger.error("interim_alloc_collection None."); save_error_flag = True
        elif not processed_courses_for_review: flash("No courses processed to save.", "warning")
        else:
            try:
                # *** STEP 1: Purge existing documents ***
                logger.info(f"Purging existing documents from '{interim_alloc_collection.name}' collection...")
                delete_result = interim_alloc_collection.delete_many({}) # Empty filter deletes all
                logger.info(f"Purged {delete_result.deleted_count} documents from interim collection.")
                # *** End Purge Step ***
                batch_timestamp = datetime.datetime.now(pytz.utc); bulk_ops = []
                for p_course in processed_courses_for_review:
                    # Define filter using SF code and FINAL labbuild course
                    filter_criteria = {
                        "sf_course_code": p_course.get("sf_course_code"),
                        "labbuild_course": p_course.get("labbuild_course"), # Use final name
                        "status": "pending_review"
                    }
                    filter_criteria = {k: v for k, v in filter_criteria.items() if v is not None and k is not None} # Ensure valid keys/values

                    # Define update data ($set overwrites)
                    update_data = {"$set": {
                        # Include all relevant SF fields passed in p_course
                        "sf_course_type": p_course.get('sf_course_type'), "sf_start_date": p_course.get('sf_start_date'),
                        "sf_end_date": p_course.get('sf_end_date'), "sf_trainer": p_course.get('sf_trainer'),
                        "sf_pax": p_course.get('sf_pax'), "sf_pods_req": p_course.get('sf_pods_req'),
                        # Include build specific fields
                        "vendor": p_course.get('vendor'),
                        "labbuild_course": p_course.get('labbuild_course'), # Ensure final name is set
                        "host_assignments_pods": p_course.get('host_assignments_pods', {}),
                        "host_assignments_display": p_course.get('host_assignments_display', ''),
                        "total_memory_gb": p_course.get('total_memory_gb'), # *** ADD MEMORY TO SAVE ***
                        "review_timestamp": batch_timestamp,
                        "status": "pending_review"
                     }}
                    bulk_ops.append(UpdateOne(filter_criteria, update_data, upsert=True))

                if bulk_ops:
                    logger.info(f"Performing {len(bulk_ops)} upsert ops into interimallocation.")
                    result = interim_alloc_collection.bulk_write(bulk_ops)
                    inserted = result.upserted_count # More accurate count for inserts via upsert
                    updated = result.matched_count - inserted # Estimate updated count
                    logger.info(f"Upsert result: Inserted={inserted}, Matched/Updated={updated}")
                    flash(f"{inserted} added, {updated} updated for review.", "success")
                else: flash("No course documents to save/update.", "warning")
            except BulkWriteError as bwe: logger.error(f"Bulk write error: {bwe.details}", exc_info=True); flash("DB bulk error.", "danger"); save_error_flag = True
            except PyMongoError as e: logger.error(f"Mongo error upserting: {e}", exc_info=True); flash("DB save error.", "danger"); save_error_flag = True
            except Exception as e: logger.error(f"Unexpected upsert error: {e}", exc_info=True); flash("Error saving review.", "danger"); save_error_flag = True

        # --- 7. Render the review template ---
        return render_template(
            'intermediate_build_review.html',
            selected_courses=processed_courses_for_review, # Pass final list
            save_error=save_error_flag,
            current_theme=current_theme
        )

    # --- Outer Exception Handling ---
    except Exception as e:
        logger.error(f"Critical error in intermediate_build_review route: {e}", exc_info=True)
        flash("An unexpected error occurred preparing the build review.", "danger")
        # Redirect back to the page where selection happened
        return redirect(url_for('main.view_upcoming_courses'))

@bp.route('/build-rules/add', methods=['POST'])
def add_build_rule():
    """Adds a new build rule to the collection."""
    if build_rules_collection is None:
        flash("Build rules database collection is unavailable.", "danger")
        return redirect(url_for('settings.view_build_rules')) # Redirect to settings blueprint

    try:
        rule_name = request.form.get('rule_name', '').strip()
        priority_str = request.form.get('priority', '').strip()
        conditions_json = request.form.get('conditions', '{}').strip()
        actions_json = request.form.get('actions', '{}').strip()

        # Basic Validation
        if not rule_name:
            flash("Rule Name is required.", "danger")
            return redirect(url_for('settings.view_build_rules'))
        try:
            priority = int(priority_str)
            if priority < 1: raise ValueError()
        except (ValueError, TypeError):
             flash("Priority must be a positive integer.", "danger")
             return redirect(url_for('settings.view_build_rules'))
        try:
            conditions = json.loads(conditions_json)
            if not isinstance(conditions, dict): raise ValueError()
        except (json.JSONDecodeError, ValueError):
             flash("Conditions field contains invalid JSON.", "danger")
             return redirect(url_for('settings.view_build_rules'))
        try:
            actions = json.loads(actions_json)
            if not isinstance(actions, dict): raise ValueError()
        except (json.JSONDecodeError, ValueError):
             flash("Actions field contains invalid JSON.", "danger")
             return redirect(url_for('settings.view_build_rules'))

        # Prepare document
        new_rule = {
            "rule_name": rule_name,
            "priority": priority,
            "conditions": conditions,
            "actions": actions,
            "created_at": datetime.datetime.now(pytz.utc) # Add timestamp
        }

        # Insert into database
        result = build_rules_collection.insert_one(new_rule)
        logger.info(f"Added new build rule '{rule_name}' with ID: {result.inserted_id}")
        flash(f"Successfully added build rule '{rule_name}'.", "success")

    except PyMongoError as e:
        logger.error(f"Database error adding build rule: {e}")
        flash("Database error adding rule.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error adding build rule: {e}", exc_info=True)
        flash("An unexpected error occurred.", "danger")

    return redirect(url_for('settings.view_build_rules'))


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

@bp.route('/toggle-tag-extend', methods=['POST'])
def toggle_tag_extend():
    """
    Toggles the 'extend' field (string "true"/"false") for a given tag
    in the 'currentallocation' collection based on the current state provided
    in the form submission.

    Redirects back to the allocations page, preserving any active filters
    and pagination settings.
    """
    tag_name = request.form.get('tag_name')
    # Get the status *as sent from the form* (state *before* the click)
    # Default to 'false' string if the input is missing, for safety.
    current_status_str = request.form.get('current_extend_status', 'false')

    # --- Preserve Filters & Pagination for Redirect ---
    # Collect all relevant query parameters that might have been passed back
    # from the allocations page form submission.
    redirect_args = {}
    for key in ['filter_tag', 'filter_vendor', 'filter_course', 'filter_host', 'filter_number', 'page', 'per_page']:
        # Use request.form for POST data, fallback to request.args for GET params if needed
        # Form submission usually puts everything in request.form for POST
        value = request.form.get(key)
        if value is not None: # Only include params that are present
             # Clean up filter keys if they have the prefix from the form
             clean_key = key.replace('filter_', '')
             redirect_args[clean_key] = value

    logger.info(f"Received toggle request for tag: '{tag_name}', current extend status from form: '{current_status_str}'")
    logger.debug(f"Redirect args collected: {redirect_args}")

    # --- Input Validation ---
    if not tag_name:
        flash("Tag name missing for toggle operation.", "danger")
        return redirect(url_for('main.view_allocations', **redirect_args))

    # --- Database Check ---
    if alloc_collection is None:
        flash("Allocation database collection is unavailable.", "danger")
        logger.error("alloc_collection is None in toggle_tag_extend.")
        return redirect(url_for('main.view_allocations', **redirect_args))

    # --- Determine the NEW status (opposite of current) ---
    try:
        # Convert received string status to boolean for logical flipping
        current_status_bool = current_status_str.lower() == 'true'
        # The new state is the opposite boolean
        new_status_bool = not current_status_bool
        # Convert the NEW state back to the STRING format ("true" or "false")
        # This must match the data type expected by DB queries/other logic.
        new_status_str = "true" if new_status_bool else "false"
    except Exception as e:
        logger.error(f"Error determining new status for tag '{tag_name}': {e}")
        flash("Internal error processing status toggle.", "danger")
        return redirect(url_for('main.view_allocations', **redirect_args))

    logger.info(f"Attempting to update tag '{tag_name}' extend status to: '{new_status_str}'")

    # --- Perform Database Update ---
    try:
        result = alloc_collection.update_one(
            {"tag": tag_name}, # Filter: Find the document by tag name
            {"$set": {"extend": new_status_str}} # Update: Set the 'extend' field
        )

        # --- Check Update Result and Provide Feedback ---
        logger.debug(f"MongoDB update result for tag '{tag_name}': Matched={result.matched_count}, Modified={result.modified_count}")

        if result.matched_count == 0:
            flash(f"Tag '{tag_name}' not found in database.", "warning")
            logger.warning(f"Tag '{tag_name}' not found during extend toggle update.")
        elif result.modified_count == 0:
             # Occurs if the document was found but the value was already the target value.
             flash(f"Tag '{tag_name}' extend status was not changed (already set to '{new_status_str}'?).", "info")
             logger.warning(f"Tag '{tag_name}' matched but extend status not modified. Value might already be '{new_status_str}'.")
        else:
            # Success Case
            new_status_display = "NO REUSE (Locked)" if new_status_str == "true" else "ALLOW REUSE (Unlocked)"
            flash(f"Tag '{tag_name}' updated. Pod reuse policy set to: {new_status_display}.", "success")
            logger.info(f"Successfully toggled extend status for tag '{tag_name}' to '{new_status_str}'.")

    except PyMongoError as e:
        logger.error(f"Database error toggling extend status for tag '{tag_name}': {e}", exc_info=True)
        flash("Database error updating tag status.", "danger")
    except Exception as e:
        logger.error(f"Unexpected error toggling extend status for tag '{tag_name}': {e}", exc_info=True)
        flash("An unexpected error occurred.", "danger")

    # --- Redirect back to the allocations page, preserving filters & pagination ---
    # The **redirect_args unpacks the dictionary into keyword arguments for url_for
    return redirect(url_for('main.view_allocations', **redirect_args))