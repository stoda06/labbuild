# dashboard/routes/actions.py

import logging
import threading
import datetime
import pytz
import re
import json
from flask import (
    Blueprint, request, redirect, url_for, flash, current_app, jsonify, render_template
)
from pymongo.errors import PyMongoError, BulkWriteError
import pymongo
from pymongo import ASCENDING, UpdateOne
from collections import defaultdict
from typing import List, Dict, Optional, Tuple, Any
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

def _find_all_matching_rules(rules: list, vendor: str, code: str, type_str: str) -> list[dict]:
    """
    Finds all build rules matching the course criteria.
    Assumes input 'rules' list is already sorted by priority (ascending).
    """
    matching_rules_list = []
    # Normalize inputs for case-insensitive matching
    cc_lower = code.lower() if code else ""
    ct_lower = type_str.lower() if type_str else ""
    v_lower = vendor.lower() if vendor else ""

    for rule in rules: 
        conditions = rule.get("conditions", {})
        
        match = True # Assume match until a condition fails

        # 1. Vendor check (must match if specified in rule)
        rule_vendor_cond = conditions.get("vendor")
        if rule_vendor_cond and rule_vendor_cond.lower() != v_lower:
            match = False
        
        # 2. Course Code Contains check (only if match is still true)
        if match and "course_code_contains" in conditions:
            terms = conditions["course_code_contains"]
            terms = [terms] if not isinstance(terms, list) else terms # Ensure list
            if not any(str(t).lower() in cc_lower for t in terms):
                match = False
        
        # 3. Course Code NOT Contains check (only if match is still true)
        if match and "course_code_not_contains" in conditions:
            terms = conditions["course_code_not_contains"]
            terms = [terms] if not isinstance(terms, list) else terms # Ensure list
            if any(str(t).lower() in cc_lower for t in terms):
                match = False

        # 4. Course Type Contains check (only if match is still true)
        if match and "course_type_contains" in conditions:
            terms = conditions["course_type_contains"]
            terms = [terms] if not isinstance(terms, list) else terms # Ensure list
            if not any(str(t).lower() in ct_lower for t in terms):
                match = False
        
        if match: # If all specific conditions passed (or no specific conditions to fail), add the rule
            matching_rules_list.append(rule)
            
    return matching_rules_list


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

def _format_date_for_review(raw_date_str: Optional[str], context: str) -> str:
    # ... (Implementation from previous responses) ...
    if not raw_date_str or raw_date_str == "N/A": return "N/A"
    try: datetime.datetime.strptime(raw_date_str, "%Y-%m-%d"); return raw_date_str
    except ValueError:
        for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"):
            try: return datetime.datetime.strptime(raw_date_str, fmt).strftime("%Y-%m-%d")
            except ValueError: continue
        logger.warning(f"Could not parse date '{raw_date_str}' for {context}, keeping original.")
        return raw_date_str

@bp.route('/intermediate-build-review', methods=['POST'])
def intermediate_build_review():
    """
    Processes selected Salesforce courses, auto-assigns student pods respecting
    locked (extend:true) and prioritizing reusable (extend:false) pods,
    saves proposals to `interimallocation`, and renders the review page.
    """
    current_theme = request.cookies.get('theme', 'light')
    selected_courses_json = request.form.get('selected_courses')

    processed_courses_for_student_review: List[Dict] = []
    build_rules: List[Dict] = []
    all_available_host_names: List[str] = []
    initial_host_capacities_gb: Dict[str, Optional[float]] = {}
    db_locked_pods: Dict[str, set] = defaultdict(set)
    db_reusable_candidates_by_vendor_host: Dict[str, Dict[str, set]] = \
        defaultdict(lambda: defaultdict(set))
    released_memory_by_host: Dict[str, float] = defaultdict(float)
    host_memory_before_batch_assignment: Dict[str, float] = {}
    course_configs_for_memory_lookup: List[Dict] = []
    pods_assigned_in_this_batch: Dict[str, set] = defaultdict(set)

    if not selected_courses_json:
        flash("No courses selected for review.", "warning")
        return redirect(url_for('main.view_upcoming_courses'))
    try:
        selected_courses_input = json.loads(selected_courses_json)
        if not isinstance(selected_courses_input, list) or \
           not selected_courses_input:
            raise ValueError("Invalid course data: Expected non-empty list.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Invalid data received for review: {e}", "danger")
        logger.error("Failed to parse selected_courses JSON: %s", e,
                       exc_info=True)
        return redirect(url_for('main.view_upcoming_courses'))

    if interim_alloc_collection is None:
        logger.critical("interim_alloc_collection is None. Cannot proceed.")
        flash("Critical error: Interim DB unavailable.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    # --- 1. Preparatory Data Fetching ---
    try:
        if build_rules_collection is not None:
            build_rules = list(build_rules_collection.find().sort(
                "priority", ASCENDING
            ))
        else:
            logger.warning("Build rules collection is unavailable.")

        if alloc_collection is not None:
            logger.info("Pre-loading existing allocations from DB "
                        "to determine locked and reusable pods.")
            # Ensure 'extend' field defaults if missing
            alloc_collection.update_many(
                {"extend": {"$exists": False}}, {"$set": {"extend": "false"}}
            )
            db_allocs_cursor = alloc_collection.find(
                {},
                {"courses.vendor": 1, "courses.course_name": 1,
                 "courses.pod_details.host": 1,
                 "courses.pod_details.pod_number": 1,
                 "courses.pod_details.class_number": 1,
                 "courses.pod_details.pods.pod_number": 1,
                 "extend": 1, "_id": 0}
            )
            # Use a temporary set to track which pod_ids had their memory counted for release
            # to avoid double-counting if a pod appears in multiple course entries under same tag (unlikely but possible)
            _counted_for_release_mem_tracker = set()

            for tag_doc_db in db_allocs_cursor:
                is_tag_locked = str(tag_doc_db.get("extend", "false")).lower() == "true"
                for course_alloc_db in tag_doc_db.get("courses", []):
                    vendor = course_alloc_db.get("vendor", "").lower()
                    lb_course = course_alloc_db.get("course_name")
                    if not vendor or not lb_course:
                        continue
                    mem_pod = _get_memory_for_course(lb_course)

                    for pd_db in course_alloc_db.get("pod_details", []):
                        host = pd_db.get("host")
                        ids_in_detail: List[int] = []
                        pod_num = pd_db.get("pod_number")
                        if pod_num is not None:
                            try: ids_in_detail.append(int(pod_num))
                            except (ValueError, TypeError): pass
                        if vendor == 'f5': # F5 specific identifiers
                            class_num = pd_db.get("class_number")
                            if class_num is not None:
                                try: ids_in_detail.append(int(class_num))
                                except (ValueError, TypeError): pass
                            for np_item in pd_db.get("pods", []):
                                np_num_val = np_item.get("pod_number")
                                if np_num_val is not None:
                                    try: ids_in_detail.append(int(np_num_val))
                                    except (ValueError, TypeError): pass
                        
                        for item_id in set(ids_in_detail): # Process unique IDs from this detail
                            if is_tag_locked:
                                db_locked_pods[vendor].add(item_id)
                            elif host and mem_pod > 0: # extend is "false" for the tag
                                db_reusable_candidates_by_vendor_host[vendor][host].add(item_id)
                                mem_tracker_key = f"{vendor}_{host}_{item_id}"
                                if mem_tracker_key not in _counted_for_release_mem_tracker:
                                    released_memory_by_host[host] += mem_pod
                                    _counted_for_release_mem_tracker.add(mem_tracker_key)
        else:
            logger.warning("alloc_collection is None. Cannot determine "
                           "locked/reusable pods from existing DB.")
        
        if host_collection is not None:
            hosts_docs = list(host_collection.find({"include_for_build": "true"}))
            all_available_host_names = sorted([
                h['host_name'] for h in hosts_docs if 'host_name' in h
            ])
            if hosts_docs:
                initial_host_capacities_gb = get_hosts_available_memory_parallel(
                    hosts_docs
                )
            for h_doc in hosts_docs:
                h_name = h_doc.get("host_name")
                vc_cap = initial_host_capacities_gb.get(h_name)
                if h_name and vc_cap is not None:
                    rel_mem = released_memory_by_host.get(h_name, 0.0)
                    host_memory_before_batch_assignment[h_name] = \
                        round(vc_cap + rel_mem, 2)
        else:
            logger.warning("host_collection is None. Cannot get host capacities.")

        if course_config_collection is not None:
            course_configs_for_memory_lookup = list(course_config_collection.find(
                {}, {"course_name": 1, "memory_gb_per_pod": 1,
                     "memory": 1, "_id": 0}
            ))
        else:
            logger.warning("course_config_collection is None for JS memory lookup.")

    except PyMongoError as e_fetch:
        logger.error("DB error during initial data fetch: %s", e_fetch,
                       exc_info=True)
        flash("Error fetching configuration data. Cannot proceed.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))
    except Exception as e_generic_prep: # Catch other errors during prep
        logger.error("Unexpected error during data preparation: %s", e_generic_prep,
                       exc_info=True)
        flash("An unexpected server error occurred during data preparation.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    assignment_capacities_gb = host_memory_before_batch_assignment.copy()
    logger.debug("--- INITIAL State for Batch Review ---")
    logger.debug(f"  DB Locked Pods (extend:true): {dict(db_locked_pods)}")
    logger.debug(f"  DB Reusable Candidates (extend:false): "
                 f"{ {v: dict(h) for v, h in db_reusable_candidates_by_vendor_host.items()} }")
    logger.debug(f"  Host Mem Before Batch (incl. DB released): "
                 f"{host_memory_before_batch_assignment}")
    logger.debug(f"  Starting Assignment Capacities for Batch: "
                 f"{assignment_capacities_gb}")

    batch_review_id = str(ObjectId())
    try:
        if interim_alloc_collection is not None:
            del_res = interim_alloc_collection.delete_many(
                {"status": {"$regex": "^pending_"}}
            ) # Clear only pending from potentially previous failed sessions
            logger.info(f"Cleared {del_res.deleted_count} old pending review "
                        f"items. New batch_review_id: {batch_review_id}")
    except PyMongoError as e_clear_interim:
        logger.error("Error clearing old interim items for batch %s: %s",
                       batch_review_id, e_clear_interim, exc_info=True)
        # Non-fatal, proceed but log

    bulk_interim_ops: List[UpdateOne] = []

    # --- Process Each Selected SF Course for Student Pod Auto-Assignment ---
    for course_idx, course_input_data in enumerate(selected_courses_input): # type: ignore
        sf_code = course_input_data.get(
            'sf_course_code', f'SF_UNKN_{str(ObjectId())[:4]}'
        )
        user_lb_course = course_input_data.get('labbuild_course')
        vendor = course_input_data.get('vendor', '').lower()
        sf_type = course_input_data.get('sf_course_type', '')
        sf_pods_req_orig = max(1, int(course_input_data.get('sf_pods_req', 1)))

        assigned_hosts_pods_map_student = defaultdict(list)
        student_assignment_warning: Optional[str] = None
        final_lb_course_student = user_lb_course

        eff_actions_student: Dict[str, Any] = {}
        rules = _find_all_matching_rules(
            build_rules, vendor, sf_code, sf_type
        )
        action_keys = ["set_labbuild_course", "host_priority",
                       "allow_spillover", "set_max_pods", "start_pod_number"]
        for r_item in rules: # PEP 8: r_item instead of r
            acts = r_item.get("actions", {})
            for k_item in action_keys: # PEP 8: k_item instead of k
                if k_item not in eff_actions_student and k_item in acts:
                    eff_actions_student[k_item] = acts[k_item]
        
        if "set_labbuild_course" in eff_actions_student:
            final_lb_course_student = eff_actions_student["set_labbuild_course"]
        if not final_lb_course_student:
            student_assignment_warning = "No LabBuild Course. Manual selection."
        
        mem_per_student_pod = 0.0
        if not student_assignment_warning and final_lb_course_student:
            mem_per_student_pod = _get_memory_for_course(final_lb_course_student)
            if mem_per_student_pod <= 0:
                student_assignment_warning = (
                    (student_assignment_warning + ". " if student_assignment_warning else "") +
                    f"Memory for LB '{final_lb_course_student}' is 0."
                )
        
        eff_pods_req_student = sf_pods_req_orig
        if "set_max_pods" in eff_actions_student and \
           eff_actions_student["set_max_pods"] is not None:
            try:
                max_p_rule = int(eff_actions_student["set_max_pods"])
                if max_p_rule > 0:
                    eff_pods_req_student = min(sf_pods_req_orig, max_p_rule)
            except (ValueError, TypeError):
                logger.warning(f"Invalid 'set_max_pods' rule for {sf_code}")

        if not student_assignment_warning and mem_per_student_pod > 0 and \
           eff_pods_req_student > 0:
            logger.info(f"--- Auto-Assign Student Pods for SF '{sf_code}', "
                        f"LB '{final_lb_course_student}' ---")
            logger.info(f"  Requires: {eff_pods_req_student} pods. "
                        f"Mem/Pod: {mem_per_student_pod:.2f}GB.")

            hosts_to_try_student: List[str] = []
            priority_h_rule = eff_actions_student.get("host_priority", [])
            if isinstance(priority_h_rule, list):
                hosts_to_try_student.extend(
                    [h for h in priority_h_rule if h in assignment_capacities_gb]
                )
            if eff_actions_student.get("allow_spillover", True):
                hosts_to_try_student.extend(sorted(
                    [h_n for h_n in assignment_capacities_gb
                     if h_n not in hosts_to_try_student]
                ))
            if not hosts_to_try_student and all_available_host_names:
                 hosts_to_try_student.append(all_available_host_names[0])
                 warn = "No hosts by rule/cap; using default. VERIFY."
                 student_assignment_warning = \
                     (student_assignment_warning + ". " if student_assignment_warning else "") + warn
            elif not hosts_to_try_student: # No hosts at all
                 warn = "No hosts available for student assignment."
                 student_assignment_warning = \
                     (student_assignment_warning + ". " if student_assignment_warning else "") + warn
            
            logger.debug(f"  Hosts to try for '{sf_code}': {hosts_to_try_student}")
            pods_left_to_assign = eff_pods_req_student

            if hosts_to_try_student:
                start_pod_cfg = 1
                try:
                    sp_val_cfg = eff_actions_student.get("start_pod_number")
                    if sp_val_cfg is not None:
                        start_pod_cfg = max(1, int(sp_val_cfg))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid start_pod_number rule for {sf_code}")

                # Use a copy of main capacities for this course's simulation
                # This allows trying different hosts for the same course without
                # prematurely depleting capacity that another part of this course might use on another host.
                temp_host_caps_for_course_iteration = assignment_capacities_gb.copy()

                for host_idx, host_target in enumerate(hosts_to_try_student):
                    if pods_left_to_assign <= 0:
                        break
                    
                    # Capacity on this host *before* this course attempts to use it for this segment
                    current_host_iter_cap = temp_host_caps_for_course_iteration.get(host_target, 0.0)
                    logger.debug(f"  Try Host {host_idx+1}: '{host_target}' "
                                 f"(Capacity for this course iteration: {current_host_iter_cap:.2f}GB)")

                    # --- ATTEMPT TO ASSIGN NEW CONTIGUOUS PODS FIRST ---
                    assigned_new_on_this_host_for_course = 0
                    if pods_left_to_assign > 0:
                        logger.debug(f"    Trying NEW pods first for '{sf_code}' on '{host_target}' "
                                     f"starting from candidate {start_pod_cfg}.")
                        cand_new_pod = start_pod_cfg
                        
                        temp_new_pods_segment: List[int] = [] # Pods for this contiguous segment

                        while pods_left_to_assign > 0: # Try to assign all remaining
                            # Find next available NEW pod number
                            skipped_log_count = 0
                            initial_cand_for_log = cand_new_pod
                            while True:
                                is_db_locked = cand_new_pod in db_locked_pods.get(vendor, set())
                                is_batch_taken = cand_new_pod in pods_assigned_in_this_batch.get(vendor, set())
                                if not is_db_locked and not is_batch_taken:
                                    break
                                if skipped_log_count < 5 or cand_new_pod % 10 == 0:
                                    logger.debug(f"      New pod candidate {cand_new_pod} for '{vendor}' skipped. "
                                                 f"DBLock:{is_db_locked}, BatchTake:{is_batch_taken}.")
                                skipped_log_count +=1
                                cand_new_pod += 1
                            if skipped_log_count > 0 and cand_new_pod != initial_cand_for_log:
                                logger.debug(f"      Found free new pod candidate {cand_new_pod} after skipping.")

                            is_first_pod_segment_on_host = not assigned_hosts_pods_map_student.get(host_target) and not temp_new_pods_segment
                            mem_needed = mem_per_student_pod if is_first_pod_segment_on_host else \
                                         (mem_per_student_pod * SUBSEQUENT_POD_MEMORY_FACTOR)
                            
                            logger.debug(f"      Considering NEW pod {cand_new_pod}. Need: {mem_needed:.2f}GB. "
                                         f"HostIterCap: {current_host_iter_cap:.2f}GB. FirstSeg? {is_first_pod_segment_on_host}")

                            if current_host_iter_cap >= mem_needed:
                                temp_new_pods_segment.append(cand_new_pod)
                                current_host_iter_cap -= mem_needed # Reduce iteration capacity
                                pods_left_to_assign -= 1
                                assigned_new_on_this_host_for_course += 1
                                logger.info(f"      TENTATIVELY ASSIGNED NEW Pod {cand_new_pod} to '{host_target}' for '{sf_code}'. Left for course: {pods_left_to_assign}")
                                cand_new_pod += 1 # Try next for contiguous block
                            else:
                                logger.debug(f"      Host '{host_target}' iter cap insufficient for new pod {cand_new_pod}.")
                                break # No more capacity on THIS host for more NEW pods

                        # Commit the new pods assigned in this segment
                        if temp_new_pods_segment:
                            assigned_hosts_pods_map_student[host_target].extend(temp_new_pods_segment)
                            for p_num in temp_new_pods_segment:
                                pods_assigned_in_this_batch[vendor].add(p_num)
                    
                    # --- THEN TRY REUSABLE PODS TO FILL GAPS (if still pods_left_to_assign > 0) ---
                    if pods_left_to_assign > 0:
                        logger.debug(f"    Still need {pods_left_to_assign} pods for '{sf_code}' on '{host_target}'. Trying REUSABLE.")
                        db_reusables_on_host = sorted(list(
                            db_reusable_candidates_by_vendor_host.get(vendor, {}).get(host_target, set())
                        ))
                        for r_pod in db_reusables_on_host:
                            if pods_left_to_assign <= 0: break
                            if r_pod in pods_assigned_in_this_batch.get(vendor, set()):
                                continue # Already used by another course in this batch

                            is_first = not assigned_hosts_pods_map_student.get(host_target) # First for this course on this host
                            mem_needed = mem_per_student_pod if is_first else (mem_per_student_pod * SUBSEQUENT_POD_MEMORY_FACTOR)
                            if current_host_iter_cap >= mem_needed:
                                assigned_hosts_pods_map_student[host_target].append(r_pod)
                                pods_assigned_in_this_batch[vendor].add(r_pod)
                                current_host_iter_cap -= mem_needed
                                pods_left_to_assign -= 1
                                logger.info(f"      ASSIGNED REUSABLE(DB extend:false) Pod {r_pod} to '{host_target}' for '{sf_code}'. Left: {pods_left_to_assign}")
                
                    temp_host_caps_for_course_iteration[host_target] = current_host_iter_cap # Update iteration cap

            # After trying all hosts for THIS course, update the *actual global* assignment_capacities_gb
            if assigned_hosts_pods_map_student:
                for h_assigned, p_list_assigned in assigned_hosts_pods_map_student.items():
                    if p_list_assigned:
                        num_on_h = len(p_list_assigned)
                        mem_consumed_by_course = (mem_per_student_pod + (num_on_h - 1) * mem_per_student_pod * SUBSEQUENT_POD_MEMORY_FACTOR) if num_on_h > 0 else 0
                        assignment_capacities_gb[h_assigned] = max(0, round(assignment_capacities_gb.get(h_assigned, 0.0) - mem_consumed_by_course, 2))
                        logger.info(f"  Host '{h_assigned}' main capacity updated to: {assignment_capacities_gb[h_assigned]:.2f}GB after '{sf_code}'.")

            total_assigned_stud = sum(len(p_list) for p_list in assigned_hosts_pods_map_student.values())
            if total_assigned_stud < eff_pods_req_student:
                warn = f"Auto-assigned {total_assigned_stud}/{eff_pods_req_student} student pods. Review."
                student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + warn
            elif not assigned_hosts_pods_map_student and eff_pods_req_student > 0:
                warn = f"No student pods auto-assigned (req: {eff_pods_req_student}). Manual."
                student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + warn
        
        # --- Prepare interim_student_data and add to bulk_interim_ops ---
        interim_doc_id = ObjectId()
        interim_student_data = {
            "_id": interim_doc_id, "batch_review_id": batch_review_id,
            "sf_course_code": sf_code,
            "sf_course_type": sf_type,
            "sf_start_date": _format_date_for_review(course_input_data.get('sf_start_date'), sf_code),
            "sf_end_date": _format_date_for_review(course_input_data.get('sf_end_date'), sf_code),
            "sf_pods_req_original": sf_pods_req_orig,
            "vendor": vendor,
            "final_labbuild_course_student": final_lb_course_student,
            "memory_gb_one_student_pod": mem_per_student_pod,
            "effective_pods_req_student": eff_pods_req_student,
            "student_assignments_auto_proposed": dict(assigned_hosts_pods_map_student),
            "student_assignment_warning": student_assignment_warning,
            "initial_interactive_sub_rows_student": [], # Populated below
            "status": "pending_student_review",
            "created_at": datetime.datetime.now(pytz.utc),
            "updated_at": datetime.datetime.now(pytz.utc),
            "trainer_labbuild_course": None,
            "trainer_memory_gb_one_pod": None,
            "trainer_assignment_auto_proposed": None,
            "trainer_assignment_final": None,
            "trainer_assignment_warning": None
        }
        
        initial_student_rows = []
        if assigned_hosts_pods_map_student:
            for h, p_list in assigned_hosts_pods_map_student.items():
                if p_list:
                    sorted_p = sorted(list(set(p_list))) # Ensure unique and sorted
                    if not sorted_p: continue
                    s_val, e_val = sorted_p[0], sorted_p[0]
                    for i_val_idx in range(1, len(sorted_p)):
                        if sorted_p[i_val_idx] == e_val + 1:
                            e_val = sorted_p[i_val_idx]
                        else: 
                            initial_student_rows.append({"host": h, "start_pod": s_val, "end_pod": e_val})
                            s_val = e_val = sorted_p[i_val_idx]
                    initial_student_rows.append({"host": h, "start_pod": s_val, "end_pod": e_val})
        
        if not initial_student_rows and eff_pods_req_student > 0: # Fallback default row if no assignments
            def_h_stud = None
            if isinstance(eff_actions_student.get("host_priority"), list) and \
               eff_actions_student.get("host_priority"):
                def_h_stud = eff_actions_student["host_priority"][0]
                if def_h_stud not in all_available_host_names:
                    def_h_stud = all_available_host_names[0] if all_available_host_names else None
            elif all_available_host_names:
                def_h_stud = all_available_host_names[0]
            
            def_s_stud = 1
            try:
                sp_val_fb = eff_actions_student.get("start_pod_number")
                def_s_stud = max(1, int(sp_val_fb)) if sp_val_fb is not None else 1
            except (ValueError, TypeError): pass
            def_e_stud = max(def_s_stud, def_s_stud + eff_pods_req_student - 1)
            initial_student_rows.append(
                {"host": def_h_stud, "start_pod": def_s_stud, "end_pod": def_e_stud}
            )
        
        interim_student_data["initial_interactive_sub_rows_student"] = initial_student_rows
        
        display_course_data = interim_student_data.copy()
        display_course_data['interim_doc_id'] = str(interim_doc_id)
        display_course_data['sf_pods_req'] = eff_pods_req_student # For template display
        processed_courses_for_student_review.append(display_course_data)

        bulk_interim_ops.append(
            UpdateOne({"_id": interim_doc_id}, {"$set": interim_student_data}, upsert=True)
        )
    # --- End SF Course Loop ---

    logger.debug("--- FINAL State after batch processing for review ---")
    logger.debug(f"  Pods Assigned In This Batch: {dict(pods_assigned_in_this_batch)}")
    logger.debug(f"  Final Assignment Capacities (after this batch): {dict(assignment_capacities_gb)}")

    if bulk_interim_ops:
        try:
            if interim_alloc_collection is not None:
                result_db_interim = interim_alloc_collection.bulk_write(bulk_interim_ops)
                logger.info(f"Interim student proposals saved to batch '{batch_review_id}': "
                            f"Upserted: {result_db_interim.upserted_count}, "
                            f"Modified: {result_db_interim.modified_count}")
            else:
                raise PyMongoError("interim_alloc_collection is None before bulk_write.")
        except (PyMongoError, BulkWriteError) as e_save_interim_s:
            logger.error(f"Error saving student proposals to interim DB for batch "
                         f"'{batch_review_id}': {e_save_interim_s}", exc_info=True)
            flash("Error saving initial review session data. Please try again.", "danger")
            return redirect(url_for('main.view_upcoming_courses'))
    
    return render_template(
        'intermediate_build_review.html',
        selected_courses=processed_courses_for_student_review,
        batch_review_id=batch_review_id,
        current_theme=current_theme,
        all_hosts=all_available_host_names,
        course_configs_for_memory=course_configs_for_memory_lookup,
        subsequent_pod_memory_factor=SUBSEQUENT_POD_MEMORY_FACTOR
    )


@bp.route('/prepare-trainer-pods', methods=['POST'])
def prepare_trainer_pods():
    """
    1. Receives finalized student assignments and `batch_review_id`.
    2. Updates student assignments in `interimallocation` to 'student_confirmed'.
    3. Performs auto-assignment for TRAINER pods based on confirmed student plan
       and current capacities/rules.
    4. Updates `interimallocation` documents with trainer pod proposals and
       sets status to 'pending_trainer_review'.
    5. Renders `trainer_pod_review.html` for user modification of trainer assignments.
    """
    current_theme = request.cookies.get('theme', 'light')
    final_student_plan_json = request.form.get('final_build_plan')
    batch_review_id = request.form.get('batch_review_id')

    if not batch_review_id:
        flash("Review session ID missing. Cannot prepare trainer pods.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))
    if not final_student_plan_json:
        flash("No finalized student build plan data received.", "danger")
        # Try to redirect back to intermediate review, passing batch_review_id if possible
        # This depends on how intermediate_build_review handles GET requests or if it needs POST
        return redirect(url_for('main.view_upcoming_courses')) # Simplified for now

    try:
        finalized_student_data_from_form = json.loads(final_student_plan_json)
        if not isinstance(finalized_student_data_from_form, list):
            raise ValueError("Invalid student plan format from form.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Error processing submitted student assignments: {e}", "danger")
        return redirect(url_for('main.view_upcoming_courses')) # Simplified

    if interim_alloc_collection is None:
        flash("Interim allocation database is unavailable.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    # --- 1. Update interimallocation with finalized student assignments ---
    student_update_ops_db: List[UpdateOne] = []
    confirmed_student_courses_for_tp_logic: List[Dict] = []

    try:
        for student_final_item in finalized_student_data_from_form:
            interim_doc_id_str = student_final_item.get('interim_doc_id')
            if not interim_doc_id_str:
                logger.warning(f"Missing 'interim_doc_id' in student data for TP prep: {student_final_item.get('sf_course_code')}")
                continue
            
            update_payload = {
                "student_assignments_final": student_final_item.get("interactive_assignments", []),
                "student_estimated_memory_final_gb": student_final_item.get("estimated_memory_gb", 0.0),
                "student_assignment_final_warning": student_final_item.get("assignment_warning"),
                "status": "student_confirmed", # Mark student part as confirmed
                "updated_at": datetime.datetime.now(pytz.utc)
            }
            student_update_ops_db.append(
                UpdateOne({"_id": ObjectId(interim_doc_id_str), "batch_review_id": batch_review_id}, 
                          {"$set": update_payload})
            )
            
            full_interim_doc_for_student = interim_alloc_collection.find_one(
                {"_id": ObjectId(interim_doc_id_str), "batch_review_id": batch_review_id}
            )
            if full_interim_doc_for_student:
                full_interim_doc_for_student.update(update_payload) 
                confirmed_student_courses_for_tp_logic.append(full_interim_doc_for_student)
            else:
                logger.error(f"Critical: Could not refetch interim doc ID {interim_doc_id_str} after planning update.")
                # This is a problem, as we need this full doc for trainer logic.
                # Depending on requirements, either skip this course for TP or error out.
                # For now, let's skip it to avoid cascading errors.
                flash(f"Error retrieving confirmed student data for {student_final_item.get('sf_course_code')}. Trainer pod assignment may be incomplete.", "warning")

        if student_update_ops_db:
            result_stud_upd = interim_alloc_collection.bulk_write(student_update_ops_db)
            logger.info(f"Updated {result_stud_upd.modified_count} student assignments in interim "
                        f"(batch '{batch_review_id}') to 'student_confirmed'.")
        else:
            logger.info("No student assignments found to update in interim for TP prep.")

    except Exception as e_stud_update:
        logger.error(f"Error updating student assignments in interim DB for batch '{batch_review_id}': {e_stud_update}", exc_info=True)
        flash("Error saving confirmed student assignments. Please try again.", "danger")
        return redirect(url_for('main.view_upcoming_courses')) # Simplified


    # --- 2. Prepare data and then Auto-Assign Trainer Pods ---
    courses_for_trainer_page_display: List[Dict] = []
    trainer_final_update_ops_db: List[UpdateOne] = []
    
    # Initial data fetch for capacities, existing allocations (as in previous full prepare_trainer_pods)
    build_rules_tp: List[Dict] = []
    all_hosts_dd: List[str] = []
    working_host_caps_gb_tp: Dict[str, float] = {} 
    taken_pods_vendor_tp: Dict[str, set] = defaultdict(set)
    lb_courses_on_host_registry: Dict[str, set] = defaultdict(set)
    course_configs_mem_tp_page: List[Dict] = []

    # ... (Full logic to populate:
    #      build_rules_tp,
    #      all_hosts_dd,
    #      working_host_caps_gb_tp (from initial_host_capacities_gb),
    #      taken_pods_vendor_tp (from alloc_collection),
    #      lb_courses_on_host_registry (from alloc_collection)
    #      - This is the same preparatory data fetching as the start of intermediate_build_review
    #        or the previous full prepare_trainer_pods version.
    # )
    # For brevity, assuming these are correctly populated.
    # CRITICAL: After populating from DB, update working_host_caps_gb_tp and registries
    # based on the `confirmed_student_courses_for_tp_logic`.

    for student_course_doc in confirmed_student_courses_for_tp_logic:
        # Display student part (read-only on trainer_pod_review.html)
        courses_for_trainer_page_display.append({"type": "student", "data": {
            "interim_doc_id": str(student_course_doc["_id"]), # Pass this for the JS on trainer page
            "sf_course_code": student_course_doc.get("sf_course_code"),
            "labbuild_course": student_course_doc.get("final_labbuild_course_student"), # Use confirmed student LB course
            "vendor": student_course_doc.get("vendor"),
            "sf_course_type": student_course_doc.get("sf_course_type"),
            "memory_gb_one_pod": student_course_doc.get("memory_gb_one_student_pod"),
            "interactive_assignments": student_course_doc.get("student_assignments_final"),
            "estimated_memory_gb": student_course_doc.get("student_estimated_memory_final_gb"),
            "assignment_warning": student_course_doc.get("student_assignment_final_warning")
        }})

        # --- Initialize Trainer Pod Variables with Defaults ---
        tp_sf_code = student_course_doc.get('sf_course_code', 'N/A_SF') + "-TP"
        tp_vendor = student_course_doc.get('vendor', '').lower()
        tp_sf_type = student_course_doc.get('sf_course_type', '')
        
        tp_host: Optional[str] = None
        tp_pod: Optional[int] = None
        tp_warning: Optional[str] = None
        # Start with student's confirmed LabBuild course, rules might override
        tp_lb_course: Optional[str] = student_course_doc.get('final_labbuild_course_student')
        tp_full_mem: float = float(student_course_doc.get('memory_gb_one_student_pod', 0.0))


        if not tp_vendor:
            tp_warning = "Vendor missing from student data. Cannot assign trainer pod."
            logger.warning(f"For TP of {student_course_doc.get('sf_course_code')}: {tp_warning}")
        else:
            # Apply rules for TP
            tp_rules = _find_all_matching_rules(build_rules_tp, tp_vendor, tp_sf_code, tp_sf_type)
            tp_eff_actions: Dict[str, Any] = {}
            tp_action_priority = ["set_labbuild_course", "host_priority", "allow_spillover", "start_pod_number"]
            for r in tp_rules:
                acts = r.get("actions", {});
                for k_act in tp_action_priority:
                    if k_act not in tp_eff_actions and k_act in acts: tp_eff_actions[k_act] = acts[k_act]

            if "set_labbuild_course" in tp_eff_actions:
                new_tp_lb_course = tp_eff_actions["set_labbuild_course"]
                if new_tp_lb_course != tp_lb_course:
                    logger.info(f"TP for '{tp_sf_code}': LB course changed by rule from '{tp_lb_course}' to '{new_tp_lb_course}'.")
                tp_lb_course = new_tp_lb_course
            
            if tp_lb_course: # Only recalculate memory if we have a LB course for trainer
                tp_full_mem = _get_memory_for_course(tp_lb_course)
            else: # No LB course for trainer
                tp_warning = (tp_warning + ". " if tp_warning else "") + "No LabBuild course for Trainer Pod."

            if not tp_warning and tp_full_mem <= 0: # If no previous warning, but memory is bad
                tp_warning = (tp_warning + ". " if tp_warning else "") + \
                             f"Memory for TP LB course '{tp_lb_course}' is 0 or unknown."
            
            # --- Actual Trainer Pod Auto-Assignment Logic ---
            if not tp_warning: # Only if basic TP info (LB course, memory) is valid
                logger.info(f"Auto-assigning TP for '{tp_sf_code}', LB '{tp_lb_course}', Full Mem {tp_full_mem:.2f}GB") # type: ignore
                
                tp_hosts_to_try: List[str] = []
                priority_hosts_tp_rule = tp_eff_actions.get("host_priority", []) # type: ignore
                allow_spillover_tp_rule = tp_eff_actions.get("allow_spillover", True) # type: ignore
                
                # Tentative memory needed for initial filtering of hosts (worst case: full memory)
                # A more refined filter would use factored memory if a similar LB course is on the host.
                # For now, let's assume a host must have at least *some* capacity.
                # The actual mem_needed_tp will be calculated per host.
                min_mem_for_candidate_host = tp_full_mem * SUBSEQUENT_POD_MEMORY_FACTOR if SUBSEQUENT_POD_MEMORY_FACTOR > 0 else tp_full_mem # type: ignore
                if min_mem_for_candidate_host <=0 and tp_full_mem > 0 : min_mem_for_candidate_host = 0.1 # Ensure positive if base mem is positive

                if isinstance(priority_hosts_tp_rule, list):
                    tp_hosts_to_try.extend([
                        h for h in priority_hosts_tp_rule 
                        if h in working_host_caps_gb_tp and 
                           working_host_caps_gb_tp.get(h, 0.0) >= min_mem_for_candidate_host 
                    ])
                
                if allow_spillover_tp_rule:
                    spill_hosts_tp = sorted([
                        h_name for h_name in working_host_caps_gb_tp
                        if h_name not in tp_hosts_to_try and # Avoid duplicates
                           working_host_caps_gb_tp.get(h_name, 0.0) >= min_mem_for_candidate_host
                    ])
                    tp_hosts_to_try.extend(spill_hosts_tp)

                # If no hosts have even minimal capacity, prepare for fallback assignment
                if not tp_hosts_to_try:
                    if all_hosts_dd: # type: ignore
                        tp_hosts_to_try.append(all_hosts_dd[0]) # Fallback to first available host overall
                        current_warning_tp = ("No hosts with sufficient initial capacity found based on rules/spillover. "
                                            f"Attempting assignment on default host '{all_hosts_dd[0]}'. VERIFY CAPACITY.") # type: ignore
                        tp_warning = (tp_warning + ". " if tp_warning else "") + current_warning_tp
                        logger.warning(f"For TP of {tp_sf_code}: {current_warning_tp}")
                    else: # No hosts configured at all
                        current_warning_tp = "No hosts configured or available for trainer pod assignment."
                        tp_warning = (tp_warning + ". " if tp_warning else "") + current_warning_tp
                        logger.error(f"For TP of {tp_sf_code}: {current_warning_tp}")
                        # Skip further assignment logic for this TP if no hosts at all
                        tp_hosts_to_try = [] # Ensure loop below doesn't run

                # Determine start pod number for new TPs from rules or default
                start_pod_num_tp_config = 1
                try:
                    sp_rule_val_tp = tp_eff_actions.get("start_pod_number") # type: ignore
                    if sp_rule_val_tp is not None:
                        start_pod_num_tp_config = max(1, int(sp_rule_val_tp))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid 'start_pod_number' in rule for TP of {tp_sf_code}. Defaulting to 1.")


                assigned_this_tp = False
                for host_candidate_tp in tp_hosts_to_try:
                    # Find next available NEW pod number for this vendor on this host
                    # (Trainer pods are usually new, not reused, but check global taken list)
                    current_new_tp_pod_candidate = start_pod_num_tp_config
                    while current_new_tp_pod_candidate in taken_pods_vendor_tp.get(tp_vendor, set()): # type: ignore
                        current_new_tp_pod_candidate += 1
                    
                    # Calculate actual memory needed for this TP on this host_candidate_tp
                    is_first_of_lb_course_type_on_host = tp_lb_course not in lb_courses_on_host_registry.get(host_candidate_tp, set()) # type: ignore
                    
                    mem_needed_for_this_tp = tp_full_mem # type: ignore
                    if not is_first_of_lb_course_type_on_host:
                        mem_needed_for_this_tp *= SUBSEQUENT_POD_MEMORY_FACTOR
                    
                    mem_needed_for_this_tp = round(mem_needed_for_this_tp, 2) # Round to avoid float issues

                    current_capacity_on_host_cand = working_host_caps_gb_tp.get(host_candidate_tp, 0.0)

                    logger.debug(f"  TP for '{tp_sf_code}' on host '{host_candidate_tp}': " # type: ignore
                                 f"LB Course '{tp_lb_course}', Is first of type? {is_first_of_lb_course_type_on_host}. " # type: ignore
                                 f"Mem needed: {mem_needed_for_this_tp:.2f}GB. Host capacity: {current_capacity_on_host_cand:.2f}GB.")

                    if current_capacity_on_host_cand >= mem_needed_for_this_tp:
                        tp_host = host_candidate_tp
                        tp_pod = current_new_tp_pod_candidate
                        
                        # Update global tracking
                        taken_pods_vendor_tp[tp_vendor].add(tp_pod) # type: ignore
                        if tp_lb_course: # type: ignore
                            lb_courses_on_host_registry[tp_host].add(tp_lb_course) # type: ignore
                        
                        # Deduct memory from this host's working capacity
                        working_host_caps_gb_tp[tp_host] -= mem_needed_for_this_tp
                        working_host_caps_gb_tp[tp_host] = max(0, round(working_host_caps_gb_tp[tp_host], 2))
                        
                        logger.info(f"SUCCESS: Assigned TP for '{tp_sf_code}' (LB: {tp_lb_course}) to Host: {tp_host}, Pod: {tp_pod}. " # type: ignore
                                    f"Mem consumed: {mem_needed_for_this_tp:.2f}GB. "
                                    f"Host '{tp_host}' new eff. capacity: {working_host_caps_gb_tp[tp_host]:.2f}GB.")
                        assigned_this_tp = True
                        tp_warning = None # Clear any previous fallback warning if successfully assigned
                        break # Found a suitable host and pod for this trainer pod
                    else:
                        # If this is the first host we are seriously considering (e.g. from priority or first in list)
                        # and it doesn't have enough space, store it as a fallback and set a warning.
                        if tp_host is None: # Only set fallback if no host has been assigned yet
                            tp_host = host_candidate_tp
                            tp_pod = current_new_tp_pod_candidate # Tentatively assign pod number too
                            current_warning_cap = (f"Host '{host_candidate_tp}' has INSUFFICIENT capacity ({current_capacity_on_host_cand:.2f}GB) "
                                                   f"for TP needing {mem_needed_for_this_tp:.2f}GB. Marked as tentative assignment.")
                            tp_warning = (tp_warning + ". " if tp_warning else "") + current_warning_cap
                            logger.warning(f"For TP of {tp_sf_code}: {current_warning_cap}")
                            # Do NOT break; continue to see if other hosts in tp_hosts_to_try have capacity.
                            # If a better host is found, tp_host, tp_pod, and tp_warning will be overridden.
                
                if not assigned_this_tp:
                    # If loop finished and assigned_this_tp is false, it means no host had enough capacity.
                    # tp_host and tp_pod might hold the fallback assignment to the first host tried (even if over capacity).
                    # The tp_warning would reflect the capacity issue.
                    if tp_host is None and all_hosts_dd: # If absolutely no host was even a candidate for fallback
                        tp_host = all_hosts_dd[0] # Assign to first available as absolute fallback
                        cand_pod_tp_abs_fallback = start_pod_num_tp_config
                        while cand_pod_tp_abs_fallback in taken_pods_vendor_tp.get(tp_vendor, set()): # type: ignore
                            cand_pod_tp_abs_fallback +=1
                        tp_pod = cand_pod_tp_abs_fallback
                        current_warning_abs_fb = f"No host from rules/spillover had capacity. Defaulted TP to '{tp_host}/{tp_pod}'. VERIFY MANUALLY."
                        tp_warning = (tp_warning + ". " if tp_warning else "") + current_warning_abs_fb
                        logger.error(f"For TP of {tp_sf_code}: {current_warning_abs_fb}")
                        # Mark this fallback assignment as "taken" to prevent reuse by next TP in this batch
                        taken_pods_vendor_tp[tp_vendor].add(tp_pod) # type: ignore
                        if tp_lb_course: lb_courses_on_host_registry[tp_host].add(tp_lb_course) # type: ignore
                        # DO NOT deduct memory if it's a capacity-exceeding fallback; user must fix.
                    elif tp_host and tp_warning and "INSUFFICIENT capacity" in tp_warning:
                        # We have a fallback assignment to an over-capacity host.
                        # Still mark the pod as taken for this batch to avoid conflicts.
                        taken_pods_vendor_tp[tp_vendor].add(tp_pod) # type: ignore
                        if tp_lb_course: lb_courses_on_host_registry[tp_host].add(tp_lb_course) # type: ignore
                        logger.warning(f"TP for '{tp_sf_code}' assigned to '{tp_host}/{tp_pod}' despite insufficient capacity warning. Manual review required.")
                        # Memory was NOT deducted for this over-capacity assignment; assignment_capacities_gb remains as it was for this host.
        # --- End Actual Trainer Pod Auto-Assignment ---


        # Prepare trainer assignment data for the interim document update
        trainer_data_for_db_update = {
            "trainer_labbuild_course": tp_lb_course,
            "trainer_memory_gb_one_pod": tp_full_mem,
            "trainer_assignment_auto_proposed": {
                "host": tp_host, "start_pod": tp_pod, "end_pod": tp_pod
            } if tp_host and tp_pod is not None else None,
            "trainer_assignment_warning": tp_warning,
            "status": "pending_trainer_review", # Update status for this course
            "updated_at": datetime.datetime.now(pytz.utc)
        }
        trainer_final_update_ops_db.append(
            UpdateOne(
                {"_id": student_course_doc["_id"], "batch_review_id": batch_review_id},
                {"$set": trainer_data_for_db_update}
            )
        )
        
        # Prepare data for rendering trainer_pod_review.html
        trainer_display_data = {
            "interim_doc_id": str(student_course_doc["_id"]),
            "sf_course_code": tp_sf_code,
            "labbuild_course": tp_lb_course,
            "vendor": tp_vendor.upper() if tp_vendor else "N/A",
            "memory_gb_one_pod": tp_full_mem,
            "assigned_host": tp_host, # Auto-assigned host for pre-population
            "start_pod": tp_pod,      # Auto-assigned pod
            "end_pod": tp_pod,        # Auto-assigned pod
            "error_message": tp_warning # This will be the capacity warning if any
        }
        courses_for_trainer_page_display.append({"type": "trainer", "data": trainer_display_data})
    # --- End Loop over student_course_doc ---

    if trainer_final_update_ops_db:
        try:
            if interim_alloc_collection is not None:
                result_tp_upd = interim_alloc_collection.bulk_write(trainer_final_update_ops_db)
                logger.info(f"Updated {result_tp_upd.modified_count} interim docs with trainer proposals for batch '{batch_review_id}'.")
            else:
                raise PyMongoError("interim_alloc_collection is None for trainer proposals.")
        except (PyMongoError, BulkWriteError, NotImplementedError) as e_tp_save:
            logger.error(f"Error saving trainer proposals to interim DB (batch '{batch_review_id}'): {e_tp_save}", exc_info=True)
            flash("Error saving trainer pod proposals. Please try again.", "danger")
            return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    if course_config_collection is not None: # For JS memory lookup on trainer page
        try: course_configs_mem_tp_page = list(course_config_collection.find({}, {"course_name": 1, "memory_gb_per_pod": 1, "memory": 1, "_id": 0}))
        except PyMongoError as e: logger.error(f"DB error fetching course_configs for TP page JS: {e}")

    return render_template(
        'trainer_pod_review.html',
        courses_and_trainer_pods=courses_for_trainer_page_display,
        batch_review_id=batch_review_id,
        current_theme=current_theme,
        all_hosts=all_hosts_dd, # Ensure this is populated correctly
        course_configs_for_memory=course_configs_mem_tp_page,
        subsequent_pod_memory_factor=SUBSEQUENT_POD_MEMORY_FACTOR
    )


@bp.route('/finalize-and-display-build-plan', methods=['POST'])
def finalize_and_display_build_plan(): # Renamed from finalize_and_schedule_builds
    """
    1. Receives finalized trainer assignments and `batch_review_id` from `trainer_pod_review.html`.
    2. Updates trainer assignments in `interimallocation` for that `batch_review_id`
       to status 'trainer_confirmed' (or a general 'build_plan_finalized').
    3. Fetches ALL items for this `batch_review_id` from `interimallocation`
       (which now have confirmed student and trainer assignments).
    4. Fetches 'extend:true' items from `currentallocation`.
    5. Combines, sorts, and renders `final_review_schedule.html`.
    """
    current_theme = request.cookies.get('theme', 'light')
    batch_review_id = request.form.get('batch_review_id')
    final_trainer_plan_json = request.form.get('final_trainer_assignments_for_review')

    if not batch_review_id:
        flash("Review session ID missing for final plan display.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    final_trainer_assignments_input: List[Dict] = []
    if final_trainer_plan_json:
        try:
            parsed_data = json.loads(final_trainer_plan_json)
            if isinstance(parsed_data, list): final_trainer_assignments_input = parsed_data
        except json.JSONDecodeError:
            logger.error("Error decoding submitted trainer assignments for final display.")
            flash("Error processing submitted trainer assignments.", "danger")
            return redirect(url_for('actions.prepare_trainer_pods', batch_review_id=batch_review_id)) # Go back

    if interim_alloc_collection is None:
        flash("Interim allocation database unavailable for final display.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    # --- 1. Update interimallocation with finalized trainer assignments for this batch ---
    trainer_final_update_ops: List[UpdateOne] = []
    try:
        for trainer_final_data in final_trainer_assignments_input:
            # The trainer_final_data should contain the interim_doc_id of the parent course
            interim_doc_id_str = trainer_final_data.get('interim_doc_id')
            if not interim_doc_id_str:
                logger.warning(f"Missing 'interim_doc_id' in trainer final data: {trainer_final_data.get('sf_course_code')}")
                continue
            
            update_payload_tp_final = {
                "trainer_labbuild_course": trainer_final_data.get("labbuild_course"), # From user's choice
                "trainer_memory_gb_one_pod": trainer_final_data.get("memory_gb_one_pod"),
                "trainer_assignment_final": trainer_final_data.get("interactive_assignments", []), # User's choice
                "trainer_assignment_final_warning": trainer_final_data.get("error_message"), # User acknowledged warning
                "status": "trainer_confirmed", # Or a more general "plan_finalized"
                "updated_at": datetime.datetime.now(pytz.utc)
            }
            trainer_final_update_ops.append(
                UpdateOne(
                    {"_id": ObjectId(interim_doc_id_str), "batch_review_id": batch_review_id},
                    {"$set": update_payload_tp_final}
                )
            )
        if trainer_final_update_ops:
            result_tp_final = interim_alloc_collection.bulk_write(trainer_final_update_ops)
            logger.info(f"Updated {result_tp_final.modified_count} trainer assignments in interim "
                        f"(batch '{batch_review_id}') to 'trainer_confirmed'.")
        else:
            logger.info("No trainer assignments to finalize in interim for display.")

    except Exception as e_tp_final_save:
        logger.error(f"Error saving finalized trainer assignments to interim DB: {e_tp_final_save}", exc_info=True)
        flash("Error saving finalized trainer assignments. Please try again.", "danger")
        return redirect(url_for('actions.prepare_trainer_pods', batch_review_id=batch_review_id))


    # --- 2. Fetch ALL confirmed items for this batch_review_id from interimallocation ---
    all_confirmed_batch_items_for_display: List[Dict] = []
    try:
        # Fetch documents that are now fully confirmed for this batch
        confirmed_status_list = ["student_confirmed", "trainer_confirmed"] # Or just one final status
        confirmed_batch_cursor = interim_alloc_collection.find({
            "batch_review_id": batch_review_id,
            "status": {"$in": confirmed_status_list}
        }).sort("sf_course_code", ASCENDING) # Initial sort

        for doc in confirmed_batch_cursor:
            # Student Part
            all_confirmed_batch_items_for_display.append({
                "type": "Student Build",
                "sf_course_code": doc.get("sf_course_code"),
                "labbuild_course": doc.get("final_labbuild_course_student"),
                "vendor": doc.get("vendor"),
                "start_date": _format_date_for_review(doc.get("sf_start_date"), f"student {doc.get('sf_course_code')}"),
                "assignments": doc.get("student_assignments_final", []),
                "status_note": doc.get("student_assignment_final_warning") or doc.get("student_assignment_warning")
            })
            # Trainer Part (if it exists and is confirmed)
            if doc.get("trainer_labbuild_course") and doc.get("status") == "trainer_confirmed":
                trainer_start = _format_date_for_review(doc.get("sf_start_date"), f"trainer {doc.get('sf_course_code')}-TP")
                all_confirmed_batch_items_for_display.append({
                    "type": "Trainer Build",
                    "sf_course_code": doc.get("sf_course_code", "") + "-TP",
                    "labbuild_course": doc.get("trainer_labbuild_course"),
                    "vendor": doc.get("vendor"),
                    "start_date": trainer_start,
                    "assignments": doc.get("trainer_assignment_final", []),
                    "status_note": doc.get("trainer_assignment_final_warning") or doc.get("trainer_assignment_warning")
                })
    except (PyMongoError, NotImplementedError) as e_fetch_final_display:
        logger.error(f"DB Error fetching confirmed items from interim for display: {e_fetch_final_display}", exc_info=True)
        flash("Error fetching data for final review page.", "danger")

    # --- 3. Fetch "Extend: True" Allocations from currentallocation (as before) ---
    extended_allocations_display: List[Dict] = []
    # ... (logic to fetch and format extended_allocations_display - UNCHANGED from previous) ...

    # --- 4. Combine and Sort ---
    final_review_list_display = all_confirmed_batch_items_for_display + extended_allocations_display # type: ignore
    # ... (Python-side sorting logic using get_sort_key_python - UNCHANGED from previous) ...
    # (Ensure get_sort_key_python is defined as in the previous response)
    def get_sort_key_python(item: Dict) -> Tuple[str, datetime.date]: # Copied for completeness
        date_str = item.get("start_date", "")
        primary_sort_key = item.get("sf_course_code", "").lower()
        secondary_sort_date = datetime.date.max 
        if date_str == "Ongoing (Extended)": secondary_sort_date = datetime.date.min 
        elif date_str and date_str != "N/A":
            try: secondary_sort_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError: pass 
        return (primary_sort_key, secondary_sort_date)
    try: final_review_list_display.sort(key=get_sort_key_python)
    except Exception as e: logger.error(f"Sort error: {e}"); flash("Error sorting items.", "warning")


    # Prepare only items that need actual building for the next step's form
    buildable_items_for_scheduling = [
        item for item in final_review_list_display # Use the combined & sorted list
        if item.get("type") in ["Student Build", "Trainer Build"]
    ]

    return render_template(
        'final_review_schedule.html',
        all_items_for_review=final_review_list_display,
        buildable_items_json=json.dumps(buildable_items_for_scheduling),
        batch_review_id=batch_review_id, # Pass for the form to /execute-scheduled-builds
        current_theme=current_theme,
    )