# dashboard/routes/actions.py

import logging
import threading
import datetime
import pytz
import re
import io
import json
from flask import (
    Blueprint, request, redirect, url_for, flash, current_app, jsonify, render_template, send_file
)
from pymongo.errors import PyMongoError, BulkWriteError
import pymongo
from pymongo import ASCENDING, UpdateOne
from collections import defaultdict
from typing import List, Dict, Optional, Tuple, Any, Union
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
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill, Color
from openpyxl.utils import get_column_letter
import math
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

def _allocate_specific_maestro_component(
    component_labbuild_course: str,
    num_pods_to_allocate: int,
    target_host_or_priority_list: Union[str, List[str]],
    allow_spillover_for_component: bool,
    all_available_hosts: List[str],
    pods_assigned_in_this_batch: Dict[str, set],
    db_locked_pods: Dict[str, set],
    current_host_capacities_gb: Dict[str, float],
    memory_per_pod_for_component: float,
    vendor_code: str,
    start_pod_suggestion: int = 1
) -> Tuple[List[Dict[str, Any]], Optional[str], float]:
    assignments_for_component: List[Dict[str, Any]] = []
    warning_message: Optional[str] = None
    total_memory_consumed_for_component = 0.0
    pods_left_for_this_component = num_pods_to_allocate

    if num_pods_to_allocate <= 0: # No pods needed for this component
        return assignments_for_component, warning_message, total_memory_consumed_for_component
    if memory_per_pod_for_component <= 0:
        warning_message = f"Memory for component '{component_labbuild_course}' is 0 or less. Cannot allocate."
        logger.warning(f"  {warning_message}")
        return assignments_for_component, warning_message, total_memory_consumed_for_component


    hosts_to_try_for_component: List[str] = []
    if isinstance(target_host_or_priority_list, str):
        if target_host_or_priority_list in current_host_capacities_gb:
            hosts_to_try_for_component = [target_host_or_priority_list]
        else:
            warning_message = f"Pinned host '{target_host_or_priority_list}' for '{component_labbuild_course}' not available or no capacity info."
            return assignments_for_component, warning_message, total_memory_consumed_for_component
    elif isinstance(target_host_or_priority_list, list):
        hosts_to_try_for_component.extend([h for h in target_host_or_priority_list if h in current_host_capacities_gb])
        if allow_spillover_for_component:
            hosts_to_try_for_component.extend(sorted([
                h_n for h_n in all_available_hosts if h_n not in hosts_to_try_for_component and h_n in current_host_capacities_gb
            ]))
    else:
        warning_message = f"Invalid target_host_or_priority_list type for '{component_labbuild_course}'."
        return assignments_for_component, warning_message, total_memory_consumed_for_component

    if not hosts_to_try_for_component:
        warning_message = f"No suitable hosts found for '{component_labbuild_course}' based on priority/pinning ({num_pods_to_allocate} pods needed)."
        return assignments_for_component, warning_message, total_memory_consumed_for_component

    logger.debug(f"  Allocating for '{component_labbuild_course}': Needs {pods_left_for_this_component} pods. Hosts to try: {hosts_to_try_for_component}. Start Suggestion: {start_pod_suggestion}")

    for host_target in hosts_to_try_for_component:
        if pods_left_for_this_component <= 0: break
        current_host_cap = current_host_capacities_gb.get(host_target, 0.0)
        assigned_on_this_host_segment: List[int] = []
        candidate_pod_num = start_pod_suggestion
        temp_pods_left_on_host_iter = pods_left_for_this_component

        while temp_pods_left_on_host_iter > 0:
            actual_candidate_pod_num = candidate_pod_num
            while True:
                if actual_candidate_pod_num not in db_locked_pods.get(vendor_code, set()) and \
                   actual_candidate_pod_num not in pods_assigned_in_this_batch.get(vendor_code, set()):
                    break
                actual_candidate_pod_num += 1

            mem_needed_for_this_pod = memory_per_pod_for_component
            if assigned_on_this_host_segment: # If we've already put at least one pod of this type on this host *in this segment*
                 mem_needed_for_this_pod *= SUBSEQUENT_POD_MEMORY_FACTOR
            
            if current_host_cap >= mem_needed_for_this_pod:
                assigned_on_this_host_segment.append(actual_candidate_pod_num)
                pods_assigned_in_this_batch.setdefault(vendor_code, set()).add(actual_candidate_pod_num)
                new_host_cap = max(0, round(current_host_cap - mem_needed_for_this_pod, 2))
                current_host_capacities_gb[host_target] = new_host_cap # Update global capacity
                current_host_cap = new_host_cap # Update local loop variable
                total_memory_consumed_for_component += mem_needed_for_this_pod
                pods_left_for_this_component -= 1
                temp_pods_left_on_host_iter -=1
                logger.info(f"    Assigned pod {actual_candidate_pod_num} of '{component_labbuild_course}' to {host_target}. Pods left for component: {pods_left_for_this_component}. Host '{host_target}' cap left: {current_host_cap:.2f}GB")
                candidate_pod_num = actual_candidate_pod_num + 1
            else:
                warn_host_full_comp = f"Host '{host_target}' capacity limit for '{component_labbuild_course}' (needs {mem_needed_for_this_pod:.2f}GB, has {current_host_cap:.2f}GB)."
                warning_message = (warning_message + ". " if warning_message else "") + warn_host_full_comp
                logger.warning(f"    {warn_host_full_comp}")
                break
        
        if assigned_on_this_host_segment:
            assigned_on_this_host_segment.sort()
            start_p, end_p = assigned_on_this_host_segment[0], assigned_on_this_host_segment[0]
            for i in range(1, len(assigned_on_this_host_segment)):
                if assigned_on_this_host_segment[i] == end_p + 1:
                    end_p = assigned_on_this_host_segment[i]
                else:
                    assignments_for_component.append({"host": host_target, "start_pod": start_p, "end_pod": end_p, "labbuild_course": component_labbuild_course})
                    start_p = end_p = assigned_on_this_host_segment[i]
            assignments_for_component.append({"host": host_target, "start_pod": start_p, "end_pod": end_p, "labbuild_course": component_labbuild_course})

    if pods_left_for_this_component > 0:
        warn_not_all_comp = f"Could not allocate all {num_pods_to_allocate} pods for '{component_labbuild_course}'; {pods_left_for_this_component} remain unassigned."
        warning_message = (warning_message + ". " if warning_message else "") + warn_not_all_comp
        logger.warning(f"  {warn_not_all_comp}")

    return assignments_for_component, warning_message, round(total_memory_consumed_for_component, 2)
# --- END Helper ---

# Assume bp is defined in your routes file, e.g.
# bp = Blueprint('actions', __name__)

@bp.route('/intermediate-build-review', methods=['POST'])
def intermediate_build_review():
    current_theme = request.cookies.get('theme', 'light')
    selected_courses_json = request.form.get('selected_courses')

    processed_courses_for_student_review: List[Dict] = []
    build_rules: List[Dict] = []
    all_available_host_names: List[str] = []
    db_locked_pods: Dict[str, set] = defaultdict(set)
    db_reusable_candidates_by_vendor_host: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    host_memory_start_of_batch: Dict[str, float] = {}
    course_configs_for_memory_lookup: List[Dict] = []
    
    # These will be modified by allocation helpers and represent the state *during* this batch processing
    pods_assigned_in_this_batch: Dict[str, set] = defaultdict(set)
    current_assignment_capacities_gb: Dict[str, float] = {}
    
    available_lab_courses_by_vendor: Dict[str, set] = defaultdict(set)

    if not selected_courses_json:
        flash("No courses selected for review.", "warning")
        return redirect(url_for('main.view_upcoming_courses'))
    try:
        selected_courses_input = json.loads(selected_courses_json)
        if not isinstance(selected_courses_input, list) or not selected_courses_input:
            raise ValueError("Invalid course data: Expected non-empty list.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Invalid data received for review: {e}", "danger")
        logger.error("Failed to parse selected_courses JSON: %s", e, exc_info=True)
        return redirect(url_for('main.view_upcoming_courses'))

    if interim_alloc_collection is None:
        logger.critical("interim_alloc_collection is None. Cannot proceed.")
        flash("Critical error: Interim DB unavailable.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    try:
        if build_rules_collection is not None:
            build_rules = list(build_rules_collection.find().sort("priority", pymongo.ASCENDING))
            logger.info(f"Loaded {len(build_rules)} build rules for intermediate review.")
        else:
            logger.warning("Build rules collection is None. Allocation will use defaults or user input only.")

        temp_released_memory_by_host: Dict[str, float] = defaultdict(float)
        if alloc_collection is not None:
            alloc_collection.update_many({"extend": {"$exists": False}}, {"$set": {"extend": "false"}})
            db_allocs_cursor = alloc_collection.find({}, {"courses.vendor": 1, "courses.course_name":1, "courses.pod_details.host": 1, "courses.pod_details.pod_number": 1, "courses.pod_details.class_number": 1, "courses.pod_details.pods.pod_number": 1, "extend": 1, "_id": 0})
            _counted_for_release_mem_tracker = set()
            for tag_doc_db in db_allocs_cursor:
                is_tag_locked = str(tag_doc_db.get("extend", "false")).lower() == "true"
                for course_alloc_db in tag_doc_db.get("courses", []):
                    vendor = course_alloc_db.get("vendor", "").lower(); lb_course = course_alloc_db.get("course_name")
                    if not vendor or not lb_course: continue
                    mem_pod = _get_memory_for_course(lb_course)
                    for pd_db in course_alloc_db.get("pod_details", []):
                        host = pd_db.get("host"); ids_in_detail: List[int] = []
                        pod_num = pd_db.get("pod_number"); class_num = pd_db.get("class_number")
                        if pod_num is not None: 
                            try: ids_in_detail.append(int(pod_num)); 
                            except (ValueError,TypeError): pass
                        if vendor == 'f5':
                            if class_num is not None: 
                                try: ids_in_detail.append(int(class_num)); 
                                except (ValueError,TypeError): pass
                            for np_item in pd_db.get("pods", []):
                                np_n_val = np_item.get("pod_number")
                                if np_n_val is not None: 
                                    try: ids_in_detail.append(int(np_n_val)); 
                                    except (ValueError,TypeError): pass
                        for item_id_val in set(ids_in_detail): # Renamed item_id to avoid conflict
                            if is_tag_locked: db_locked_pods[vendor].add(item_id_val)
                            elif host and mem_pod > 0:
                                db_reusable_candidates_by_vendor_host[vendor][host].add(item_id_val)
                                mem_tracker_key = f"{vendor}_{host}_{item_id_val}"
                                if mem_tracker_key not in _counted_for_release_mem_tracker:
                                    temp_released_memory_by_host[host] += mem_pod
                                    _counted_for_release_mem_tracker.add(mem_tracker_key)
            del _counted_for_release_mem_tracker
        else: logger.warning("alloc_collection is None.")
        
        initial_host_raw_capacities_gb: Dict[str, Optional[float]] = {}
        if host_collection is not None:
            hosts_docs = list(host_collection.find({"include_for_build": "true"}))
            all_available_host_names = sorted([h['host_name'] for h in hosts_docs if 'host_name' in h])
            if hosts_docs: initial_host_raw_capacities_gb = get_hosts_available_memory_parallel(hosts_docs)
            for h_doc_prep in hosts_docs:
                h_name_prep = h_doc_prep.get("host_name"); vc_cap_prep = initial_host_raw_capacities_gb.get(h_name_prep)
                if h_name_prep and vc_cap_prep is not None:
                    rel_mem_prep = temp_released_memory_by_host.get(h_name_prep, 0.0)
                    host_memory_start_of_batch[h_name_prep] = round(vc_cap_prep + rel_mem_prep, 2)
        else: logger.warning("host_collection is None.")

        if course_config_collection is not None:
            course_configs_for_memory_lookup = list(course_config_collection.find({}, {"course_name": 1, "vendor_shortcode": 1, "memory_gb_per_pod": 1, "memory": 1, "_id": 0}))
            for cfg in course_configs_for_memory_lookup:
                vendor_cfg = cfg.get('vendor_shortcode', '').lower(); name = cfg.get('course_name')
                if vendor_cfg and name: available_lab_courses_by_vendor[vendor_cfg].add(name)
        else: logger.warning("course_config_collection is None.")

    except PyMongoError as e_fetch:
        logger.error(f"DB error during initial data fetch for intermediate review: {e_fetch}", exc_info=True)
        flash("Error fetching configuration data. Cannot proceed.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))
    except Exception as e_generic_prep:
        logger.error(f"Unexpected error during data preparation for intermediate review: {e_generic_prep}", exc_info=True)
        flash("An unexpected server error occurred during data preparation.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    current_assignment_capacities_gb = host_memory_start_of_batch.copy()
    logger.debug(f"--- Intermediate Review Start ---")
    logger.debug(f"  Initial Host Memory (Start of Batch): {host_memory_start_of_batch}")
    logger.debug(f"  Initial DB Locked Pods: {dict(db_locked_pods)}")
    logger.debug(f"  Initial DB Reusable Candidates: { {v: dict(h) for v,h in db_reusable_candidates_by_vendor_host.items()} }")

    batch_review_id = str(ObjectId())
    try:
        if interim_alloc_collection is not None:
            del_res = interim_alloc_collection.delete_many({"status": {"$regex": "^pending_"}})
            logger.info(f"Cleared {del_res.deleted_count} old pending review items. New batch_review_id: {batch_review_id}")
    except PyMongoError as e_clear_interim:
        logger.error(f"Error clearing old interim items for batch {batch_review_id}: {e_clear_interim}", exc_info=True)

    bulk_interim_ops: List[UpdateOne] = []

    for course_idx, course_input_data in enumerate(selected_courses_input):
        sf_code = course_input_data.get('sf_course_code', f'SF_UNKN_{str(ObjectId())[:4]}')
        user_selected_lb_course_student = course_input_data.get('labbuild_course')
        vendor_student = course_input_data.get('vendor', '').lower()
        sf_type_student = course_input_data.get('sf_course_type', '')
        
        sf_pods_req_raw = course_input_data.get('sf_pods_req', '1')
        initial_pods_req_from_sf_processing = 1
        if isinstance(sf_pods_req_raw, (int, float)):
            initial_pods_req_from_sf_processing = max(1, int(sf_pods_req_raw))
        elif isinstance(sf_pods_req_raw, str):
            match = re.match(r"^\s*(\d+)", sf_pods_req_raw)
            if match:
                try: initial_pods_req_from_sf_processing = max(1, int(match.group(1)))
                except ValueError: logger.warning(f"Could not parse numeric part of sf_pods_req '{sf_pods_req_raw}' for course '{sf_code}'. Defaulting to 1.")
            else: logger.warning(f"sf_pods_req '{sf_pods_req_raw}' for course '{sf_code}' is not purely numeric and no number found at start. Defaulting to 1.")
        else: logger.warning(f"Unexpected type for sf_pods_req '{sf_pods_req_raw}' for course '{sf_code}'. Defaulting to 1.")

        eff_actions_student: Dict[str, Any] = {}
        student_assignment_warning: Optional[str] = None
        matched_rules_for_course = _find_all_matching_rules(build_rules, vendor_student, sf_code, sf_type_student)
        action_keys_to_extract = ["set_labbuild_course", "host_priority", "allow_spillover", "set_max_pods", "start_pod_number", "maestro_split_build", "override_pods_req"]
        for rule in matched_rules_for_course:
            rule_actions = rule.get("actions", {})
            for key in action_keys_to_extract:
                if key not in eff_actions_student and key in rule_actions:
                    eff_actions_student[key] = rule_actions[key]

        final_lb_course_student = eff_actions_student.get('set_labbuild_course')
        if not final_lb_course_student or (final_lb_course_student not in available_lab_courses_by_vendor.get(vendor_student, set())):
            if final_lb_course_student: student_assignment_warning = f"Rule-defined LB course '{final_lb_course_student}' invalid for '{vendor_student}'. "
            if user_selected_lb_course_student and user_selected_lb_course_student in available_lab_courses_by_vendor.get(vendor_student, set()):
                final_lb_course_student = user_selected_lb_course_student
                student_assignment_warning = (student_assignment_warning or "") + f"Using user-selected '{final_lb_course_student}'."
            elif user_selected_lb_course_student:
                 student_assignment_warning = (student_assignment_warning or "") + f"User-selected LB course '{user_selected_lb_course_student}' invalid. "
                 final_lb_course_student = None
        
        maestro_split_config = eff_actions_student.get("maestro_split_build")
        is_special_maestro_handling = isinstance(maestro_split_config, dict) and vendor_student == "cp"

        if not final_lb_course_student and not is_special_maestro_handling: # Maestro handles its own LB course parts
            student_assignment_warning = (student_assignment_warning or "") + "No valid LabBuild course determined for standard build."

        host_priority_student = eff_actions_student.get('host_priority', all_available_host_names)
        if not isinstance(host_priority_student, list) or not host_priority_student: host_priority_student = all_available_host_names
        allow_spillover_student = eff_actions_student.get('allow_spillover', True)
        start_pod_suggestion_student = max(1, int(eff_actions_student.get('start_pod_number', 1)))
        max_pods_constraint_student = eff_actions_student.get('set_max_pods')
        
        eff_pods_req_student = initial_pods_req_from_sf_processing
        if eff_actions_student.get("override_pods_req") is not None:
            try: eff_pods_req_student = int(eff_actions_student.get("override_pods_req"))
            except (ValueError, TypeError):pass
        if max_pods_constraint_student is not None:
            try: eff_pods_req_student = min(eff_pods_req_student, int(max_pods_constraint_student))
            except (ValueError, TypeError): pass
        eff_pods_req_student = max(0, eff_pods_req_student)
        
        interim_doc_id = ObjectId()
        interim_student_data = {
            "_id": interim_doc_id, "batch_review_id": batch_review_id,
            "sf_course_code": sf_code, "sf_course_type": sf_type_student,
            "sf_start_date": _format_date_for_review(course_input_data.get('sf_start_date'), sf_code),
            "sf_end_date": _format_date_for_review(course_input_data.get('sf_end_date'), sf_code),
            "sf_trainer": course_input_data.get('Trainer', 'N/A'),
            "sf_pax": course_input_data.get('sf_pax', 0),
            "sf_pods_req_original": initial_pods_req_from_sf_processing, 
            "vendor": vendor_student, "status": "pending_student_review",
            "created_at": datetime.datetime.now(pytz.utc),
            "updated_at": datetime.datetime.now(pytz.utc),
            "rule_actions_applied": eff_actions_student 
        }
        
        initial_interactive_sub_rows_for_template: List[Dict[str,Any]] = []

        if is_special_maestro_handling and maestro_split_config:
            logger.info(f"Applying MAESTRO_SPLIT_BUILD handling for SF Course: {sf_code}")
            interim_student_data["effective_pods_req_student"] = 4 
            interim_student_data["final_labbuild_course_student"] = "MAESTRO_COMPOSITE"
            total_maestro_memory_consumed = 0.0
            maestro_components_allocations = []
            
            main_lb_course = maestro_split_config.get("main_course", "maestro-r81")
            main_lb_count = int(maestro_split_config.get("main_course_count", 2))
            rack1_lb_course = maestro_split_config.get("rack1_course", "maestro-rack1")
            rack2_lb_course = maestro_split_config.get("rack2_course", "maestro-rack2")
            rack_host_pin = maestro_split_config.get("rack_host", "hotshot")
            
            current_maestro_pod_suggestion = start_pod_suggestion_student

            mem_main = _get_memory_for_course(main_lb_course)
            if mem_main > 0:
                main_asgns, main_warn, main_mem = _allocate_specific_maestro_component(
                    main_lb_course, main_lb_count, host_priority_student, allow_spillover_student, 
                    all_available_host_names, pods_assigned_in_this_batch, db_locked_pods, 
                    current_assignment_capacities_gb, mem_main, vendor_student, 
                    start_pod_suggestion=current_maestro_pod_suggestion
                )
                if main_warn: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + f"{main_lb_course}: {main_warn}"
                maestro_components_allocations.extend(main_asgns); total_maestro_memory_consumed += main_mem
                if main_asgns: current_maestro_pod_suggestion = main_asgns[-1]['end_pod'] + 10 
            else: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + f"Memory for {main_lb_course} is 0. Skipping."

            mem_rack1 = _get_memory_for_course(rack1_lb_course)
            if mem_rack1 > 0 :
                rack1_asgns, rack1_warn, rack1_mem = _allocate_specific_maestro_component(
                    rack1_lb_course, 1, rack_host_pin, False, 
                    all_available_host_names, pods_assigned_in_this_batch, db_locked_pods, 
                    current_assignment_capacities_gb, mem_rack1, vendor_student, 
                    start_pod_suggestion=current_maestro_pod_suggestion
                )
                if rack1_warn: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + f"{rack1_lb_course}: {rack1_warn}"
                maestro_components_allocations.extend(rack1_asgns); total_maestro_memory_consumed += rack1_mem
                if rack1_asgns: current_maestro_pod_suggestion = rack1_asgns[-1]['end_pod'] + 1 
            else: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + f"Memory for {rack1_lb_course} is 0. Skipping."

            mem_rack2 = _get_memory_for_course(rack2_lb_course)
            if mem_rack2 > 0:
                rack2_asgns, rack2_warn, rack2_mem = _allocate_specific_maestro_component(
                    rack2_lb_course, 1, rack_host_pin, False, 
                    all_available_host_names, pods_assigned_in_this_batch, db_locked_pods, 
                    current_assignment_capacities_gb, mem_rack2, vendor_student, 
                    start_pod_suggestion=current_maestro_pod_suggestion
                )
                if rack2_warn: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + f"{rack2_lb_course}: {rack2_warn}"
                maestro_components_allocations.extend(rack2_asgns); total_maestro_memory_consumed += rack2_mem
            else: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + f"Memory for {rack2_lb_course} is 0. Skipping."
            
            initial_interactive_sub_rows_for_template = maestro_components_allocations
            interim_student_data["memory_gb_one_student_pod"] = total_maestro_memory_consumed 
            interim_student_data["student_assignment_warning"] = student_assignment_warning
        
        else: # Standard (non-Maestro-Special) course handling
            interim_student_data["effective_pods_req_student"] = eff_pods_req_student
            interim_student_data["final_labbuild_course_student"] = final_lb_course_student
            mem_per_student_pod_std = 0.0
            if not student_assignment_warning and final_lb_course_student:
                mem_per_student_pod_std = _get_memory_for_course(final_lb_course_student)
                if mem_per_student_pod_std <= 0:
                    student_assignment_warning = ((student_assignment_warning + ". ") if student_assignment_warning else "") + \
                                                 f"Memory for primary LB course '{final_lb_course_student}' is 0. Cannot auto-assign."
            interim_student_data["memory_gb_one_student_pod"] = mem_per_student_pod_std

            if not student_assignment_warning and mem_per_student_pod_std > 0 and eff_pods_req_student > 0:
                std_assignments, std_warn, std_mem_consumed = _allocate_specific_maestro_component(
                    final_lb_course_student, eff_pods_req_student, host_priority_student, allow_spillover_student,
                    all_available_host_names, pods_assigned_in_this_batch, db_locked_pods,
                    current_assignment_capacities_gb, mem_per_student_pod_std, vendor_student,
                    start_pod_suggestion=start_pod_suggestion_student
                )
                if std_warn: student_assignment_warning = (student_assignment_warning + ". " if student_assignment_warning else "") + std_warn
                initial_interactive_sub_rows_for_template = std_assignments
            interim_student_data["student_assignment_warning"] = student_assignment_warning

        if not initial_interactive_sub_rows_for_template and eff_pods_req_student > 0 and not student_assignment_warning :
            default_host_student = host_priority_student[0] if host_priority_student else (all_available_host_names[0] if all_available_host_names else None)
            default_start_pod = start_pod_suggestion_student
            default_end_pod = default_start_pod + eff_pods_req_student - 1 if eff_pods_req_student > 0 else default_start_pod
            default_lb_course_for_fallback = final_lb_course_student
            if is_special_maestro_handling:
                default_lb_course_for_fallback = maestro_split_config.get("main_course", "maestro-r81") if maestro_split_config else "maestro-r81"

            if default_lb_course_for_fallback: 
                initial_interactive_sub_rows_for_template.append({
                    "host": default_host_student, "start_pod": default_start_pod, "end_pod": default_end_pod,
                    "labbuild_course": default_lb_course_for_fallback
                })
                current_warning_fallback = interim_student_data.get("student_assignment_warning", "")
                if not default_host_student:
                    interim_student_data["student_assignment_warning"] = (current_warning_fallback + ". " if current_warning_fallback else "") + "No suitable host for default assignment row."
                else: 
                    interim_student_data["student_assignment_warning"] = (current_warning_fallback + ". " if current_warning_fallback else "") + "Auto-allocation failed to assign all pods; a default row was created. Please review."
            else: 
                current_warning_fallback = interim_student_data.get("student_assignment_warning", "")
                interim_student_data["student_assignment_warning"] = (current_warning_fallback + ". " if current_warning_fallback else "") + "Cannot create default assignment row: No LabBuild course."
        
        interim_student_data["initial_interactive_sub_rows_student"] = initial_interactive_sub_rows_for_template
        
        display_course_data = interim_student_data.copy()
        display_course_data['interim_doc_id'] = str(interim_doc_id)
        display_course_data['sf_pods_req'] = interim_student_data.get("effective_pods_req_student", 0)
        if is_special_maestro_handling:
            display_course_data['final_labbuild_course_student'] = "MAESTRO (Multi-Component)"
        else:
            display_course_data['final_labbuild_course_student'] = interim_student_data.get("final_labbuild_course_student")
        
        processed_courses_for_student_review.append(display_course_data)
        bulk_interim_ops.append(
            UpdateOne({"_id": interim_doc_id}, {"$set": interim_student_data}, upsert=True)
        )
    
    logger.debug(f"--- Intermediate Review End ---")
    logger.debug(f"  Host Memory AFTER Batch Proposal Simulation: {current_assignment_capacities_gb}")
    logger.debug(f"  Pods Tentatively Assigned in Batch: {dict(pods_assigned_in_this_batch)}")

    if bulk_interim_ops:
        try:
            if interim_alloc_collection is not None:
                result_db_interim = interim_alloc_collection.bulk_write(bulk_interim_ops)
                logger.info(f"Interim student proposals saved to batch '{batch_review_id}': Upserted: {result_db_interim.upserted_count}, Modified: {result_db_interim.modified_count}")
        except (PyMongoError, BulkWriteError) as e_save_interim_s:
            logger.error(f"Error saving student proposals to interim DB for batch '{batch_review_id}': {e_save_interim_s}", exc_info=True)
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
    3. For each student course, checks build rules. If a rule disables the
       trainer pod, it's skipped. Otherwise, auto-assigns trainer pods.
    4. Updates `interimallocation` with trainer pod proposals/skips
       and sets status to 'pending_trainer_review'.
    5. Renders `trainer_pod_review.html`.
    """
    current_theme = request.cookies.get('theme', 'light')
    final_student_plan_json = request.form.get('final_build_plan')
    batch_review_id = request.form.get('batch_review_id')

    if not batch_review_id:
        flash("Review session ID missing. Cannot prepare trainer pods.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))
    if not final_student_plan_json:
        flash("No finalized student build plan data received.", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    try:
        finalized_student_data_from_form = json.loads(final_student_plan_json)
        if not isinstance(finalized_student_data_from_form, list):
            raise ValueError("Invalid student plan format from form.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Error processing submitted student assignments: {e}", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

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
                "status": "student_confirmed",
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
                flash(f"Error retrieving confirmed student data for {student_final_item.get('sf_course_code')}. Trainer pod assignment may be incomplete.", "warning")

        if student_update_ops_db:
            result_stud_upd = interim_alloc_collection.bulk_write(student_update_ops_db)
            logger.info(f"Updated {result_stud_upd.modified_count} student assignments in interim "
                        f"(batch '{batch_review_id}') to 'student_confirmed'.")
    except (PyMongoError, BulkWriteError) as e_stud_update: # Removed NotImplementedError
        logger.error(f"Error updating student assignments in interim DB for batch '{batch_review_id}': {e_stud_update}", exc_info=True)
        flash("Error saving confirmed student assignments. Please try again.", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))
    except Exception as e_stud_generic: # Catch other potential errors
        logger.error(f"Unexpected error during student update phase: {e_stud_generic}", exc_info=True)
        flash("An unexpected error occurred while saving student assignments.", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))


    # --- 2. Prepare data and then Auto-Assign Trainer Pods ---
    courses_for_trainer_page_display: List[Dict] = []
    trainer_interim_update_ops: List[UpdateOne] = []
    
    build_rules_tp: List[Dict] = []
    all_hosts_dd: List[str] = []
    initial_host_caps_gb_tp: Dict[str, Optional[float]] = {}
    working_host_caps_gb_tp: Dict[str, float] = {}
    taken_pods_vendor_tp: Dict[str, set] = defaultdict(set) # Pods taken from DB + this batch's students + this batch's TPs
    lb_courses_on_host_registry: Dict[str, set] = defaultdict(set) # Tracks LB courses on hosts
    course_configs_mem_tp_page: List[Dict] = []

    try: # Preparatory data fetching for trainer pod assignment
        if build_rules_collection is not None:
            build_rules_tp = list(build_rules_collection.find().sort("priority", ASCENDING))

        if host_collection is not None:
            hosts_docs = list(host_collection.find({"include_for_build": "true"}))
            all_hosts_dd = sorted([hd['host_name'] for hd in hosts_docs if 'host_name' in hd])
            if hosts_docs:
                initial_host_caps_gb_tp = get_hosts_available_memory_parallel(hosts_docs)
            # Initialize working capacities (no released memory consideration here as it's post-student)
            working_host_caps_gb_tp = {
                h_name: cap for h_name, cap in initial_host_caps_gb_tp.items() if cap is not None
            }
        
        # Pre-load taken pods from DB AND from confirmed student assignments in this batch
        if alloc_collection is not None:
            db_cursor = alloc_collection.find({}, {"courses.vendor": 1, "courses.course_name":1, "courses.pod_details": 1})
            for tag_doc in db_cursor:
                for course_alloc in tag_doc.get("courses", []):
                    vendor, db_lb_course = course_alloc.get("vendor", "").lower(), course_alloc.get("course_name")
                    if not vendor: continue
                    for pd in course_alloc.get("pod_details", []):
                        db_host = pd.get("host")
                        if db_host and db_lb_course: lb_courses_on_host_registry[db_host].add(db_lb_course)
                        pod_num_val = pd.get("pod_number")
                        if pod_num_val is not None: 
                            try:taken_pods_vendor_tp[vendor].add(int(pod_num_val)); 
                            except:pass
                        if vendor == 'f5':
                            class_val = pd.get("class_number");
                            if class_val is not None: 
                                try:taken_pods_vendor_tp[vendor].add(int(class_val)); 
                                except:pass
                            for np in pd.get("pods",[]): np_val = np.get("pod_number");
                            if np_val is not None: 
                                try:taken_pods_vendor_tp[vendor].add(int(np_val)); 
                                except:pass
        
        for stud_course_confirmed in confirmed_student_courses_for_tp_logic:
            stud_vendor = stud_course_confirmed.get("vendor","").lower()
            stud_lb_name = stud_course_confirmed.get("final_labbuild_course_student")
            mem_one_stud_pod = float(stud_course_confirmed.get("memory_gb_one_student_pod",0.0))

            for stud_asgn in stud_course_confirmed.get("student_assignments_final",[]):
                stud_host = stud_asgn.get("host")
                if not stud_host or not stud_vendor or not stud_lb_name or mem_one_stud_pod <=0: continue
                
                lb_courses_on_host_registry[stud_host].add(stud_lb_name)
                try:
                    s_pod, e_pod = int(stud_asgn.get('start_pod',0)), int(stud_asgn.get('end_pod',0))
                    if s_pod > 0 and e_pod >= s_pod:
                        for i_pod in range(s_pod, e_pod + 1): taken_pods_vendor_tp[stud_vendor].add(i_pod)
                        # Deduct student memory from working_host_caps_gb_tp
                        num_stud_pods_on_host = e_pod - s_pod + 1
                        mem_to_deduct = mem_one_stud_pod + ((num_stud_pods_on_host -1) * mem_one_stud_pod * SUBSEQUENT_POD_MEMORY_FACTOR if num_stud_pods_on_host > 1 else 0)
                        working_host_caps_gb_tp[stud_host] = max(0, round(working_host_caps_gb_tp.get(stud_host, 0.0) - mem_to_deduct, 2))
                except (ValueError, TypeError): pass

        logger.debug(f"TP Prep: Initial Host Caps: {initial_host_caps_gb_tp}")
        logger.debug(f"TP Prep: Working Host Caps (after student deductions): {working_host_caps_gb_tp}")
        logger.debug(f"TP Prep: Taken Pods (DB + Batch Students): {dict(taken_pods_vendor_tp)}")
        logger.debug(f"TP Prep: LB Courses on Hosts (DB + Batch Students): {dict(lb_courses_on_host_registry)}")


        if course_config_collection is not None:
            course_configs_mem_tp_page = list(course_config_collection.find(
                {}, {"course_name": 1, "memory_gb_per_pod": 1, "memory": 1, "_id": 0}
            ))

    except PyMongoError as e_prep_tp:
        logger.error(f"DB error during TP data prep: {e_prep_tp}", exc_info=True)
        flash("Error preparing data for trainer pod assignment.", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))
    except Exception as e_generic_tp_prep:
        logger.error(f"Unexpected error during TP data prep: {e_generic_tp_prep}", exc_info=True)
        flash("An server error occurred preparing for trainer pod assignment.", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))


    # --- Iterate through confirmed student courses to propose/skip trainer pods ---
    for student_course_doc in confirmed_student_courses_for_tp_logic:
        courses_for_trainer_page_display.append({"type": "student", "data": {
            "interim_doc_id": str(student_course_doc["_id"]),
            "sf_course_code": student_course_doc.get("sf_course_code"),
            "labbuild_course": student_course_doc.get("final_labbuild_course_student"),
            "vendor": student_course_doc.get("vendor"),
            "sf_course_type": student_course_doc.get("sf_course_type"),
            "memory_gb_one_pod": student_course_doc.get("memory_gb_one_student_pod"),
            "interactive_assignments": student_course_doc.get("student_assignments_final"),
            "estimated_memory_gb": student_course_doc.get("student_estimated_memory_final_gb"),
            "assignment_warning": student_course_doc.get("student_assignment_final_warning") or student_course_doc.get("student_assignment_warning")
        }})

        student_sf_code = student_course_doc.get('sf_course_code', 'UNKNOWN_SF_FOR_TP')
        tp_sf_code_target = student_sf_code + "-TP"
        tp_vendor = student_course_doc.get('vendor', '').lower()
        student_sf_type = student_course_doc.get('sf_course_type', '') 

        relevant_rules = _find_all_matching_rules(
            build_rules_tp, tp_vendor, student_sf_code, student_sf_type
        )
        disable_this_trainer_pod = any(
            r.get("actions", {}).get("disable_trainer_pod") is True for r in relevant_rules
        )
        if disable_this_trainer_pod:
            logger.info(f"Rule disabled trainer pod for student course '{student_sf_code}'.")

        tp_lb_course: Optional[str] = None
        tp_full_mem: float = 0.0
        tp_host: Optional[str] = None
        tp_pod: Optional[int] = None
        tp_assignment_warning: Optional[str] = None
        
        trainer_data_for_db_update: Dict[str, Any] = {
            "status": "pending_trainer_review",
            "updated_at": datetime.datetime.now(pytz.utc)
        }

        if disable_this_trainer_pod:
            tp_assignment_warning = "Trainer pod build explicitly disabled by build rule."
            trainer_data_for_db_update.update({
                "trainer_labbuild_course": None, "trainer_memory_gb_one_pod": 0.0,
                "trainer_assignment_auto_proposed": None,
                "trainer_assignment_warning": tp_assignment_warning,
                "status": "trainer_disabled_by_rule"
            })
            trainer_display_data = {"labbuild_course": "N/A - Disabled", "memory_gb_one_pod": 0.0}
        else:
            tp_lb_course = student_course_doc.get('final_labbuild_course_student')
            tp_full_mem = float(student_course_doc.get('memory_gb_one_student_pod', 0.0))
            
            tp_eff_actions: Dict[str, Any] = {}
            tp_rules_for_specific_tp = _find_all_matching_rules(build_rules_tp, tp_vendor, tp_sf_code_target, student_sf_type)
            tp_action_keys = ["set_labbuild_course", "host_priority", "allow_spillover", "start_pod_number"]
            for r_tp in tp_rules_for_specific_tp:
                acts_tp = r_tp.get("actions", {});
                for k_tp in tp_action_keys:
                    if k_tp not in tp_eff_actions and k_tp in acts_tp: tp_eff_actions[k_tp] = acts_tp[k_tp]

            if "set_labbuild_course" in tp_eff_actions:
                new_tp_lb = tp_eff_actions["set_labbuild_course"]
                if new_tp_lb != tp_lb_course: logger.info(f"TP for '{tp_sf_code_target}': LB course changed by rule to '{new_tp_lb}'.")
                tp_lb_course = new_tp_lb
            
            if tp_lb_course: tp_full_mem = _get_memory_for_course(tp_lb_course)
            else: tp_assignment_warning = (tp_assignment_warning + ". " if tp_assignment_warning else "") + "No LB course for TP."

            if not tp_assignment_warning and tp_full_mem <= 0:
                tp_assignment_warning = (tp_assignment_warning + ". " if tp_assignment_warning else "") + \
                                         f"Memory for TP LB course '{tp_lb_course}' is 0."
            
            if not tp_assignment_warning: # Proceed with assignment logic
                logger.info(f"Auto-assigning TP for '{tp_sf_code_target}', LB '{tp_lb_course}', FullMem {tp_full_mem:.2f}GB")
                tp_hosts_to_try: List[str] = []
                priority_hosts_tp = tp_eff_actions.get("host_priority", [])
                min_mem_cand_tp = tp_full_mem * SUBSEQUENT_POD_MEMORY_FACTOR if SUBSEQUENT_POD_MEMORY_FACTOR > 0 else tp_full_mem
                if min_mem_cand_tp <=0 and tp_full_mem > 0 : min_mem_cand_tp = 0.1

                if isinstance(priority_hosts_tp, list):
                    tp_hosts_to_try.extend([h for h in priority_hosts_tp if h in working_host_caps_gb_tp and working_host_caps_gb_tp.get(h,0.0) >= min_mem_cand_tp])
                if tp_eff_actions.get("allow_spillover", True):
                    tp_hosts_to_try.extend(sorted([h_n for h_n in working_host_caps_gb_tp if h_n not in tp_hosts_to_try and working_host_caps_gb_tp.get(h_n,0.0) >= min_mem_cand_tp]))

                if not tp_hosts_to_try and all_hosts_dd:
                    tp_hosts_to_try.append(all_hosts_dd[0])
                    tp_assignment_warning = (tp_assignment_warning + ". " if tp_assignment_warning else "") + "No hosts by rule/cap for TP; using default. VERIFY."
                elif not tp_hosts_to_try:
                     tp_assignment_warning = (tp_assignment_warning + ". " if tp_assignment_warning else "") + "No hosts available for TP."

                start_pod_tp_cfg = 1; 
                try: sp_tp_cfg = tp_eff_actions.get("start_pod_number"); start_pod_tp_cfg = max(1,int(sp_tp_cfg)) if sp_tp_cfg is not None else 1; 
                except:pass
                
                assigned_this_tp_flag = False
                if tp_hosts_to_try:
                    for host_cand in tp_hosts_to_try:
                        cand_pod = start_pod_tp_cfg
                        while cand_pod in taken_pods_vendor_tp.get(tp_vendor, set()):
                            cand_pod +=1
                        
                        is_first_type = tp_lb_course not in lb_courses_on_host_registry.get(host_cand, set())
                        mem_needed = tp_full_mem if is_first_type else (tp_full_mem * SUBSEQUENT_POD_MEMORY_FACTOR)
                        mem_needed = round(mem_needed, 2)
                        host_cap = working_host_caps_gb_tp.get(host_cand, 0.0)

                        if host_cap >= mem_needed:
                            tp_host, tp_pod = host_cand, cand_pod
                            taken_pods_vendor_tp[tp_vendor].add(tp_pod)
                            if tp_lb_course: lb_courses_on_host_registry[tp_host].add(tp_lb_course)
                            working_host_caps_gb_tp[tp_host] = max(0, round(host_cap - mem_needed, 2))
                            logger.info(f"ASSIGNED TP '{tp_sf_code_target}' (LB:{tp_lb_course}) to Host:{tp_host}/Pod:{tp_pod}. Mem:{mem_needed:.2f}GB. HostCapLeft:{working_host_caps_gb_tp[tp_host]:.2f}GB")
                            assigned_this_tp_flag = True; tp_assignment_warning = None # Clear fallback warning
                            break 
                        elif tp_host is None: # First host tried, insufficient capacity - make it fallback
                            tp_host, tp_pod = host_cand, cand_pod
                            warn = f"Host '{host_cand}' low cap ({host_cap:.1f}GB) for TP ({mem_needed:.1f}GB)."
                            tp_assignment_warning = (tp_assignment_warning + ". " if tp_assignment_warning else "") + warn
                    
                    if not assigned_this_tp_flag: # No host had enough capacity
                        if tp_host is None and all_hosts_dd: # Absolute fallback if no candidate was even tried
                            tp_host = all_hosts_dd[0]
                            cand_pod_fb = start_pod_tp_cfg; 
                            while cand_pod_fb in taken_pods_vendor_tp.get(tp_vendor,set()): cand_pod_fb+=1
                            tp_pod = cand_pod_fb
                        if tp_host and tp_pod is not None: # If we have a fallback (even over-capacity one)
                            taken_pods_vendor_tp[tp_vendor].add(tp_pod) # Mark as taken for this batch
                            if tp_lb_course: lb_courses_on_host_registry[tp_host].add(tp_lb_course)
                            warn = (tp_assignment_warning + ". " if tp_assignment_warning else "") + "Assigned to fallback host despite capacity issues. MANUAL REVIEW CRITICAL."
                            tp_assignment_warning = warn
                            logger.error(f"TP for '{tp_sf_code_target}' assigned to '{tp_host}/{tp_pod}' with warning: {tp_assignment_warning}")
                        else: # No host could be assigned at all
                             tp_assignment_warning = (tp_assignment_warning + ". " if tp_assignment_warning else "") + "No host could be assigned for TP."


            trainer_data_for_db_update.update({
                "trainer_labbuild_course": tp_lb_course,
                "trainer_memory_gb_one_pod": tp_full_mem,
                "trainer_assignment_auto_proposed": {"host": tp_host, "start_pod": tp_pod, "end_pod": tp_pod} if tp_host and tp_pod is not None else None,
                "trainer_assignment_warning": tp_assignment_warning
            })
            trainer_display_data = {
                "interim_doc_id": str(student_course_doc["_id"]),
                "sf_course_code": tp_sf_code_target, "labbuild_course": tp_lb_course,
                "vendor": tp_vendor.upper() if tp_vendor else "N/A",
                "memory_gb_one_pod": tp_full_mem,
                "assigned_host": tp_host, "start_pod": tp_pod, "end_pod": tp_pod,
                "error_message": tp_assignment_warning
            }
        
        trainer_interim_update_ops.append(
            UpdateOne(
                {"_id": student_course_doc["_id"], "batch_review_id": batch_review_id},
                {"$set": trainer_data_for_db_update}
            )
        )
        courses_for_trainer_page_display.append({"type": "trainer", "data": trainer_display_data})

    if trainer_interim_update_ops:
        try:
            if interim_alloc_collection is not None:
                result_tp_upd = interim_alloc_collection.bulk_write(trainer_interim_update_ops)
                logger.info(f"Updated {result_tp_upd.modified_count} interim docs with TP info for batch '{batch_review_id}'.")
        except (PyMongoError, BulkWriteError) as e_tp_save:
            logger.error(f"Error saving TP proposals to interim DB: {e_tp_save}", exc_info=True)
            flash("Error saving trainer pod proposals.", "danger")
            return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    return render_template(
        'trainer_pod_review.html',
        courses_and_trainer_pods=courses_for_trainer_page_display,
        batch_review_id=batch_review_id,
        current_theme=current_theme,
        all_hosts=all_hosts_dd,
        course_configs_for_memory=course_configs_mem_tp_page,
        subsequent_pod_memory_factor=SUBSEQUENT_POD_MEMORY_FACTOR
    )


@bp.route('/finalize-and-display-build-plan', methods=['POST'])
def finalize_and_display_build_plan():
    """
    1. Receives finalized trainer assignments and `batch_review_id`.
    2. Updates `interimallocation` for trainer pods (final assignments or skipped).
    3. Fetches all confirmed student & trainer items for this batch.
    4. Fetches 'extend:true' items from `currentallocation`.
    5. Combines, sorts, and renders `final_review_schedule.html`.
    """
    current_theme = request.cookies.get('theme', 'light')
    batch_review_id = request.form.get('batch_review_id')
    final_trainer_plan_json = request.form.get(
        'final_trainer_assignments_for_review'
    )

    if not batch_review_id:
        flash("Review session ID missing. Cannot finalize plan.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    final_trainer_assignments_input: List[Dict] = []
    if final_trainer_plan_json:
        try:
            parsed_data = json.loads(final_trainer_plan_json)
            if isinstance(parsed_data, list):
                final_trainer_assignments_input = parsed_data
        except json.JSONDecodeError:
            logger.error("Error decoding submitted trainer assignments for final plan.")
            flash("Error processing submitted trainer assignments.", "danger")
            # Redirect back to trainer review, ideally repopulating student data
            # For now, simple redirect:
            return redirect(url_for('main.view_upcoming_courses'))

    if interim_alloc_collection is None:
        flash("Interim allocation database unavailable.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    # --- 1. Update interimallocation with finalized trainer assignments ---
    trainer_final_update_ops: List[UpdateOne] = []
    try:
        for trainer_final_data in final_trainer_assignments_input:
            interim_doc_id_str = trainer_final_data.get('interim_doc_id')
            build_this_trainer = trainer_final_data.get('build_trainer', False) # From checkbox

            if not interim_doc_id_str:
                logger.warning("Missing 'interim_doc_id' in trainer final data: "
                               f"{trainer_final_data.get('sf_course_code')}")
                continue
            
            update_payload_tp_final: Dict[str, Any] = {
                "updated_at": datetime.datetime.now(pytz.utc)
            }
            if build_this_trainer:
                update_payload_tp_final["trainer_labbuild_course"] = \
                    trainer_final_data.get("labbuild_course")
                update_payload_tp_final["trainer_memory_gb_one_pod"] = \
                    trainer_final_data.get("memory_gb_one_pod")
                update_payload_tp_final["trainer_assignment_final"] = \
                    trainer_final_data.get("interactive_assignments", [])
                update_payload_tp_final["trainer_assignment_final_warning"] = \
                    trainer_final_data.get("error_message") # User acknowledged warning
                update_payload_tp_final["status"] = "trainer_confirmed"
            else: # User chose not to build, or it was rule-disabled
                update_payload_tp_final["trainer_assignment_final"] = None
                update_payload_tp_final["trainer_assignment_final_warning"] = \
                    trainer_final_data.get("error_message", "Skipped by user/rule.")
                # Keep status as 'trainer_disabled_by_rule' if it was that, else 'trainer_skipped_by_user'
                if "Disabled by rule" in str(trainer_final_data.get("error_message")):
                     update_payload_tp_final["status"] = "trainer_disabled_by_rule"
                else:
                     update_payload_tp_final["status"] = "trainer_skipped_by_user"

            trainer_final_update_ops.append(
                UpdateOne(
                    {"_id": ObjectId(interim_doc_id_str),
                     "batch_review_id": batch_review_id},
                    {"$set": update_payload_tp_final}
                )
            )
        if trainer_final_update_ops:
            result_tp_final = interim_alloc_collection.bulk_write(
                trainer_final_update_ops
            )
            logger.info(f"Updated {result_tp_final.modified_count} trainer "
                        f"assignments in interim (batch '{batch_review_id}').")
    except (PyMongoError, BulkWriteError) as e_tp_final_save:
        logger.error("Error saving finalized trainer assignments to interim DB: "
                       f"{e_tp_final_save}", exc_info=True)
        flash("Error saving finalized trainer assignments.", "danger")
        return redirect(url_for('actions.prepare_trainer_pods',
                                batch_review_id=batch_review_id)) # Go back

    # --- 2. Fetch ALL confirmed/processed items for this batch from interim ---
    all_confirmed_batch_items_for_display: List[Dict] = []
    try:
        # Statuses indicating a decision has been made for this part of the plan
        finalized_statuses = ["student_confirmed", "trainer_confirmed",
                              "trainer_disabled_by_rule", "trainer_skipped_by_user"]
        
        confirmed_batch_cursor = interim_alloc_collection.find({
            "batch_review_id": batch_review_id,
            "status": {"$in": finalized_statuses} # Fetch all relevant states
        }).sort("sf_course_code", ASCENDING)

        for doc in confirmed_batch_cursor:
            # Student Part (always present from student_confirmed stage)
            all_confirmed_batch_items_for_display.append({
                "type": "Student Build",
                "sf_course_code": doc.get("sf_course_code"),
                "labbuild_course": doc.get("final_labbuild_course_student"),
                "vendor": doc.get("vendor"),
                "start_date": _format_date_for_review(
                    doc.get("sf_start_date"),
                    f"student {doc.get('sf_course_code')}"
                ),
                "assignments": doc.get("student_assignments_final", []),
                "memory_gb_one_pod": doc.get("memory_gb_one_student_pod"), # For info
                "status_note": doc.get("student_assignment_final_warning") or \
                               doc.get("student_assignment_warning")
            })
            # Trainer Part (conditionally add based on its data)
            if doc.get("trainer_labbuild_course") or \
               doc.get("status") == "trainer_disabled_by_rule" or \
               doc.get("status") == "trainer_skipped_by_user":
                
                trainer_start = _format_date_for_review(
                    doc.get("sf_start_date"), # Trainer usually same start as student
                    f"trainer {doc.get('sf_course_code')}-TP"
                )
                trainer_status_note = doc.get("trainer_assignment_final_warning") or \
                                      doc.get("trainer_assignment_warning")
                if doc.get("status") == "trainer_disabled_by_rule":
                    trainer_status_note = "Build disabled by rule."
                elif doc.get("status") == "trainer_skipped_by_user":
                    trainer_status_note = "Build skipped by user."

                all_confirmed_batch_items_for_display.append({
                    "type": "Trainer Build",
                    "sf_course_code": doc.get("sf_course_code", "") + "-TP",
                    "labbuild_course": doc.get("trainer_labbuild_course"),
                    "vendor": doc.get("vendor"),
                    "start_date": trainer_start,
                    "assignments": doc.get("trainer_assignment_final", []),
                    "memory_gb_one_pod": doc.get("trainer_memory_gb_one_pod"),
                    "status_note": trainer_status_note
                })
    except PyMongoError as e_fetch_final:
        logger.error("DB Error fetching confirmed items from interim: %s",
                       e_fetch_final, exc_info=True)
        flash("Error fetching data for final review page.", "danger")

    # --- 3. Fetch "Extend: True" Allocations from currentallocation ---
    extended_allocations_display: List[Dict] = []
    if alloc_collection is not None:
        try:
            logger.info("Fetching 'extend:true' allocations for final review display.")
            # Query for allocations where 'extend' field is the string "true" (case-insensitive)
            # If you store 'extend' as a boolean, the query would be {"extend": True}
            extend_true_cursor = alloc_collection.find(
                {"extend": {"$regex": "^true$", "$options": "i"}}
            )
            for tag_doc_db in extend_true_cursor:
                tag = tag_doc_db.get("tag", "N/A_TAG_EXT")
                for course_alloc_db in tag_doc_db.get("courses", []):
                    vendor = course_alloc_db.get("vendor")
                    lb_course_name = course_alloc_db.get("course_name")
                    
                    # Attempt to derive a start date from the tag for display consistency
                    # This is a heuristic and depends on your tag naming convention
                    sf_start_date_str_ext = "Ongoing (Extended)" # Default for extended items
                    match_date_ext = re.search(r'(\d{2})(\d{2})(\d{2})$', tag) # Example: DDMMYY at end
                    if match_date_ext:
                        day, month, year_short = match_date_ext.groups()
                        try:
                            dt_obj_ext = datetime.datetime.strptime(
                                f"20{year_short}-{month}-{day}", "%Y-%m-%d"
                            )
                            sf_start_date_str_ext = dt_obj_ext.strftime("%Y-%m-%d")
                        except ValueError:
                            logger.debug(f"Could not parse date from tag '{tag}' for extended display.")
                    
                    # Attempt to derive a more specific SF Course Code from the tag
                    sf_code_from_tag_ext = tag
                    if vendor and lb_course_name:
                        parts_ext = tag.split('_')
                        if len(parts_ext) > 1 and vendor.upper() in parts_ext[0].upper():
                            potential_sf_code_ext = parts_ext[1]
                            if re.match(r'^[A-Z0-9\-]+$', potential_sf_code_ext): # Basic check
                                sf_code_from_tag_ext = potential_sf_code_ext
                    
                    # Collect assignment details for display
                    assignment_details_for_display_ext = []
                    for pd_db_ext in course_alloc_db.get("pod_details", []):
                        host_ext = pd_db_ext.get("host", "N/A")
                        item_id_ext_val = pd_db_ext.get("pod_number") or \
                                          pd_db_ext.get("class_number") or "N/A"
                        nested_pods_display_str = ""
                        if vendor and vendor.lower() == 'f5' and pd_db_ext.get("pods"):
                            f5_pod_numbers = [
                                str(np.get("pod_number"))
                                for np in pd_db_ext.get("pods", [])
                                if np.get("pod_number") is not None
                            ]
                            if f5_pod_numbers:
                                nested_pods_display_str = f" (Pods: {', '.join(f5_pod_numbers)})"
                        
                        assignment_details_for_display_ext.append({
                            "host": host_ext,
                            # For "extended" items, 'pods' key in assignments list will show the pod/class ID
                            "pods": f"{item_id_ext_val}{nested_pods_display_str}"
                        })

                    extended_allocations_display.append({
                        "type": "extended", # To identify in template
                        "sf_course_code": sf_code_from_tag_ext,
                        "labbuild_course": lb_course_name or "N/A",
                        "vendor": vendor or "N/A",
                        "start_date": sf_start_date_str_ext,
                        "status_note": f"Keep Running (Tag: {tag})",
                        "assignments": assignment_details_for_display_ext
                        # No memory or pod req here as it's just for display of existing
                    })
        except PyMongoError as e_ext_disp:
            logger.error("DB Error fetching 'extend:true' items for display: %s",
                           e_ext_disp, exc_info=True)
            flash("Could not load all existing 'Keep Running' allocations for review.", "warning")
        except Exception as e_generic_ext: # Catch other unexpected errors
            logger.error("Unexpected error fetching 'extend:true' items: %s",
                           e_generic_ext, exc_info=True)
            flash("Server error while loading existing allocations.", "warning")
    else:
        logger.warning("alloc_collection is None. Cannot fetch 'extend:true' items.")
    # --- End Fetch "Extend: True" ---

    # --- 4. Combine and Sort ---
    final_review_list_display = all_confirmed_batch_items_for_display + \
                                extended_allocations_display
    
    def get_sort_key_python(item: Dict) -> Tuple[str, datetime.date]:
        date_str = item.get("start_date", "")
        primary_sort_key = item.get("sf_course_code", "").lower()
        secondary_sort_date = datetime.date.max
        if date_str == "Ongoing (Extended)":
            secondary_sort_date = datetime.date.min
        elif date_str and date_str != "N/A":
            try:
                secondary_sort_date = datetime.datetime.strptime(
                    date_str, "%Y-%m-%d"
                ).date()
            except ValueError: pass # Keep max date if parse fails
        return (primary_sort_key, secondary_sort_date)

    try:
        final_review_list_display.sort(key=get_sort_key_python)
    except Exception as e_sort_final:
        logger.error(f"Error sorting final review items: {e_sort_final}", exc_info=True)
        flash("Error organizing items for display.", "warning")

    buildable_items_for_scheduling = [
        item for item in final_review_list_display
        if item.get("type") in ["Student Build", "Trainer Build"] and # Only buildable types
           item.get("assignments") # And they have assignments
    ]

    return render_template(
        'final_review_schedule.html',
        all_items_for_review=final_review_list_display,
        buildable_items_json=json.dumps(buildable_items_for_scheduling),
        batch_review_id=batch_review_id,
        current_theme=current_theme,
    )


@bp.route('/execute-scheduled-builds', methods=['POST'])
def execute_scheduled_builds():
    """
    Receives the confirmed build plan, teardown preference, and scheduling options,
    then schedules the labbuild setup (and optionally teardown) commands.
    """
    confirmed_plan_json = request.form.get('confirmed_build_plan_data')
    schedule_option = request.form.get('schedule_option', 'now')
    batch_review_id = request.form.get('batch_review_id') # For updating interim docs

    perform_teardown_first = request.form.get('perform_teardown_first') == 'yes'
    logger.info(f"Execute Scheduled Builds for Batch ID '{batch_review_id}': "
                f"Teardown before setup = {perform_teardown_first}, "
                f"Schedule Option = {schedule_option}")

    schedule_start_time_str: Optional[str] = None
    stagger_minutes: int = 30 # Default stagger

    if schedule_option == 'specific_time_all':
        schedule_start_time_str = request.form.get('schedule_start_time')
    elif schedule_option == 'staggered':
        schedule_start_time_str = request.form.get('schedule_start_time_staggered')
        stagger_minutes_str = request.form.get('schedule_stagger_minutes', '30')
        try:
            stagger_minutes = int(stagger_minutes_str)
            if stagger_minutes < 1:
                logger.warning("Stagger minutes less than 1, defaulting to 1.")
                stagger_minutes = 1
        except (ValueError, TypeError):
            logger.warning(f"Invalid stagger_minutes value '{stagger_minutes_str}', "
                           "defaulting to 30.")
            stagger_minutes = 30

    if not confirmed_plan_json:
        flash("No confirmed build plan received to schedule.", "danger")
        return redirect(url_for('actions.finalize_and_display_build_plan',
                                batch_review_id=batch_review_id))
    try:
        buildable_items_from_form = json.loads(confirmed_plan_json)
        if not isinstance(buildable_items_from_form, list):
            raise ValueError("Invalid confirmed build plan format: not a list.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Error processing confirmed build plan: {e}", "danger")
        logger.error("Error parsing confirmed_build_plan_data: %s", e, exc_info=True)
        return redirect(url_for('actions.finalize_and_display_build_plan',
                                batch_review_id=batch_review_id))

    if scheduler is None or not scheduler.running:
        flash("Scheduler not running. Cannot schedule builds.", "danger")
        logger.error("Attempted to schedule builds, but scheduler is not running.")
        return redirect(url_for('main.index'))

    logger.info(f"Received {len(buildable_items_from_form)} buildable items to schedule for "
                f"batch '{batch_review_id}' with option: '{schedule_option}'.")
    
    from apscheduler.triggers.date import DateTrigger # Import here to keep it local
    scheduler_tz_obj = scheduler.timezone
    
    # Determine the base_run_time_utc for the very first operation
    base_run_time_utc: datetime.datetime = datetime.datetime.now(pytz.utc)
    if schedule_option != 'now' and schedule_start_time_str:
        try:
            naive_dt_form = datetime.datetime.fromisoformat(schedule_start_time_str)
            if hasattr(scheduler_tz_obj, 'localize'): # For pytz
                localized_dt = scheduler_tz_obj.localize(naive_dt_form, is_dst=None) # type: ignore
            else: # For zoneinfo or other standard tzinfo
                localized_dt = naive_dt_form.replace(tzinfo=scheduler_tz_obj)
            base_run_time_utc = localized_dt.astimezone(pytz.utc)
            logger.info(f"Base run time for scheduling (UTC): {base_run_time_utc}, "
                        f"from form input '{schedule_start_time_str}' in tz '{scheduler_tz_obj}'.")
        except ValueError as e_date_sched:
            logger.error(f"Invalid datetime format '{schedule_start_time_str}': {e_date_sched}. "
                         "Defaulting to 'now'.")
            flash("Invalid start time format provided. Scheduling jobs for 'now' instead.", "warning")
            base_run_time_utc = datetime.datetime.now(pytz.utc) # Fallback

    # This will be the actual run time for the current operation block (teardown then setup)
    current_operation_block_start_time_utc = base_run_time_utc
    stagger_delta = datetime.timedelta(minutes=stagger_minutes)
    setup_delay_after_teardown = datetime.timedelta(minutes=2) # Time between teardown and setup
    
    scheduled_ops_count = 0
    failed_ops_count = 0
    # Store details of scheduled operations for logging/DB update
    scheduled_op_details_for_interim: List[Dict[str, Any]] = []


    for item_idx, item_to_build in enumerate(buildable_items_from_form):
        item_sf_code = item_to_build.get('sf_course_code', f'UNKNOWN_SF_{item_idx}')
        item_vendor = item_to_build.get('vendor')
        item_lb_course = item_to_build.get('labbuild_course')

        if not item_vendor or not item_lb_course:
            logger.error(f"Skipping item '{item_sf_code}' due to missing vendor or LabBuild course.")
            failed_ops_count += 1 # Count this as a failed scheduling attempt
            continue
            
        assignments = item_to_build.get("assignments", [])
        if not assignments:
            logger.warning(f"No host/pod assignments for '{item_sf_code}'. Skipping scheduling for this item.")
            failed_ops_count += 1
            continue

        for assignment_idx, assignment_block in enumerate(assignments):
            host = assignment_block.get('host')
            start_pod = assignment_block.get('start_pod')
            end_pod = assignment_block.get('end_pod')

            if not host or start_pod is None or end_pod is None:
                logger.error(f"Skipping assignment for '{item_sf_code}' due to missing "
                             f"host/pod range: {assignment_block}")
                failed_ops_count +=1
                continue

            # Calculate the start time for this specific block of operations
            # (teardown + setup for one assignment segment)
            if schedule_option == 'staggered':
                # Only add stagger_delta if it's not the very first assignment block overall
                if item_idx > 0 or assignment_idx > 0:
                    current_operation_block_start_time_utc += stagger_delta
            elif schedule_option == 'now':
                current_operation_block_start_time_utc = datetime.datetime.now(pytz.utc)
            # For 'specific_time_all', current_operation_block_start_time_utc remains base_run_time_utc
            
            # --- Construct base arguments for this item assignment ---
            base_args_for_item_assignment = [
                '-v', item_vendor,
                '-g', item_lb_course,
                '--host', host, 
                '-s', str(start_pod),
                '-e', str(end_pod)
            ]
            # Create a descriptive tag
            tag_type_suffix = item_to_build.get('type','item').split(' ')[0].lower()
            tag = f"{item_sf_code.replace('/', '_').replace(' ', '_')}_{tag_type_suffix}"
            if len(assignments) > 1:
                tag += f"_part{assignment_idx+1}"
            base_args_for_item_assignment.extend(['-t', tag[:45]]) # Keep tag reasonably short

            # Add F5 class number if applicable (ensure key exists in item_to_build)
            f5_class_num = item_to_build.get('f5_class_number') # Assuming this key exists if it's F5
            if item_vendor.lower() == 'f5' and f5_class_num is not None:
                 base_args_for_item_assignment.extend(['-cn', str(f5_class_num)])

            # --- Schedule Teardown Job (if requested) ---
            teardown_job_id = None
            actual_teardown_run_time_utc = current_operation_block_start_time_utc
            if perform_teardown_first:
                args_list_teardown = ['teardown'] + base_args_for_item_assignment
                job_name_td = f"Teardown_{tag}"
                try:
                    trigger_td = DateTrigger(run_date=actual_teardown_run_time_utc, timezone=pytz.utc)
                    job_td = scheduler.add_job(
                        run_labbuild_task, trigger=trigger_td, args=[args_list_teardown],
                        name=job_name_td, misfire_grace_time=1800, # Shorter grace for teardown
                        replace_existing=False 
                    )
                    teardown_job_id = job_td.id
                    scheduled_op_details_for_interim.append({
                        "operation": "teardown", "job_id": job_td.id, "name": job_name_td, 
                        "run_time_utc": actual_teardown_run_time_utc.isoformat(), "args": args_list_teardown
                    })
                    logger.info(f"Scheduled Teardown Job '{job_td.id}': {job_name_td} at "
                                f"{actual_teardown_run_time_utc.astimezone(scheduler_tz_obj).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    scheduled_ops_count += 1
                except Exception as e_sched_td:
                    logger.error(f"Failed to schedule TEARDOWN job {job_name_td}: {e_sched_td}", exc_info=True)
                    failed_ops_count += 1
            
            # --- Schedule Setup Job ---
            args_list_setup = ['setup'] + base_args_for_item_assignment
            # Potentially add --re-build if teardown was just scheduled (optional, teardown should handle clean state)
            # if perform_teardown_first:
            #     args_list_setup.append('--re-build') 
            job_name_setup = f"Setup_{tag}"
            
            actual_setup_run_time_utc = current_operation_block_start_time_utc
            if perform_teardown_first: # If teardown was scheduled, setup runs after it
                actual_setup_run_time_utc = actual_teardown_run_time_utc + setup_delay_after_teardown

            try:
                trigger_setup = DateTrigger(run_date=actual_setup_run_time_utc, timezone=pytz.utc)
                job_setup = scheduler.add_job(
                    run_labbuild_task, trigger=trigger_setup, args=[args_list_setup],
                    name=job_name_setup, misfire_grace_time=7200, # Longer grace for setup
                    replace_existing=False
                )
                scheduled_op_details_for_interim.append({
                    "operation": "setup", "job_id": job_setup.id, "name": job_name_setup,
                    "run_time_utc": actual_setup_run_time_utc.isoformat(), "args": args_list_setup,
                    "depends_on_job_id": teardown_job_id if perform_teardown_first else None
                })
                logger.info(f"Scheduled Setup Job '{job_setup.id}': {job_name_setup} at "
                            f"{actual_setup_run_time_utc.astimezone(scheduler_tz_obj).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                scheduled_ops_count += 1
            except Exception as e_sched_setup:
                logger.error(f"Failed to schedule SETUP job {job_name_setup}: {e_sched_setup}", exc_info=True)
                failed_ops_count += 1
        # End loop for assignment_blocks
    # End loop for item_to_build

    # --- Update interimallocation status for the entire batch ---
    if interim_alloc_collection is not None and batch_review_id:
        final_batch_status = "builds_scheduled_with_errors" # Default if any failures
        if scheduled_ops_count > 0 and failed_ops_count == 0:
            final_batch_status = "builds_fully_scheduled"
        elif scheduled_ops_count == 0 and failed_ops_count > 0:
            final_batch_status = "build_scheduling_all_failed"
        elif scheduled_ops_count == 0 and failed_ops_count == 0: # No buildable items
             final_batch_status = "no_builds_to_schedule"
        
        try:
            # Update all documents in this batch that were confirmed for student/trainer
            update_filter = {
                "batch_review_id": batch_review_id,
                "status": {"$in": ["student_confirmed", "trainer_confirmed", 
                                    "trainer_disabled_by_rule", "trainer_skipped_by_user"]}
            }
            update_doc = {
                "$set": {
                    "status": final_batch_status,
                    "scheduled_operations": scheduled_op_details_for_interim, 
                    "scheduled_at_utc": datetime.datetime.now(pytz.utc)
                }
            }
            update_result = interim_alloc_collection.update_many(update_filter, update_doc)
            logger.info(f"Updated status to '{final_batch_status}' for {update_result.modified_count} items in batch '{batch_review_id}'.")
        except (PyMongoError, NotImplementedError) as e_upd_interim: # Catch general PyMongoError
            logger.error(f"Error updating final interim status for batch '{batch_review_id}': {e_upd_interim}", exc_info=True)

    flash(f"Attempted to schedule operations. Scheduled: {scheduled_ops_count}. "
          f"Failures: {failed_ops_count}.",
          'success' if failed_ops_count == 0 and scheduled_ops_count > 0 else 'warning' if scheduled_ops_count > 0 else 'danger')
    return redirect(url_for('main.index'))

def _dispatch_bulk_labbuild_tasks(
        tasks_to_run: List[Dict[str, Any]], # Each dict: {'args': list, 'description': str, 'db_update_info': tuple (optional)}
        action_name: str,
        operation_if_power_toggle: Optional[str] = None):
    """
    Helper to run a list of labbuild tasks sequentially in a background thread.
    Optionally updates power state in DB after each power toggle task.
    """
    if not tasks_to_run:
        logger.info(f"No tasks to dispatch for bulk {action_name}.")
        return

    def worker():
        total_tasks = len(tasks_to_run)
        logger.info(f"Starting bulk {action_name} worker for {total_tasks} tasks.")
        success_count = 0
        fail_count = 0

        for i, task_info in enumerate(tasks_to_run):
            args = task_info['args']
            description = task_info['description']
            db_update_info = task_info.get('db_update_info') # For power toggle

            logger.info(f"Executing task {i+1}/{total_tasks} for bulk {action_name}: {description} - CMD: {' '.join(args)}")
            try:
                # run_labbuild_task itself handles subprocess.run and logging
                # We assume it's synchronous for this sequential helper,
                # or we'd need a more complex Future-based approach here too.
                # For now, assuming run_labbuild_task blocks until completion.
                run_labbuild_task(args) # This runs the labbuild.py script
                
                # If it's a power toggle and task appeared successful (no exception from run_labbuild_task),
                # update the DB. A more robust way would be for run_labbuild_task to return success/failure.
                if action_name == "power_toggle" and db_update_info and operation_if_power_toggle:
                    tag, course, host, item_num, item_type, class_num_ctx = db_update_info
                    new_power_state_bool = (operation_if_power_toggle == 'start')
                    update_power_state_in_db(tag, course, host, item_num, item_type, new_power_state_bool, class_num_ctx)
                
                success_count += 1
            except Exception as e:
                logger.error(f"Error in bulk {action_name} task {description}: {e}", exc_info=True)
                fail_count += 1
            # Add a small delay between tasks if desired, e.g., time.sleep(1)
        
        logger.info(f"Bulk {action_name} worker finished. Success: {success_count}, Failed: {fail_count}.")
        # Flash messages from a background thread is tricky.
        # Usually, you'd use SSE or another mechanism to update UI.
        # For now, main route will flash a general submission message.

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    flash(f"Submitted {len(tasks_to_run)} tasks for bulk {action_name}. Check logs for progress.", "info")


@bp.route('/bulk-toggle-power', methods=['POST'])
def bulk_toggle_power():
    selected_items_json = request.form.get('selected_items_json')
    operation = request.args.get('operation') # 'start' or 'stop'

    if not selected_items_json or not operation or operation not in ['start', 'stop']:
        flash("Invalid bulk power toggle request.", "danger")
        return redirect(url_for('main.view_allocations', **request.form)) # Pass back filters

    try:
        selected_items = json.loads(selected_items_json)
        if not isinstance(selected_items, list):
            raise ValueError("Selected items data is not a list.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Error processing selected items: {e}", "danger")
        return redirect(url_for('main.view_allocations', **request.form))

    tasks_to_run = []
    for item in selected_items:
        # Validate item structure (basic)
        if not all(k in item for k in ['vendor', 'course', 'host', 'item_type', 'item_number', 'tag']):
            logger.warning(f"Skipping invalid item in bulk power toggle: {item}")
            continue

        args = [
            'manage',
            '-v', item['vendor'],
            '-g', item['course'],
            '--host', item['host'],
            '-t', item['tag'],
            '-o', operation
        ]
        item_num_str = str(item['item_number'])
        if item['item_type'] == 'f5_class':
            args.extend(['-cn', item_num_str])
        elif item['item_type'] == 'pod':
            args.extend(['-s', item_num_str, '-e', item_num_str])
            if item.get('vendor', '').lower() == 'f5' and item.get('class_number'):
                args.extend(['-cn', str(item['class_number'])])
        else:
            logger.warning(f"Unknown item_type '{item['item_type']}' for bulk power toggle. Skipping item: {item.get('tag')}/{item.get('course')}/{item_num_str}")
            continue
        
        description = f"Power {operation} for {item['item_type']} {item_num_str} (Tag: {item['tag']}, Course: {item['course']})"
        db_update_info = (
            item['tag'], item['course'], item['host'], item['item_number'],
            item['item_type'], item.get('class_number') 
        )
        tasks_to_run.append({'args': args, 'description': description, 'db_update_info': db_update_info})

    if tasks_to_run:
        _dispatch_bulk_labbuild_tasks(tasks_to_run, "power_toggle", operation_if_power_toggle=operation)
    else:
        flash("No valid items found to process for bulk power toggle.", "info")
    
    # Preserve filters from the form submission for redirection
    preserved_filters = {k: v for k, v in request.form.items() if k.startswith('filter_') or k in ['page', 'per_page']}
    return redirect(url_for('main.view_allocations', **preserved_filters))


@bp.route('/bulk-teardown-items', methods=['POST'])
def bulk_teardown_items():
    selected_items_json = request.form.get('selected_items_json')
    if not selected_items_json:
        flash("No items selected for bulk teardown.", "warning")
        return redirect(url_for('main.view_allocations', **request.form))

    try:
        selected_items = json.loads(selected_items_json)
        if not isinstance(selected_items, list): raise ValueError
    except:
        flash("Error processing selected items for teardown.", "danger")
        return redirect(url_for('main.view_allocations', **request.form))

    tasks_to_run = []
    for item in selected_items:
        if not all(k in item for k in ['vendor', 'course', 'host', 'item_type', 'item_number', 'tag']):
            logger.warning(f"Skipping invalid item in bulk teardown (infra): {item}")
            continue
        
        args = [
            'teardown', # Not --db-only, so full teardown
            '-v', item['vendor'],
            '-g', item['course'],
            '--host', item['host'],
            '-t', item['tag']
        ]
        item_num_str = str(item['item_number'])
        if item['item_type'] == 'f5_class':
            args.extend(['-cn', item_num_str])
        elif item['item_type'] == 'pod':
            args.extend(['-s', item_num_str, '-e', item_num_str])
            if item.get('vendor', '').lower() == 'f5' and item.get('class_number'):
                args.extend(['-cn', str(item['class_number'])])
        else:
            logger.warning(f"Unknown item_type '{item['item_type']}' for bulk teardown. Skipping.")
            continue
        
        description = f"Teardown Infra for {item['item_type']} {item_num_str} (Tag: {item['tag']}, Course: {item['course']})"
        tasks_to_run.append({'args': args, 'description': description})
        # DB deletion should happen AFTER successful labbuild teardown.
        # This is complex to coordinate from a simple fire-and-forget thread.
        # For now, user will need to run bulk DB delete separately or individual DB delete.

    if tasks_to_run:
        _dispatch_bulk_labbuild_tasks(tasks_to_run, "infrastructure_teardown")
    else:
        flash("No valid items found to process for bulk infrastructure teardown.", "info")

    preserved_filters = {k: v for k, v in request.form.items() if k.startswith('filter_') or k in ['page', 'per_page']}
    return redirect(url_for('main.view_allocations', **preserved_filters))


@bp.route('/bulk-db-delete-items', methods=['POST'])
def bulk_db_delete_items():
    selected_items_json = request.form.get('selected_items_json')
    if not selected_items_json:
        flash("No items selected for bulk DB deletion.", "warning")
        return redirect(url_for('main.view_allocations', **request.form))

    try:
        selected_items = json.loads(selected_items_json)
        if not isinstance(selected_items, list): raise ValueError
    except:
        flash("Error processing selected items for DB deletion.", "danger")
        return redirect(url_for('main.view_allocations', **request.form))

    deleted_count = 0
    failed_count = 0
    processed_tags_courses = set() # To avoid redundant messages for same course in a tag

    for item in selected_items:
        try:
            tag = item.get('tag')
            course_name = item.get('course')
            item_type = item.get('item_type')
            # item_number holds pod_number for type 'pod' or class_number for type 'f5_class'
            item_number = int(item['item_number']) if item.get('item_number') else None
            
            # For F5 pods, class_number context is in item['class_number']
            # For F5 class itself, item_number is class_number and item['class_number'] is also class_number
            class_number_context = int(item['class_number']) if item.get('class_number') else None

            pod_to_delete = None
            class_to_delete = None

            if item_type == 'f5_class':
                class_to_delete = item_number
            elif item_type == 'pod':
                pod_to_delete = item_number
                if item.get('vendor', '').lower() == 'f5': # F5 pod needs class context
                    class_to_delete = class_number_context 
                    if class_to_delete is None: # Should not happen if data is consistent
                        logger.warning(f"F5 pod {pod_to_delete} missing class context for DB delete. Item: {item}")
                        # This might delete all pods with this number if class_to_delete is None in delete_from_database
                        # For safety, skip if F5 pod and class context is missing from the selected item data
                        failed_count +=1
                        continue
            
            if not tag or not course_name:
                logger.warning(f"Skipping item for DB delete due to missing tag/course: {item}")
                failed_count += 1
                continue

            # Call delete_from_database for each distinct item
            # delete_from_database handles if pod_number or class_number is None
            if delete_from_database(tag, course_name=course_name, pod_number=pod_to_delete, class_number=class_to_delete):
                deleted_count += 1
                log_msg_key = (tag, course_name, pod_to_delete, class_to_delete)
                if log_msg_key not in processed_tags_courses:
                    logger.info(f"DB entry deleted for: Tag {tag}, Course {course_name}, Pod {pod_to_delete}, Class {class_to_delete}")
                    processed_tags_courses.add(log_msg_key)
            else:
                failed_count += 1
                logger.error(f"Failed DB delete for: Tag {tag}, Course {course_name}, Pod {pod_to_delete}, Class {class_to_delete}")

        except Exception as e:
            failed_count += 1
            logger.error(f"Error during bulk DB delete for item {item}: {e}", exc_info=True)

    if deleted_count > 0:
        flash(f"Successfully deleted {deleted_count} DB entries.", "success")
    if failed_count > 0:
        flash(f"Failed to delete {failed_count} DB entries. Check logs.", "danger")
    if deleted_count == 0 and failed_count == 0:
        flash("No valid items found to process for DB deletion.", "info")
        
    preserved_filters = {k: v for k, v in request.form.items() if k.startswith('filter_') or k in ['page', 'per_page']}
    return redirect(url_for('main.view_allocations', **preserved_filters))

def set_cell_style(cell, bold: bool = False, italic: bool = False, underline: str = None,
                   strike: bool = False, font_name: str = 'Calibri', font_size: int = 11,
                   font_color: str = None,  # Hex ARGB color string e.g., "FF00FF00" for green
                   alignment_horizontal: str = None, # e.g., 'general', 'left', 'right', 'center', 'justify'
                   alignment_vertical: str = None,   # e.g., 'top', 'center', 'bottom', 'justify', 'distributed'
                   wrap_text: bool = None,
                   shrink_to_fit: bool = None,
                   indent: int = 0,
                   text_rotation: int = 0,
                   border_style: str = None,    # e.g., 'thin', 'medium', 'thick', 'double', 'dotted', 'dashed'
                   border_color: str = "000000", # Default black
                   fill_color: str = None,      # Hex ARGB color string e.g., "FFFFFF00" for yellow
                   number_format: str = None    # e.g., 'General', '0.00', 'mm-dd-yy'
                   ):
    """
    Applies comprehensive styling to an openpyxl cell.
    Creates new style objects as openpyxl styles are immutable.
    """
    # --- Font ---
    # Start with defaults or existing font properties
    current_font = cell.font
    new_font_kwargs = {
        'name': current_font.name if current_font.name is not None else font_name,
        'sz': current_font.sz if current_font.sz is not None else font_size,
        'bold': current_font.bold if bold is None else bold,
        'italic': current_font.italic if italic is None else italic,
        'underline': current_font.underline if underline is None else underline,
        'strike': current_font.strike if strike is None else strike,
        'color': current_font.color if font_color is None else (Color(font_color) if isinstance(font_color, str) else font_color)
        # Copy other font properties if needed like scheme, family, charset, etc.
        # For simplicity, we'll stick to common ones here.
    }
    # Only create a new Font object if there's a reason to (a change or explicit setting)
    # This check can be more granular if performance is critical for millions of cells.
    cell.font = Font(**new_font_kwargs)

    # --- Alignment ---
    if (alignment_horizontal is not None or
            alignment_vertical is not None or
            wrap_text is not None or
            shrink_to_fit is not None or
            indent != 0 or # Default indent is 0, so any non-zero is a change
            text_rotation != 0): # Default rotation is 0

        existing_alignment = cell.alignment
        new_align_kwargs = {}

        # Preserve existing alignment properties if not overridden
        if existing_alignment:
            new_align_kwargs.update({
                'horizontal': existing_alignment.horizontal,
                'vertical': existing_alignment.vertical,
                'textRotation': existing_alignment.textRotation, # openpyxl uses textRotation (int)
                'wrapText': existing_alignment.wrapText,
                'shrinkToFit': existing_alignment.shrinkToFit,
                'indent': existing_alignment.indent,
                'relativeIndent': existing_alignment.relativeIndent,
                'justifyLastLine': existing_alignment.justifyLastLine,
                'readingOrder': existing_alignment.readingOrder,
            })

        # Apply new/overridden values
        if alignment_horizontal is not None:
            new_align_kwargs['horizontal'] = alignment_horizontal
        if alignment_vertical is not None:
            new_align_kwargs['vertical'] = alignment_vertical
        if wrap_text is not None:
            new_align_kwargs['wrapText'] = wrap_text
        if shrink_to_fit is not None:
            new_align_kwargs['shrinkToFit'] = shrink_to_fit
        if indent != 0: # Only set if different from default
            new_align_kwargs['indent'] = indent
        if text_rotation != 0: # Only set if different from default
            new_align_kwargs['textRotation'] = text_rotation
        
        cell.alignment = Alignment(**new_align_kwargs)

    # --- Border ---
    if border_style:
        side = Side(border_style=border_style, color=border_color)
        cell.border = Border(left=side, right=side, top=side, bottom=side)

    # --- Fill ---
    if fill_color:
        cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")

    # --- Number Format ---
    if number_format:
        cell.number_format = number_format

def get_host_shortname(hostname_fqdn_or_short: str) -> str:
    """Returns a standardized short host name (ni, cl, ul, un, ho, k2) or original if not matched."""
    if not hostname_fqdn_or_short:
        return "N/A"
    
    hn_lower = hostname_fqdn_or_short.lower()
    
    if "nightbird" in hn_lower or hn_lower == "ni": return "Ni"
    if "cliffjumper" in hn_lower or hn_lower == "cl": return "Cl"
    if "ultramagnus" in hn_lower or hn_lower == "ul": return "Ul"
    if "unicron" in hn_lower or hn_lower == "un": return "Un"
    if "hotshot" in hn_lower or hn_lower == "ho": return "Ho"
    if "k2" in hn_lower : return "K2" # Assuming K2 is another short name

    # Fallback to first two letters if it's an FQDN and not recognized
    if '.' in hn_lower:
        short = hn_lower.split('.')[0][:2]
        if short == "ni": return "Ni"
        # Add other FQDN prefix mappings if needed
    
    return hostname_fqdn_or_short # Return original if no specific mapping

@bp.route('/export-allocations-excel')
def export_allocations_excel():
    logger.info("Exporting allocations to Excel (New Format)...")
    if alloc_collection is None or course_config_collection is None or host_collection is None:
        flash("Database collections unavailable for export.", "danger")
        return redirect(url_for('main.view_allocations'))

    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "Allocations"

        # --- 1. RAM Summary Header (as per image) ---
        ram_summary_data_from_image = [
            ["RAM Summary", None, None, None], # Merged later
            [None, "Allocated RAM", "CPU", "HDD"],
            ["Nightbird", 2000, 2000, 2000],
            ["Hotshot", 2000, 2000, 2000],
            ["Cliffjumper", 1200, 1200, 1200],
            ["Unicron", 1000, 1000, 1000],
            ["Ultramagnus", 1000, 1000, 1000],
            ["K2", 1000, 1000, 1000] # Added K2 based on common hosts
        ]
        yellow_fill = "FFFF00"
        for r_idx, r_data in enumerate(ram_summary_data_from_image, start=1):
            for c_idx, value in enumerate(r_data, start=1):
                if value is not None:
                    cell = ws.cell(row=r_idx, column=c_idx, value=value)
                    is_header_row = r_idx <= 2
                    set_cell_style(cell, bold=is_header_row, fill_color=yellow_fill, alignment_horizontal="center" if is_header_row or c_idx > 1 else "left")
        ws.merge_cells('A1:D1')
        current_row = len(ram_summary_data_from_image) + 2 # Start main table after some space

        # --- Main Table Headers (from image) ---
        main_headers = [
            "Course Job Code", "Start Date", "Last Day", "Location", "Trainer Name", "Course Name",
            "Start/End Pod", None, "Username", "Password", "Class", "Students", "Vendor Pods", "Group",
            "Version", "Course Version", "RAM", "Virtual Hosts", "Ni", "Cl", "Ul", "Un", "Ho", "K2" # Added K2 here too
        ] # Col H is for the "->" arrow

        header_fill_color = "ADD8E6" # Light Blue
        thin_black_border = Border(left=Side(style='thin', color="000000"), 
                                 right=Side(style='thin', color="000000"), 
                                 top=Side(style='thin', color="000000"), 
                                 bottom=Side(style='thin', color="000000"))

        # --- Data Fetching and Processing ---
        all_alloc_docs = list(alloc_collection.find({}))
        all_course_configs_list = list(course_config_collection.find({}))
        course_configs_dict = {cc['course_name']: cc for cc in all_course_configs_list}
        
        # Get host locations mapping if available
        host_locations = {h.get('host_name'): h.get('location', 'Virtual') for h in host_collection.find({}, {"host_name":1, "location":1})}


        processed_data_by_vendor = defaultdict(list)

        for tag_doc in all_alloc_docs:
            tag_name = tag_doc.get("tag", "UNKNOWN_TAG")
            for course_alloc in tag_doc.get("courses", []):
                vendor_code = course_alloc.get("vendor", "UN").upper()
                labbuild_course_name = course_alloc.get("course_name")
                course_config = course_configs_dict.get(labbuild_course_name, {})

                for pod_detail in course_alloc.get("pod_details", []):
                    is_trainer_pod = "-TP" in tag_name.upper()
                    excel_row_data = {
                        "Course Job Code": tag_name if not is_trainer_pod else tag_name.replace("-TP",""), # Base code for TP
                        "Start Date": course_alloc.get("start_date", "N/A") if not is_trainer_pod else "",
                        "Last Day": course_alloc.get("end_date", "N/A") if not is_trainer_pod else "", # Assuming end_date is last day
                        "Location": host_locations.get(pod_detail.get("host"), "Virtual"),
                        "Trainer Name": course_alloc.get("trainer_name", "N/A") if not is_trainer_pod else "",
                        "Course Name": labbuild_course_name if not is_trainer_pod else "Trainer pods",
                        "Username": course_config.get("student_user", "labcp-X/labpa-X") if not is_trainer_pod else \
                                    (course_config.get("trainer_user", "labcp-X") if "-TP" in tag_name.upper() else ""),
                        "Password": course_config.get("student_password", "password") if not is_trainer_pod else \
                                    (course_config.get("trainer_password", "password") if "-TP" in tag_name.upper() else ""),
                        "Class": "", "Students": "", "Vendor Pods": "", # To be filled
                        "Group": course_config.get("group", ""), # From courseconfig
                        "Version": labbuild_course_name, # Main LabBuild course name
                        "Course Version": course_config.get("course_version", course_config.get("build#", "")), # Specific build/version
                        "RAM": 0, # To be calculated
                        "Virtual Hosts": "", # Representative VM
                        "Ni": 0, "Cl": 0, "Ul": 0, "Un": 0, "Ho": 0, "K2": 0, # Host RAM allocations
                        "_host_shortname": get_host_shortname(pod_detail.get("host")), # Internal helper
                        "_original_host_fqdn": pod_detail.get("host")
                    }

                    # Determine representative Virtual Host (e.g., first component)
                    components_list = course_config.get("components", [])
                    if not components_list and course_config.get("groups"): # F5 structure
                        for grp in course_config.get("groups",[]): components_list.extend(grp.get("component",[]))
                    
                    if components_list:
                        first_comp = components_list[0]
                        base_vm_name = first_comp.get("base_vm", first_comp.get("component_name", "N/A"))
                        excel_row_data["Virtual Hosts"] = base_vm_name


                    # Pods, Class, Students calculation
                    pod_num = pod_detail.get("pod_number")
                    class_num = pod_detail.get("class_number")
                    num_student_pods_in_range = 0

                    if vendor_code == "F5" and class_num is not None:
                        excel_row_data["Class"] = class_num
                        f5_student_pods = pod_detail.get("pods", [])
                        if isinstance(f5_student_pods, list):
                            excel_row_data["Students"] = len(f5_student_pods)
                            excel_row_data["Vendor Pods"] = ", ".join(sorted([str(p.get("pod_number")) for p in f5_student_pods if p.get("pod_number") is not None]))
                            num_student_pods_in_range = len(f5_student_pods)
                        # For F5, "Start/End Pod" in Excel might refer to the class number itself if no student pods, or range of student pods
                        excel_row_data["Start/End Pod"] = str(class_num) if not f5_student_pods else f"{min(p['pod_number'] for p in f5_student_pods)}-{max(p['pod_number'] for p in f5_student_pods)}"

                    elif pod_num is not None: # Non-F5 or F5 student pod without class context (unlikely from your data)
                        # The image for PA/CP shows Start/End Pod as a range.
                        # This pod_detail from currentallocation is often a single pod_number from build.
                        # We need to infer the original range if this is a student pod.
                        # For simplicity here, if it's a single pod, Start/End will be the same.
                        # Your "-TP" logic might override this.
                        excel_row_data["Start/End Pod"] = f"{pod_num} -> {pod_num}" # Default if range not obvious from this single entry
                        excel_row_data["Students"] = 1 # Assume 1 student per basic pod_detail entry unless grouped
                        excel_row_data["Vendor Pods"] = str(pod_num)
                        num_student_pods_in_range = 1
                    
                    # Override Start/End Pod for Trainer Pods (often a different range)
                    if is_trainer_pod:
                        # Trainer pods often have a specific range in their tag or pre-defined
                        # This part needs logic to correctly parse trainer pod ranges from tag or config
                        # Example: If tag "COURSE-TP-101-102"
                        tp_range_match = re.search(r'-(\d+)-(\d+)$', tag_name)
                        if tp_range_match:
                            excel_row_data["Start/End Pod"] = f"{tp_range_match.group(1)} -> {tp_range_match.group(2)}"
                            excel_row_data["Students"] = int(tp_range_match.group(2)) - int(tp_range_match.group(1)) + 1
                        else: # Fallback if TP range not in tag
                            excel_row_data["Start/End Pod"] = f"{pod_num} -> {pod_num}" if pod_num else "N/A"
                            excel_row_data["Students"] = 1 if pod_num else 0
                        excel_row_data["Username"] = course_config.get("trainer_user", excel_row_data["Username"])
                        excel_row_data["Password"] = course_config.get("trainer_password", excel_row_data["Password"])


                    # Calculate RAM for this row based on number of "student" pods and host
                    # This is a complex part based on your image.
                    # The RAM per host (Ni, Cl, etc.) is shown per *course line item*.
                    # It's the total RAM for the pods in THAT line item on THAT host.
                    # If a course like CP Maestro is split (e.g. 2 pods on Unicron, 1 on Hotshot),
                    # it appears as multiple lines in your Excel for the same "Course Job Code".
                    
                    ram_per_pod_from_config = float(course_config.get("memory_gb_per_pod", course_config.get("memory", 0)))
                    if isinstance(ram_per_pod_from_config, str) : # handle if memory is string
                        try: ram_per_pod_from_config = float(ram_per_pod_from_config)
                        except ValueError: ram_per_pod_from_config = 0.0
                    
                    calculated_ram_for_row = ram_per_pod_from_config * num_student_pods_in_range if num_student_pods_in_range > 0 else ram_per_pod_from_config # if 0 students, maybe it's a single entity ram
                    if is_trainer_pod: # Trainer pods might have different RAM or count as 1 entity
                        calculated_ram_for_row = ram_per_pod_from_config # Assuming trainer pod RAM is per entity
                        
                    excel_row_data["RAM"] = calculated_ram_for_row
                    
                    host_short_key = excel_row_data["_host_shortname"]
                    if host_short_key in ["Ni", "Cl", "Ul", "Un", "Ho", "K2"]:
                        excel_row_data[host_short_key] = calculated_ram_for_row
                    
                    processed_data_by_vendor[vendor_code].append(excel_row_data)

        # --- Write to Excel ---
        # Define vendor order for sheet
        vendor_display_order = ["PA", "CP", "NU", "F5", "AV", "PR", "OT"] # Add others as needed

        for vendor_code_ordered in vendor_display_order:
            if vendor_code_ordered not in processed_data_by_vendor:
                continue
            
            vendor_specific_data = processed_data_by_vendor[vendor_code_ordered]
            if not vendor_specific_data:
                continue

            # Vendor Section Title (e.g., "PA Courses")
            title_cell = ws.cell(row=current_row, column=1, value=f"{vendor_code_ordered} Courses")
            ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(main_headers))
            set_cell_style(title_cell, bold=True, fill_color=header_fill_color, alignment_horizontal="left", font_size=12)
            current_row += 1

            # Write Main Headers for this vendor section
            for col_idx, header_text in enumerate(main_headers, start=1):
                cell = ws.cell(row=current_row, column=col_idx, value=header_text if header_text is not None else "")
                set_cell_style(cell, bold=True, fill_color=header_fill_color, border_style="thin", alignment_horizontal="center", wrap_text=True, font_size=10)
            current_row += 1
            
            vendor_total_pods_count = 0

            for item_data in vendor_specific_data:
                col_offset = 0
                for header_key in main_headers: # Iterate based on header order
                    if header_key is None: # For the "->" arrow column
                        ws.cell(row=current_row, column=col_offset + 1, value="->")
                        set_cell_style(ws.cell(row=current_row, column=col_offset + 1), border_style="thin", alignment_horizontal="center")
                    elif header_key == "Start/End Pod": # Special handling for combined field
                         cell_val = item_data.get(header_key, "")
                         ws.cell(row=current_row, column=col_offset + 1, value=str(cell_val).split(" -> ")[0] if " -> " in str(cell_val) else str(cell_val) ) # Start
                         set_cell_style(ws.cell(row=current_row, column=col_offset + 1), border_style="thin", alignment_horizontal="center")
                         # Arrow in next column (which was None in headers)
                         ws.cell(row=current_row, column=col_offset + 2, value="->")
                         set_cell_style(ws.cell(row=current_row, column=col_offset + 2), border_style="thin", alignment_horizontal="center")
                         # End pod value in column after arrow
                         ws.cell(row=current_row, column=col_offset + 3, value=str(cell_val).split(" -> ")[1] if " -> " in str(cell_val) else "")
                         set_cell_style(ws.cell(row=current_row, column=col_offset + 3), border_style="thin", alignment_horizontal="center")
                         col_offset += 1 # Account for the extra column used by "->" and end pod
                    else:
                        cell_val = item_data.get(header_key, "")
                        ws.cell(row=current_row, column=col_offset + 1, value=cell_val)
                        align_h = "center" if header_key in ["Class", "Students", "Vendor Pods", "RAM", "Ni","Cl","Ul","Un","Ho","K2"] else "left"
                        set_cell_style(ws.cell(row=current_row, column=col_offset + 1), border_style="thin", alignment_horizontal=align_h, alignment_vertical="top", wrap_text=True)
                    col_offset += 1
                current_row += 1
                
                # Accumulate pods for vendor total (using "Students" count for now, needs refinement)
                try: vendor_total_pods_count += int(item_data.get("Students", 0))
                except ValueError: pass


            # Total Pods for Vendor
            if vendor_specific_data: # Only add total if there was data
                total_pods_label_cell = ws.cell(row=current_row, column=main_headers.index("Students") + 1 -1, value="Total Pods") # Col before "Students"
                total_pods_value_cell = ws.cell(row=current_row, column=main_headers.index("Students") + 1, value=vendor_total_pods_count)
                
                set_cell_style(total_pods_label_cell, bold=True, fill_color=header_fill_color, border_style="thin", alignment_horizontal="right")
                set_cell_style(total_pods_value_cell, bold=True, fill_color=header_fill_color, border_style="thin", alignment_horizontal="center")
                
                # Merge cells for "Total Pods" label across preceding columns
                label_col_start_merge = 1
                label_col_end_merge = main_headers.index("Students") + 1 - 2 # Up to column before label
                if label_col_end_merge >= label_col_start_merge:
                     ws.merge_cells(start_row=current_row, start_column=label_col_start_merge, end_row=current_row, end_column=label_col_end_merge)
                     for c_merge in range(label_col_start_merge, label_col_end_merge + 1):
                        set_cell_style(ws.cell(row=current_row, column=c_merge), fill_color=header_fill_color, border_style="thin")
                
                # Style remaining cells in total row
                ram_col_idx = main_headers.index("RAM") +1
                host_ram_cols_start_idx = main_headers.index("Ni") + 1
                
                # Sum RAM for hosts in this vendor section
                host_ram_sums = defaultdict(float)
                for item_d in vendor_specific_data:
                    for host_s_key in ["Ni", "Cl", "Ul", "Un", "Ho", "K2"]:
                        try: host_ram_sums[host_s_key] += float(item_d.get(host_s_key, 0))
                        except ValueError: pass
                
                # Write summed host RAM
                ws.cell(row=current_row, column=ram_col_idx, value=sum(host_ram_sums.values())) # Total RAM column
                set_cell_style(ws.cell(row=current_row, column=ram_col_idx), bold=True, fill_color=header_fill_color, border_style="thin", alignment_horizontal="center")


                for i_host_key, host_s_key_val in enumerate(["Ni", "Cl", "Ul", "Un", "Ho", "K2"]):
                    cell = ws.cell(row=current_row, column=host_ram_cols_start_idx + i_host_key, value=host_ram_sums[host_s_key_val])
                    set_cell_style(cell, bold=True, fill_color=header_fill_color, border_style="thin", alignment_horizontal="center")

                # Style any remaining cells in the total row (e.g. Virtual Hosts)
                for c_empty_total in range(main_headers.index("Vendor Pods") + 2, ram_col_idx): # Cols between Vendor Pods and RAM
                    set_cell_style(ws.cell(row=current_row, column=c_empty_total), fill_color=header_fill_color, border_style="thin")
                for c_empty_total_after in range(host_ram_cols_start_idx + len(["Ni", "Cl", "Ul", "Un", "Ho", "K2"]), len(main_headers) + 1): # After last host RAM col
                    set_cell_style(ws.cell(row=current_row, column=c_empty_total_after), fill_color=header_fill_color, border_style="thin")


                current_row += 1
            current_row += 2 # Extra space between vendor blocks

        # --- Auto-size columns (basic fit) ---
        # This is a basic auto-size, might not be perfect for merged cells or very long content.
        # Set specific widths for better control if needed.
        column_widths_map = {
            'A': 20, 'B': 12, 'C': 12, 'D': 15, 'E': 22, 'F': 50, # Course Job Code to Course Name
            'G': 12, 'H': 3, 'I': 12, # Start/End Pod, Arrow, End Pod, Username
            'J': 12, 'K': 7, 'L': 7, 'M': 15, # Password, Class, Students, Vendor Pods
            'N': 25, 'O': 25, 'P': 25, # Group, Version, Course Version
            'Q': 7, 'R': 30, # RAM, Virtual Hosts
            'S': 4, 'T': 4, 'U': 4, 'V': 4, 'W': 4, 'X': 4 # Ni, Cl, Ul, Un, Ho, K2
        }
        for col_letter, width in column_widths_map.items():
            ws.column_dimensions[col_letter].width = width
        # Example for specific columns from your image:
        # ws.column_dimensions['A'].width = 20 # Course Job Code
        # ws.column_dimensions['F'].width = 60 # Course Name
        # ... and so on for all 24 columns ...

        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)

        logger.info("Excel export generated successfully (new format).")
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name="labbuild_allocations_detailed.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logger.error(f"Error generating detailed Excel export: {e}", exc_info=True)
        flash("Error generating detailed Excel export. Please check server logs.", "danger")
        return redirect(url_for('main.view_allocations'))