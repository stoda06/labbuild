# dashboard/routes/actions.py

import logging
import threading
import datetime
import pytz
import json
from flask import (
    Blueprint, request, redirect, url_for, flash, current_app, jsonify, render_template
)
from pymongo.errors import PyMongoError
import pymongo
# Import extensions, utils, tasks from dashboard package
from ..extensions import (
    scheduler,
    db,
    interim_alloc_collection, # For saving review data
    alloc_collection,         # For toggle_power and teardown_tag
    host_collection,          # For getting host list in build_review
    course_config_collection  # For getting memory in build_review
)

from ..utils import build_args_from_form, parse_command_line, update_power_state_in_db, get_hosts_available_memory_parallel
from ..tasks import run_labbuild_task

# Import top-level utils if needed (e.g., delete_from_database)
from config_utils import get_host_by_name
from db_utils import delete_from_database

# Define Blueprint
bp = Blueprint('actions', __name__)
logger = logging.getLogger('dashboard.routes.actions')


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
            local_dt = scheduler_tz.localize(naive_dt) # Localize to scheduler's TZ
            run_date_utc = local_dt.astimezone(pytz.utc) # Convert to UTC for storage/trigger
            trigger = DateTrigger(run_date=run_date_utc)
            flash_msg = f"for {local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')}"
            log_time_str = f"UTC: {run_date_utc.isoformat()}, SchedulerTZ: {local_dt.isoformat()}"

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
        first_run_local = scheduler_tz.localize(naive_dt)
        first_run_utc = first_run_local.astimezone(pytz.utc)
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
    current_theme = request.cookies.get('theme', 'light')
    selected_courses_json = request.form.get('selected_courses')
    processed_courses_for_review = []
    save_error_flag = False
    saved_count = 0
    updated_count = 0

    if not selected_courses_json:
        flash("No courses selected for review.", "warning")
        return redirect(url_for('main.view_upcoming_courses'))

    try:
        selected_courses_input = json.loads(selected_courses_json)
        if not isinstance(selected_courses_input, list) or not selected_courses_input:
             flash("Invalid course data received.", "danger")
             return redirect(url_for('main.view_upcoming_courses'))

        # --- Fetch Host Information & FILTER ---
        hosts_to_check = []
        all_host_names_in_db = []
        if host_collection is not None:
             try:
                 # Fetch full documents for hosts marked for inclusion
                 hosts_cursor = host_collection.find({"include_for_build": "true"})
                 hosts_to_check = list(hosts_cursor) # List of host document dicts
                 all_host_names_in_db = [h['host_name'] for h in hosts_to_check if 'host_name' in h]
                 logger.info(f"Found {len(hosts_to_check)} hosts marked with include_for_build=true: {all_host_names_in_db}")
                 if not hosts_to_check:
                      flash("No hosts are marked for build inclusion in the database configuration.", "warning")
             except PyMongoError as e:
                 logger.error(f"Failed to fetch/filter host list from DB: {e}")
                 flash("Could not retrieve host list from database.", "warning")
        else:
            flash("Host collection unavailable.", "danger")
            # Proceeding without hosts will cause assignments to fail below

        # --- Fetch Host Memory IN PARALLEL (Using Filtered List) ---
        host_capacities_gb = {}
        if hosts_to_check: # Only fetch if there are hosts to check
            host_capacities_gb = get_hosts_available_memory_parallel(hosts_to_check)
        else:
             logger.warning("Skipping host memory check as no hosts are marked for inclusion.")


        # --- Process Each Selected Course ---
        for course in selected_courses_input:
            # ... (get labbuild_course, vendor, pods_req - same as before) ...
            labbuild_course = course.get('labbuild_course')
            vendor = course.get('vendor')
            pods_req_str = course.get('sf_pods_req', '1'); pods_req = 1
            try: pods_req = max(1, int(pods_req_str)) # Ensure >= 1 pod
            except: logger.warning(f"Invalid pods_req '{pods_req_str}', using 1.")

            host_assignments = {}
            memory_gb_per_pod = 0
            error_determining_hosts = None

            # 1. Get Memory Requirement (same as before)
            if course_config_collection is not None and labbuild_course:
                try:
                    # ... (logic to get memory_gb_per_pod remains the same) ...
                    config = course_config_collection.find_one({"course_name": labbuild_course})
                    if config:
                         memory_gb_per_pod = float(config.get('memory', 0))
                         if memory_gb_per_pod <= 0: error_determining_hosts = "Memory requirement unknown"
                    else: error_determining_hosts = "Course config not found"
                except Exception as e: error_determining_hosts = "DB/Config error"; logger.error(f"Err getting memory {labbuild_course}: {e}")
            else: error_determining_hosts = "DB unavailable/No course"

            # 2. Determine Host Priority & Assign Pods (if memory found)
            if error_determining_hosts is None and memory_gb_per_pod > 0:
                total_memory_needed_gb = pods_req * memory_gb_per_pod
                logger.info(f"Course '{labbuild_course}': Needs {pods_req} pods, Total: {total_memory_needed_gb:.2f} GB.")

                # Determine priority list (same logic as before)
                priority_hosts = []
                course_code = course.get('sf_course_code','').lower()
                if vendor == 'pa' and 'cortex' not in course_code: priority_hosts = ['nightbird', 'hotshot']
                elif vendor == 'pa' and 'cortex' in course_code: priority_hosts = ['unicron', 'ultramagnus', 'cliffjumper', 'nightbird']
                elif vendor == 'f5': priority_hosts = ['ultramagnus', 'cliffjumper', 'unicron', 'nightbird']
                elif vendor == 'cp': priority_hosts = ['unicron', 'ultramagnus', 'cliffjumper', 'nightbird']
                elif vendor == 'nu': priority_hosts = ['hotshot']
                else: priority_hosts = ['unicron', 'ultramagnus', 'cliffjumper', 'nightbird', 'hotshot', 'apollo'] # Default order?

                # *** Filter priority list based on hosts marked for inclusion AND having capacity data ***
                available_priority_hosts = [
                    h for h in priority_hosts
                    if h in all_host_names_in_db # Must be in the initial DB list marked for inclusion
                    and h in host_capacities_gb   # Must have capacity data returned
                    and host_capacities_gb[h] is not None # Capacity fetch must not have failed
                ]
                logger.debug(f"Filtered priority hosts for '{labbuild_course}': {available_priority_hosts}")

                # Assign Pods (using a mutable copy of capacities for this assignment round)
                current_round_capacities = host_capacities_gb.copy() # Copy capacities for this course calculation
                pods_assigned = 0
                for host_prio in available_priority_hosts:
                    # Use the capacity from our current round copy
                    available_mem_host = current_round_capacities.get(host_prio, 0)
                    if available_mem_host is None: available_mem_host = 0 # Treat fetch failure as 0 capacity

                    pods_on_this_host = 0
                    while pods_assigned < pods_req:
                        if available_mem_host >= memory_gb_per_pod:
                            available_mem_host -= memory_gb_per_pod
                            pods_on_this_host += 1
                            pods_assigned += 1
                        else: break # Not enough memory

                    if pods_on_this_host > 0:
                        host_assignments[host_prio] = pods_on_this_host
                        logger.info(f"Assigned {pods_on_this_host} pods of '{labbuild_course}' to host '{host_prio}'.")
                        # Update the capacity *in the copy* for this round
                        current_round_capacities[host_prio] = available_mem_host

                    if pods_assigned >= pods_req: break # All assigned

                if pods_assigned < pods_req:
                    logger.error(f"Insufficient capacity for course '{labbuild_course}'. Assigned {pods_assigned}/{pods_req} pods.")
                    error_determining_hosts = f"Insufficient capacity (assigned {pods_assigned}/{pods_req})"

            # 3. Add processed course data (same as before)
            processed_course = course.copy()
            if error_determining_hosts: processed_course['host_assignments'] = {"Error": error_determining_hosts}
            elif not host_assignments: processed_course['host_assignments'] = {"Error": "No suitable hosts found"}
            else: processed_course['host_assignments'] = host_assignments
            processed_courses_for_review.append(processed_course)
            # --- End Course Loop ---

        # --- Save to interimallocation collection ---
        if interim_alloc_collection is None:
            flash("Interim allocation database collection is not available.", "danger")
            logger.error("interim_alloc_collection is None. Cannot save.")
            save_error_flag = True
        elif not processed_courses_for_review:
             flash("No valid courses processed for review/saving.", "warning")
        else:
            try:
                batch_timestamp = datetime.datetime.now(pytz.utc)
                bulk_ops = []
                for p_course in processed_courses_for_review:
                    # --- Define the UNIQUE FILTER for upsert ---
                    # Using SF Course Code and selected LabBuild Course seems reasonable
                    # Adjust if your definition of a unique "pending build" is different
                    filter_criteria = {
                        "sf_course_code": p_course.get("sf_course_code"),
                        "labbuild_course": p_course.get("labbuild_course"),
                        "status": "pending_review" # Only update existing *pending* reviews? Or any status? Let's assume pending.
                    }
                    # Remove keys with None values from filter if necessary,
                    # though matching None might be intended depending on your data.
                    filter_criteria = {k: v for k, v in filter_criteria.items() if v is not None}

                    # Define the update document ($set will overwrite fields)
                    update_data = {
                        "$set": {
                            "sf_course_type": p_course.get('sf_course_type'),
                            "sf_start_date": p_course.get('sf_start_date'),
                            "sf_end_date": p_course.get('sf_end_date'),
                            "sf_trainer": p_course.get('sf_trainer'),
                            "sf_pax": p_course.get('sf_pax'),
                            "sf_pods_req": p_course.get('sf_pods_req'),
                            "vendor": p_course.get('vendor'),
                            "host_assignments": p_course.get('host_assignments', {}),
                            "review_timestamp": batch_timestamp, # Update timestamp on modification
                            "status": "pending_review" # Ensure status is set/reset
                            # Add any other fields from p_course you want to save/update
                        },
                        # Optional: $setOnInsert can set fields only when creating a new doc
                        # "$setOnInsert": {
                        #     "initial_review_timestamp": batch_timestamp
                        # }
                    }

                    # Create an UpdateOne operation with upsert=True
                    bulk_ops.append(pymongo.UpdateOne(filter_criteria, update_data, upsert=True))

                if bulk_ops:
                    logger.info(f"Performing {len(bulk_ops)} upsert operations into interimallocation.")
                    # Execute using bulk_write for efficiency
                    result = interim_alloc_collection.bulk_write(bulk_ops)
                    upserted_count = result.upserted_count
                    modified_count = result.matched_count # In upsert=True, matched but not modified isn't easily distinguished here, bulk_write modified_count is often 0 for upserts. Use matched_count.
                    # Refine counts based on upserted IDs
                    inserted_count = len(result.upserted_ids) if result.upserted_ids else 0
                    updated_count = result.matched_count # Assume matched were updated

                    logger.info(f"Interim allocation upsert result: Inserted={inserted_count}, Matched/Updated={updated_count}")
                    flash(f"{inserted_count} new course(s) added, {updated_count} existing course(s) updated for review.", "success")
                else:
                     flash("No valid course documents to save/update.", "warning")

            except pymongo.errors.BulkWriteError as bwe:
                 logger.error(f"Bulk write error saving to interimallocation: {bwe.details}", exc_info=True)
                 flash("Database error during bulk save/update.", "danger")
                 save_error_flag = True
            except PyMongoError as e:
                logger.error(f"Failed to save/update selected courses in interimallocation: {e}", exc_info=True)
                flash("Database error saving/updating courses for review.", "danger")
                save_error_flag = True
            except Exception as e:
                 logger.error(f"Unexpected error during interimallocation upsert: {e}", exc_info=True)
                 flash("An unexpected error occurred while saving.", "danger")
                 save_error_flag = True

        # --- Render the review template ---
        return render_template(
            'intermediate_build_review.html',
            selected_courses=processed_courses_for_review,
            save_error=save_error_flag,
            current_theme=current_theme
        )

    except json.JSONDecodeError:
        flash("Invalid data format received from previous page.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))
    except Exception as e:
        logger.error(f"Error in intermediate_build_review route: {e}", exc_info=True)
        flash("An error occurred preparing the build review.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))