# dashboard/routes/labbuild_actions.py

import logging
import threading
import datetime
import pytz
from flask import (
    Blueprint, request, redirect, url_for, flash
)
from ..extensions import scheduler, op_logs_collection
from ..utils import build_args_from_form, parse_command_line
from ..tasks import run_labbuild_task
from .main import get_user_timezone

bp = Blueprint('labbuild_actions', __name__)
logger = logging.getLogger('dashboard.routes.labbuild_actions')


@bp.route('/rerun/<run_id>', methods=['POST'])
def rerun_job(run_id):
    """
    Fetches the arguments from a previous run and re-submits it as a new
    immediate job.
    """
    logger.info(f"Received re-run request for run_id: {run_id}")

    if op_logs_collection is None:
        flash("Database collection for operation logs is unavailable.", "danger")
        return redirect(request.referrer or url_for('main.all_logs'))

    try:
        # Find the original log entry
        original_log = op_logs_collection.find_one({"run_id": run_id})
        if not original_log:
            flash(f"Could not find original run log for ID: {run_id}", "danger")
            return redirect(request.referrer or url_for('main.all_logs'))

        # Extract the original arguments dictionary
        original_args_dict = original_log.get('args')
        if not original_args_dict or not isinstance(original_args_dict, dict):
            flash(f"No valid arguments found in the log for run ID: {run_id}", "danger")
            return redirect(request.referrer or url_for('main.all_logs'))

        # --- Reconstruct the argument list for the subprocess ---
        # This logic is similar to build_args_from_dict but simpler
        # as we are just converting a stored dictionary back to a list.
        command = original_args_dict.get('command')
        if not command:
            flash("Original command not found in log arguments.", "danger")
            return redirect(request.referrer or url_for('main.all_logs'))
        
        args_list = [command]
        
        # Mapping of keys to flags
        arg_map = {
            'vendor': '-v', 'course': '-g', 'host': '--host',
            'start_pod': '-s', 'end_pod': '-e', 'class_number': '-cn', 'tag': '-t',
            'component': '-c', 'operation': '-o', 'thread': '-th',
            'clonefrom': '--clonefrom', 'memory': '-mem', 'prtg_server': '--prtg-server',
            'datastore': '-ds', 'start_date': '--start-date', 'end_date': '--end-date',
            'trainer_name': '--trainer-name', 'username': '--username', 'password': '--password'
        }
        
        for key, flag in arg_map.items():
            value = original_args_dict.get(key)
            if value is not None and value != '':
                args_list.extend([flag, str(value)])
        
        # Handle boolean flags
        bool_flags = {
            're_build': '--re-build', 'full': '--full', 'monitor_only': '--monitor-only',
            'db_only': '--db-only', 'perm': '--perm', 'verbose': '--verbose'
        }
        for key, flag in bool_flags.items():
            if original_args_dict.get(key) is True:
                args_list.append(flag)

        # Run the command in a background thread
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True)
        thread.start()
        
        flash(f"Re-running command from log {run_id}. New job submitted.", 'info')
        logger.info(f"Re-submitted job from run {run_id} with command: {' '.join(args_list)}")

    except Exception as e:
        logger.error(f"Failed to re-run job from log {run_id}: {e}", exc_info=True)
        flash(f"Error re-running job: {e}", 'danger')

    # Redirect back to the page the user came from (either all_logs or log_detail)
    return redirect(request.referrer or url_for('main.index'))


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
    args_list, error_msg = build_args_from_form(form_data)

    if error_msg:
        flash(f'Invalid form data: {error_msg}', 'danger')
        return redirect(url_for('main.index'))
    if not args_list:
         flash('Failed to build command arguments.', 'danger')
         return redirect(url_for('main.index'))

    if not scheduler or not scheduler.running:
        flash("Scheduler not running. Cannot schedule job.", "danger")
        return redirect(url_for('main.index'))

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

        from apscheduler.triggers.date import DateTrigger
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger
        
        # --- THIS IS THE MODIFIED TIMEZONE LOGIC ---
        # Get the user's currently selected timezone object
        user_tz = get_user_timezone()

        if schedule_type == 'date' and schedule_time_str:
            naive_dt = datetime.datetime.fromisoformat(schedule_time_str)
            
            # Localize the naive datetime from the form using the user's selected timezone
            # This correctly interprets the time as if it were entered in that user's zone.
            user_local_dt = user_tz.localize(naive_dt, is_dst=None)
            
            # The scheduler itself is running in UTC, so we convert the user's local time to UTC.
            run_date_utc = user_local_dt.astimezone(pytz.utc)
            
            trigger = DateTrigger(run_date=run_date_utc) # Schedule with UTC-aware datetime
            
            flash_msg = f"for {user_local_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}"
            log_time_str = f"UserLocal: {user_local_dt.isoformat()}, ScheduledUTC: {run_date_utc.isoformat()}"

        elif schedule_type == 'cron' and cron_expression:
            # Cron jobs are now interpreted in the user's selected timezone
            trigger = CronTrigger.from_crontab(cron_expression, timezone=user_tz)
            flash_msg = f"with cron: '{cron_expression}' (in {user_tz.zone})"
            log_time_str = flash_msg
        # --- END MODIFIED TIMEZONE LOGIC ---

        elif schedule_type == 'interval' and interval_value:
             # Interval is not timezone-dependent, this logic is fine
             interval_val_int = int(interval_value)
             if interval_val_int < 1: raise ValueError("Interval must be >= 1.")
             kwargs = {interval_unit: interval_val_int}
             trigger = IntervalTrigger(**kwargs)
             flash_msg = f"every {interval_val_int} {interval_unit}"
             log_time_str = flash_msg
        else:
             flash('Invalid schedule details provided.', 'danger')
             return redirect(url_for('main.index'))

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
        user_tz = get_user_timezone()
        naive_dt = datetime.datetime.fromisoformat(start_time_str)
        first_run_local = user_tz.localize(naive_dt, is_dst=None)
        first_run_utc = first_run_local.astimezone(pytz.utc)
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