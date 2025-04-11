# dashboard/app.py

import os
import sys
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, make_response
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
import subprocess
import threading
import shlex
from dotenv import load_dotenv
import datetime
import pytz # For timezone support
from bson import ObjectId
import json
import logging
from urllib.parse import quote_plus # Ensure quote_plus is imported
import re
import math

from flask.json.provider import DefaultJSONProvider

# --- APScheduler Setup ---
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# --- Configuration ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
load_dotenv(os.path.join(project_root, '.env'))

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("FLASK_SECRET_KEY", "change_this_in_production") # CHANGE THIS!
app.logger.setLevel(logging.INFO) # Set Flask logger level

# --- Custom JSON Provider to handle BSON types ---
class BsonJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj) # Convert ObjectId to string
        if isinstance(obj, datetime.datetime):
            # Ensure datetime is handled correctly (e.g., ISO format)
            # Your format_datetime helper should handle this, but as a fallback:
            return obj.isoformat()
        # Let the default method handle others
        return super().default(obj)

# Assign the custom provider to the app
app.json = BsonJSONProvider(app)
# --- End Custom JSON Provider Setup ---

# --- MongoDB Connection ---
MONGO_USER = os.getenv("MONGO_USER", "labbuild_user")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF")
MONGO_HOST = os.getenv("MONGO_HOST")
DB_NAME = "labbuild_db"
LOG_COLLECTION = "operation_logs" # Consolidated operation logs
STD_LOG_COLLECTION = "logs"       # Standard debug/info logs
SCHEDULE_COLLECTION = "scheduled_jobs"
# --- Add constants for config collections ---
COURSE_CONFIG_COLLECTION = "courseconfig"
HOST_COLLECTION = "host"

if not MONGO_HOST:
    app.logger.critical("MONGO_HOST environment variable not set. Exiting.")
    sys.exit(1)

MONGO_URI = f"mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:27017/{DB_NAME}"

try:
    # Main client for app routes
    mongo_client_app = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client_app.admin.command('ping')
    db = mongo_client_app[DB_NAME]
    op_logs_collection = db[LOG_COLLECTION]
    std_logs_collection = db[STD_LOG_COLLECTION]
    course_config_collection = db[COURSE_CONFIG_COLLECTION]
    host_collection = db[HOST_COLLECTION]
    app.logger.info("Successfully connected App MongoDB client.")

    # Separate client specifically for the scheduler job store
    mongo_client_scheduler = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client_scheduler.admin.command('ping')
    app.logger.info("Successfully connected Scheduler MongoDB client.")

except ConnectionFailure as e:
    app.logger.critical(f"MongoDB connection failed: {e}")
    sys.exit(1)

# --- Scheduler Initialization ---
jobstores = {
    # Pass the dedicated client instance
    'default': MongoDBJobStore(database=DB_NAME, collection=SCHEDULE_COLLECTION, client=mongo_client_scheduler)
}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone=pytz.utc)
try:
    scheduler.start()
    app.logger.info("Scheduler started successfully.")
except Exception as e:
     app.logger.error(f"Error starting scheduler: {e}")

# --- Core Task Execution Function ---
def run_labbuild_task(args_list):
    """Runs labbuild.py as a subprocess with the given arguments."""
    labbuild_script_path = os.path.join(project_root, 'labbuild.py')
    command = [sys.executable, labbuild_script_path] + args_list
    app.logger.info(f"Executing background task: {' '.join(shlex.quote(arg) for arg in command)}")
    try:
        process = subprocess.run(command, capture_output=True, text=True, check=False, cwd=project_root, timeout=7200) # Add timeout (e.g., 2 hours)
        app.logger.info(f"labbuild task finished command '{' '.join(args_list)}'. RC: {process.returncode}")
        if process.returncode != 0:
            app.logger.error(f"labbuild task stderr:\n{process.stderr}")
        # Optionally log stdout too for debugging
        # if process.stdout:
        #      app.logger.debug(f"labbuild task stdout:\n{process.stdout}")
    except subprocess.TimeoutExpired:
        app.logger.error(f"labbuild task timed out: {' '.join(args_list)}")
        # How to update the corresponding run log? Difficult without run_id here.
        # Consider passing run_id to this function and updating the log on timeout.
    except FileNotFoundError:
         app.logger.error(f"Error: '{labbuild_script_path}' not found.")
    except Exception as e:
        app.logger.error(f"Failed run labbuild subprocess command '{' '.join(args_list)}': {e}", exc_info=True)

# --- Helper Functions ---
def build_args_from_form(form_data):
    """Converts form dictionary to a list of CLI arguments for labbuild.py,
       ensuring -v vendor comes before the command."""
    try:
        command = form_data.get('command')
        vendor = form_data.get('vendor')

        if not command: app.logger.error("Command missing in form data."); return None
        if not vendor: app.logger.error("Vendor missing in form data."); return None

        args = ['-v', vendor, command] # Correct order

        arg_map = { # Map fields to flags
            'course': '-g', 'host': '--host', 'start_pod': '-s', 'end_pod': '-e',
            'class_number': '-cn', 'tag': '-t', 'component': '-c', 'operation': '-o',
            'memory': '-mem', 'prtg_server': '--prtg-server'
        }
        for key, flag in arg_map.items():
            value = form_data.get(key)
            if value: args.extend([flag, value]) # Add if value exists and is not empty

        bool_flags = { # Map boolean flags
             're_build': '--re-build', 'full': '--full', 'monitor_only': '--monitor-only',
             'db_only': '--db-only', 'perm': '--perm', 'verbose': '--verbose'
        }
        for key, flag in bool_flags.items():
            if form_data.get(key): args.append(flag) # Add if checkbox is checked

        # --- Refined Validation ---
        if command in ['setup', 'manage', 'teardown'] \
           and not form_data.get('db_only') \
           and not form_data.get('monitor_only'):
            # Check if it's an F5 class-only operation (no pods needed)
            is_f5_class_only = (
                form_data.get('vendor', '').lower() == 'f5' and
                not form_data.get('start_pod') and # Pod fields are empty or missing
                not form_data.get('end_pod')
            )
            # Check if it's AEP Common build (often no pods needed)
            # This requires more context - assuming a specific course name pattern for now
            is_aep_common_build = (
                command == 'setup' and
                form_data.get('vendor', '').lower() == 'av' and
                'aep' in form_data.get('course', '').lower() and
                'Student' not in (form_data.get('component') or '') # Simplistic check
            )


            if not is_f5_class_only and not is_aep_common_build:
                # Check if start_pod is missing or empty
                start_pod_val = form_data.get('start_pod')
                if start_pod_val is None or start_pod_val == '': # Check explicitly for None or ""
                    app.logger.warning("Start pod missing or empty for standard operation.")
                    # Depending on strictness, you could `flash` an error and `return None` here

                # Check if end_pod is missing or empty
                end_pod_val = form_data.get('end_pod')
                if end_pod_val is None or end_pod_val == '': # Check explicitly for None or ""
                    app.logger.warning("End pod missing or empty for standard operation.")
                    # Optionally: flash error and return None

            # Host is generally always required for these operations
            host_val = form_data.get('host')
            if not host_val: # Check if missing or empty
                app.logger.warning("Host missing or empty for standard operation.")
                # Optionally: flash error and return None
        # --- End Refined Validation ---

        app.logger.debug(f"Built arguments: {args}")
        return args
    except Exception as e:
        app.logger.error(f"Error building args from form: {e}", exc_info=True)
        return None
    
# --- Helper to build filter query for all_logs ---
def build_log_filter_query(request_args):
    """Builds a MongoDB filter dictionary based on request arguments."""

    mongo_filter = {}

    # --- String contains filters (case-insensitive) ---
    # Uses $regex for partial matching
    string_filters = {
        'run_id': 'run_id',
        'command': 'command',
        'status': 'overall_status',
        # For nested args, use dot notation
        'vendor': 'args.vendor',
        'host': 'args.host'
    }
    for form_key, mongo_key in string_filters.items():
        filter_form_key = f"filter_{form_key}"
        value = request_args.get(filter_form_key, '').strip()
        if value: # Only add filter if value is not empty
            # Escape regex special characters in user input for safety
            escaped_value = re.escape(value)
            mongo_filter[mongo_key] = {'$regex': escaped_value, '$options': 'i'}
            

    # --- Date range filters for 'start_time' ---
    start_after_str = request_args.get('filter_start_after', '').strip()
    start_before_str = request_args.get('filter_start_before', '').strip()
    date_filter = {}
    try:
        if start_after_str:
            naive_dt_start = datetime.datetime.strptime(start_after_str, '%Y-%m-%d')
            # $gte: Greater than or equal to the start of the selected day (UTC)
            date_filter['$gte'] = pytz.utc.localize(datetime.datetime.combine(naive_dt_start.date(), datetime.time.min))
        if start_before_str:
            naive_dt_end = datetime.datetime.strptime(start_before_str, '%Y-%m-%d')
            # $lt: Less than the start of the *next* day (UTC) - makes the range inclusive of the end date
            date_filter['$lt'] = pytz.utc.localize(datetime.datetime.combine(naive_dt_end.date() + datetime.timedelta(days=1), datetime.time.min))

        if date_filter: # Add to main filter only if dates were valid
            mongo_filter['start_time'] = date_filter
            app.logger.debug(f"Applying date filter: {mongo_filter['start_time']}")

    except ValueError:
        if start_after_str or start_before_str: # Only flash if user actually entered something invalid
             flash("Invalid date format provided for filtering. Use YYYY-MM-DD.", "warning")
             app.logger.warning(f"Invalid date format received: start='{start_after_str}', end='{start_before_str}'")
        # Don't add the invalid date filter

    app.logger.debug(f"Constructed MongoDB filter: {mongo_filter}")
    return mongo_filter

def format_datetime(dt_utc):
    """Formats UTC datetime as an ISO 8601 string for JavaScript parsing."""
    if not dt_utc or not isinstance(dt_utc, datetime.datetime):
        return None
    if dt_utc.tzinfo is None: dt_utc = pytz.utc.localize(dt_utc)
    else: dt_utc = dt_utc.astimezone(pytz.utc)
    return dt_utc.isoformat(timespec='milliseconds') # Include milliseconds

# --- Flask Routes ---
@app.route('/')
def index():
    """Main dashboard page. Reads theme cookie."""
    # --- Read theme cookie ---
    current_theme = request.cookies.get('theme', 'light') # Default to light
    vendors = []
    hosts = []
    recent_runs = []
    jobs_display = []

    # --- Fetch Vendors ---
    try:
        # Get distinct, non-null, non-empty vendor codes, sort them
        distinct_vendors = course_config_collection.distinct("vendor_shortcode")
        vendors = sorted([v for v in distinct_vendors if v]) # Filter out None/empty
    except PyMongoError as e:
        app.logger.error(f"Error fetching vendors: {e}")
        flash("Error fetching vendor list from database.", "danger")

    # --- Fetch Hosts ---
    try:
        # Get distinct, non-null, non-empty host names, sort them
        distinct_hosts = host_collection.distinct("host_name")
        hosts = sorted([h for h in distinct_hosts if h]) # Filter out None/empty
    except PyMongoError as e:
        app.logger.error(f"Error fetching hosts: {e}")
        flash("Error fetching host list from database.", "danger")
    try: # Fetch runs
        recent_runs_cursor = op_logs_collection.find().sort('start_time', -1).limit(5)
        for run in recent_runs_cursor:
            run['start_time_iso'] = format_datetime(run.get('start_time'))
            run['end_time_iso'] = format_datetime(run.get('end_time'))
            recent_runs.append(run)
    except PyMongoError as e: app.logger.error(f"Error fetching op logs: {e}"); flash("Error fetching runs.", "danger")

    try: # Fetch jobs
        jobs = scheduler.get_jobs()
        jobs.sort(key=lambda j: j.next_run_time or datetime.datetime.max.replace(tzinfo=pytz.utc))
        for job in jobs:
            trigger_info = 'Unknown'; next_run_iso = format_datetime(job.next_run_time)
            if isinstance(job.trigger, DateTrigger): trigger_info = f"Once @ {job.next_run_time.isoformat() if job.next_run_time else '?'}"
            elif isinstance(job.trigger, CronTrigger): trigger_info = f"Cron: {job.trigger}"
            elif isinstance(job.trigger, IntervalTrigger): trigger_info = f"Every {job.trigger.interval}"
            jobs_display.append({'id': job.id, 'name': job.name, 'next_run_time_iso': next_run_iso, 'args': str(job.args), 'trigger_info': trigger_info})
    except Exception as e: app.logger.error(f"Error fetching jobs: {e}"); flash("Error fetching jobs.", "danger")

    # --- Pass data to template ---
    return render_template(
        'index.html',
        recent_runs=recent_runs,
        jobs=jobs_display,
        vendors=vendors, # Pass vendors list
        hosts=hosts,     # Pass hosts list
        current_theme=current_theme
    )

@app.route('/api/courses')
def api_courses():
    """Returns course name suggestions based on query and optional vendor."""
    query = request.args.get('q', '').strip()
    vendor = request.args.get('vendor', '').strip()

    suggestions = []
    if not query: # Don't search if query is empty
        return jsonify(suggestions)

    try:
        mongo_filter = {
            # Case-insensitive search starting with the query
            'course_name': {'$regex': f'^{re.escape(query)}', '$options': 'i'}
        }
        if vendor:
            # Add vendor filter if provided (case-insensitive)
            mongo_filter['vendor_shortcode'] = {'$regex': f'^{re.escape(vendor)}$', '$options': 'i'}

        # Query, limit results, project only course_name
        cursor = course_config_collection.find(
            mongo_filter,
            {'course_name': 1, '_id': 0}
        ).limit(15) # Limit suggestions

        suggestions = [doc['course_name'] for doc in cursor if 'course_name' in doc]

    except PyMongoError as e:
        app.logger.error(f"Error fetching course suggestions (q={query}, v={vendor}): {e}")
        # Return empty list on error, or could return 500 status
    except Exception as e:
         app.logger.error(f"Unexpected error fetching course suggestions (q={query}, v={vendor}): {e}", exc_info=True)

    return jsonify(suggestions)

@app.route('/run', methods=['POST'])
def run_now():
    """Handle immediate run request."""
    form_data = request.form.to_dict()
    args_list = build_args_from_form(form_data)
    if not args_list: flash('Invalid form data.', 'danger'); return redirect(url_for('index'))
    try:
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True); thread.start()
        flash(f"Submitted immediate run: {' '.join(args_list)}", 'info')
    except Exception as e: app.logger.error(f"Failed start thread for run_now: {e}"); flash("Error starting build.", 'danger')
    return redirect(url_for('index'))

@app.route('/schedule', methods=['POST'])
def schedule_run():
    """Handle schedule run request."""
    form_data = request.form.to_dict(); schedule_time_str = form_data.get('schedule_time')
    schedule_type = form_data.get('schedule_type', 'date'); cron_expression = form_data.get('cron_expression')
    interval_value = form_data.get('interval_value'); interval_unit = form_data.get('interval_unit', 'minutes')
    args_list = build_args_from_form(form_data)
    if not args_list: flash('Invalid form data.', 'danger'); return redirect(url_for('index'))

    try:
        job_name = f"{form_data.get('command')}_{form_data.get('vendor')}_{form_data.get('course')}"
        job = None
        if schedule_type == 'date' and schedule_time_str:
            naive_dt = datetime.datetime.fromisoformat(schedule_time_str)
            # !! Important: Assume user input is local time, need to know which local time zone
            # For simplicity, using a fixed one here. A better approach might involve JS sending UTC offset
            # or letting user select timezone.
            local_tz_str = os.getenv('SERVER_TIMEZONE', 'America/New_York') # Example: Get from env or default
            local_tz = pytz.timezone(local_tz_str)
            local_dt = local_tz.localize(naive_dt); run_date_utc = local_dt.astimezone(pytz.utc)
            job = scheduler.add_job(run_labbuild_task, trigger=DateTrigger(run_date=run_date_utc), args=[args_list], name=job_name, misfire_grace_time=3600)
            flash(f"Scheduled job '{job.id}' for {run_date_utc.isoformat()}", 'success')

        elif schedule_type == 'cron' and cron_expression:
            parts = cron_expression.split();
            if len(parts) != 5: raise ValueError("Invalid cron expression format.")
            job = scheduler.add_job(run_labbuild_task, trigger=CronTrigger.from_crontab(cron_expression, timezone=pytz.utc), args=[args_list], name=job_name, misfire_grace_time=3600)
            flash(f"Scheduled job '{job.id}' with cron: '{cron_expression}' UTC", 'success')

        elif schedule_type == 'interval' and interval_value:
             try: interval_val_int = int(interval_value); kwargs = {interval_unit: interval_val_int}; job = scheduler.add_job(run_labbuild_task, trigger=IntervalTrigger(**kwargs), args=[args_list], name=job_name, misfire_grace_time=3600); flash(f"Scheduled job '{job.id}' every {interval_val_int} {interval_unit}", 'success')
             except (ValueError, TypeError) as e: raise ValueError(f"Invalid interval: {e}")
        else: flash('Invalid schedule details.', 'danger'); return redirect(url_for('index'))
    except Exception as e: app.logger.error(f"Failed schedule job: {e}", exc_info=True); flash(f"Failed schedule job: {e}", 'danger')
    return redirect(url_for('index'))


@app.route('/status/<run_id>')
def get_run_status(run_id):
    """API endpoint to get the current status of a specific run."""
    try:
        log_data_consolidated = op_logs_collection.find_one({'run_id': run_id})
        if not log_data_consolidated: return jsonify({'status': 'error', 'message': 'Run ID not found'}), 404

        status_info = {
            'run_id': run_id,
            'overall_status': log_data_consolidated.get('overall_status', 'running'),
            'start_time_iso': format_datetime(log_data_consolidated.get('start_time')),
            'end_time_iso': format_datetime(log_data_consolidated.get('end_time')),
            'duration_seconds': log_data_consolidated.get('duration_seconds'),
            'success_count': log_data_consolidated.get('summary', {}).get('success_count', 0),
            'failure_count': log_data_consolidated.get('summary', {}).get('failure_count', 0),
            'pod_statuses_count': len(log_data_consolidated.get('pod_statuses', [])),
            'last_pod_status': None
        }
        pod_statuses = log_data_consolidated.get('pod_statuses', [])
        if pod_statuses:
            last_status = pod_statuses[-1]
            status_info['last_pod_status'] = {
                'identifier': last_status.get('identifier'), 'status': last_status.get('status'),
                'step': last_status.get('failed_step'), 'error': last_status.get('error_message'),
                'timestamp_iso': format_datetime(last_status.get('timestamp'))
            }
        return jsonify(status_info)
    except PyMongoError as e: app.logger.error(f"Error fetch status run {run_id}: {e}"); return jsonify({'status': 'error', 'message': 'DB error.'}), 500
    except Exception as e: app.logger.error(f"Unexpected error fetch status run {run_id}: {e}", exc_info=True); return jsonify({'status': 'error', 'message': 'Server error.'}), 500


@app.route('/logs/<run_id>')
def log_detail(run_id):
    """Display details and standard logs. Reads theme cookie."""
    # --- Read theme cookie ---
    current_theme = request.cookies.get('theme', 'light') # Default to light

    operation_log_data = None; std_logs = []
    try: # Fetch Op Log
        operation_log_data = op_logs_collection.find_one({'run_id': run_id})
        if operation_log_data:
            operation_log_data['start_time_iso'] = format_datetime(operation_log_data.get('start_time'))
            operation_log_data['end_time_iso'] = format_datetime(operation_log_data.get('end_time'))
            if 'pod_statuses' in operation_log_data:
                for pod_log in operation_log_data['pod_statuses']:
                     pod_log['timestamp_iso'] = format_datetime(pod_log.get('timestamp'))
    except PyMongoError as e: app.logger.error(f"Error fetch op log {run_id}: {e}"); flash(f"DB error fetch op log {run_id}.", "danger")

    try: # Fetch Standard Logs
        std_logs_cursor = std_logs_collection.find({'run_id': run_id}).sort('timestamp', 1)
        for log in std_logs_cursor:
            log['timestamp_iso'] = format_datetime(log.get('timestamp'))
            std_logs.append(log)
        if not operation_log_data and not std_logs: flash(f"No logs for run_id {run_id}.", 'warning'); return redirect(url_for('index'))
    except PyMongoError as e: app.logger.error(f"Error fetch std logs {run_id}: {e}"); flash(f"DB error fetch std logs {run_id}.", "danger")

    # Pass current_theme to template
    return render_template('log_detail.html', log=operation_log_data, std_logs=std_logs, current_theme=current_theme)

@app.route('/jobs/delete/<job_id>', methods=['POST'])
def delete_job(job_id):
    """Delete a scheduled job."""
    try: scheduler.remove_job(job_id); flash(f"Job {job_id} deleted.", 'success')
    except Exception as e: app.logger.error(f"Failed delete job {job_id}: {e}", exc_info=True); flash(f"Error deleting job {job_id}: {e}", 'danger')
    return redirect(url_for('index'))

@app.route('/logs/all')
def all_logs():
    """Displays all operation logs with filtering and pagination."""
    current_theme = request.cookies.get('theme', 'light')
    page = request.args.get('page', 1, type=int)
    per_page = 50 # Number of logs per page

    # Get filter parameters from query string
    current_filters = {k: v for k, v in request.args.items() if k != 'page'} # Exclude page itself

    # Build the MongoDB query filter
    mongo_filter = build_log_filter_query(request.args)

    logs = []
    total_logs = 0
    try:
        # Get total count for pagination
        total_logs = op_logs_collection.count_documents(mongo_filter)

        # Fetch the paginated logs
        skip = (page - 1) * per_page
        logs_cursor = op_logs_collection.find(mongo_filter).sort('start_time', -1).skip(skip).limit(per_page)

        for log in logs_cursor:
            log['start_time_iso'] = format_datetime(log.get('start_time'))
            log['end_time_iso'] = format_datetime(log.get('end_time'))
            logs.append(log)

    except PyMongoError as e:
        app.logger.error(f"Error fetching all logs page {page}: {e}")
        flash("Error fetching logs from database.", "danger")

    total_pages = math.ceil(total_logs / per_page)

    # Generate args for pagination links, preserving filters
    pagination_args = current_filters.copy()

    return render_template(
        'all_logs.html',
        logs=logs,
        current_page=page,
        total_pages=total_pages,
        total_logs=total_logs,
        pagination_args=pagination_args, # Pass args for link generation
        current_filters=current_filters, # Pass filters to repopulate form
        current_theme=current_theme
    )

# --- Cleanup Scheduler on Exit ---
def shutdown_scheduler():
    """Shuts down the scheduler gracefully."""
    global mongo_client_scheduler # Access the global scheduler client
    if scheduler and scheduler.running:
        app.logger.info("Shutting down scheduler...")
        try:
            scheduler.shutdown(wait=True) # Wait for jobs
            app.logger.info("Scheduler shut down successfully.")
        except Exception as e:
            app.logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)
    # --- Close the scheduler's dedicated client AFTER scheduler shutdown ---
    if mongo_client_scheduler:
        app.logger.info("Closing scheduler MongoDB client...")
        mongo_client_scheduler.close()
        app.logger.info("Scheduler MongoDB client closed.")

import atexit
atexit.register(shutdown_scheduler)

# --- Main Guard ---
if __name__ == '__main__':
    # Consider using Gunicorn for production instead of Flask's dev server
    # Example: gunicorn --bind 0.0.0.0:5001 "app:app" --log-level info
    app.run(debug=True, host='0.0.0.0', port=5001) # Use debug=False in production