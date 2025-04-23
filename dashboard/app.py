# --- START OF REFACTORED app.py ---

# dashboard/app.py

import os
import sys
import subprocess
import threading
import shlex
import datetime
import pytz  # For timezone support
import re
import math
import json
import logging
import time
import atexit
from collections import defaultdict
from urllib.parse import quote_plus
from typing import Optional, List
import io

# Third-party Imports
from flask import (
    Flask, render_template, request, redirect, url_for, flash, jsonify,
    make_response, Response, stream_with_context
)
from flask.json.provider import DefaultJSONProvider
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.errors import ConnectionFailure, PyMongoError
from bson import ObjectId, json_util
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import werkzeug.utils
import redis
from salesforce_utils import get_upcoming_courses_data, get_current_courses_data


# --- Configuration ---
# Determine project root dynamically
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add project root to sys.path BEFORE importing local modules
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"--- DEBUG: Added '{project_root}' to sys.path ---")
else:
    print(f"--- DEBUG: '{project_root}' already in sys.path ---")

# --- Local Imports ---
try:
    # Import constants first
    from constants import (
        DB_NAME, OPERATION_LOG_COLLECTION, LOG_COLLECTION,
        COURSE_CONFIG_COLLECTION, HOST_COLLECTION, ALLOCATION_COLLECTION,
        COURSE_MAPPING_RULES_COLLECTION
    )
    # Then import utilities
    from db_utils import delete_from_database
    from config_utils import (
        fetch_and_prepare_course_config, extract_components, get_host_by_name
    )
    from vcenter_utils import get_vcenter_instance
except ImportError as e:
    print(f"--- CRITICAL: Failed import local modules from '{project_root}'. "
          f"Error: {e} ---", file=sys.stderr)
    print(f"--- CRITICAL: Check file existence/structure. "
          f"CWD: {os.getcwd()} ---", file=sys.stderr)
    sys.exit(1)

load_dotenv(os.path.join(project_root, '.env'))

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv(
    "FLASK_SECRET_KEY", "default-secret-key-please-change"
)
app.logger.setLevel(logging.INFO)
# --- Add Redis URL config for Flask app ---
print(f"--- DEBUG [Flask]: REDIS_URL from .env: {os.getenv('REDIS_URL')} ---") # Add this
app.config["REDIS_URL"] = os.getenv("REDIS_URL", "redis://localhost:6379/0")
if not app.config["REDIS_URL"]:
    app.logger.warning(
        "REDIS_URL not set, real-time log streaming will be disabled."
    )


# --- Custom JSON Provider for BSON types ---
class BsonJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return format_datetime(obj)  # Use consistent formatting
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)  # Fallback for other non-serializable types

app.json = BsonJSONProvider(app)


# --- MongoDB Connection ---
MONGO_USER = os.getenv("MONGO_USER", "labbuild_user")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF")
MONGO_HOST = os.getenv("MONGO_HOST")
SCHEDULE_COLLECTION = "scheduled_jobs"

if not MONGO_HOST:
    app.logger.critical("MONGO_HOST environment variable not set. Exiting.")
    sys.exit(1)

MONGO_URI = (
    f"mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASSWORD)}"
    f"@{MONGO_HOST}:27017/{DB_NAME}"
)

# Global DB variables
mongo_client_app = None
mongo_client_scheduler = None
db = None
op_logs_collection = None
std_logs_collection = None
course_config_collection = None
host_collection = None
alloc_collection = None
course_mapping_rules_collection = None

try:
    # App client
    mongo_client_app = MongoClient(
        MONGO_URI, serverSelectionTimeoutMS=5000, appname="LabBuildApp"
    )
    mongo_client_app.admin.command('ping')
    db = mongo_client_app[DB_NAME]
    op_logs_collection = db[OPERATION_LOG_COLLECTION]
    std_logs_collection = db[LOG_COLLECTION]
    course_config_collection = db[COURSE_CONFIG_COLLECTION]
    host_collection = db[HOST_COLLECTION]
    alloc_collection = db[ALLOCATION_COLLECTION]
    course_mapping_rules_collection = db[COURSE_MAPPING_RULES_COLLECTION]
    app.logger.info("Successfully connected App MongoDB client.")

    # Scheduler client
    mongo_client_scheduler = MongoClient(
        MONGO_URI, serverSelectionTimeoutMS=5000, appname="LabBuildScheduler"
    )
    mongo_client_scheduler.admin.command('ping')
    app.logger.info("Successfully connected Scheduler MongoDB client.")

except ConnectionFailure as e:
    app.logger.critical(f"MongoDB connection failed: {e}")
    mongo_client_app = None
    mongo_client_scheduler = None
    sys.exit(1)
except Exception as e:
    app.logger.critical(
        f"Unexpected error during MongoDB initialization: {e}", exc_info=True
    )
    mongo_client_app = None
    mongo_client_scheduler = None
    sys.exit(1)

SERVER_TIMEZONE_STR = os.getenv('SERVER_TIMEZONE', 'Australia/Sydney') # Default to server's actual TZ
SERVER_TIMEZONE = pytz.timezone(SERVER_TIMEZONE_STR)
app.logger.info(f"--- APScheduler configured to use timezone: {SERVER_TIMEZONE_STR} ---")

# --- Scheduler Initialization ---
jobstores = {
    'default': MongoDBJobStore(
        database=DB_NAME,
        collection=SCHEDULE_COLLECTION,
        client=mongo_client_scheduler
    )
}
scheduler = BackgroundScheduler(jobstores=jobstores, timezone=SERVER_TIMEZONE)
try:
    if mongo_client_scheduler:
        scheduler.start()
        app.logger.info("Scheduler started successfully.")
    else:
        app.logger.error(
            "Cannot start scheduler: Scheduler MongoDB client connection failed."
        )
except Exception as e:
     app.logger.error(f"Error starting scheduler: {e}")


# --- Core Task Execution Function ---
def run_labbuild_task(args_list):
    """Runs labbuild.py as a subprocess with the given arguments."""
    labbuild_script_path = os.path.join(project_root, 'labbuild.py')
    python_executable = sys.executable
    command = [python_executable, labbuild_script_path] + args_list
    command_str = ' '.join(shlex.quote(arg) for arg in command)
    app.logger.info(f"Executing background task: {command_str}")
    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            cwd=project_root,
            timeout=7200  # 2 hour timeout
        )
        app.logger.info(
            f"labbuild task '{' '.join(args_list)}' finished. "
            f"RC: {process.returncode}"
        )
        if process.stdout:
             app.logger.debug(f"labbuild task stdout:\n{process.stdout}")
        if process.returncode != 0:
            app.logger.error(f"labbuild task stderr:\n{process.stderr}")

    except subprocess.TimeoutExpired:
        app.logger.error(
            f"labbuild task timed out after 7200s: {command_str}"
        )
    except FileNotFoundError:
         app.logger.error(
             f"Error: Python executable '{python_executable}' or script "
             f"'{labbuild_script_path}' not found."
         )
    except Exception as e:
        app.logger.error(
            f"Failed run labbuild subprocess command '{command_str}': {e}",
            exc_info=True
        )


# --- Helper Functions ---
def build_args_from_form(form_data):
    """Converts form dictionary to a list of CLI arguments for labbuild.py
       in the format: <command> [options...]."""
    try:
        command = form_data.get('command')
        if not command:
            app.logger.error("Command missing in form data.")
            return None

        args = [command]  # Command first

        # Map standard arguments to flags
        arg_map = {
            'vendor': '-v', 'course': '-g', 'host': '--host',
            'start_pod': '-s', 'end_pod': '-e', 'class_number': '-cn',
            'tag': '-t', 'component': '-c', 'operation': '-o',
            'memory': '-mem', 'prtg_server': '--prtg-server',
            'datastore': '-ds', 'thread': '-th'
        }
        for key, flag in arg_map.items():
            value = form_data.get(key)
            if value is not None and value != '':
                args.extend([flag, str(value)])

        # Map boolean flags (checkboxes)
        bool_flags = {
             're_build': '--re-build', 'full': '--full',
             'monitor_only': '--monitor-only', 'db_only': '--db-only',
             'perm': '--perm', 'verbose': '--verbose'
        }
        for key, flag in bool_flags.items():
            if form_data.get(key) == 'on':
                args.append(flag)

        # --- Basic Validation ---
        vendor_val = form_data.get('vendor')
        if not vendor_val:
            flash("Vendor (-v) is required.", "warning")
            return None

        if command == 'manage' and not form_data.get('operation'):
             flash("Manage operation (-o) is required for 'manage' command.",
                   "warning")
             return None

        # Core args validation
        is_f5 = vendor_val.lower() == 'f5'
        is_special_mode = (
            form_data.get('db_only') == 'on' or
            form_data.get('monitor_only') == 'on' or
            form_data.get('perm') == 'on' or
            form_data.get('component') == '?' or
            form_data.get('course') == '?'
        )
        if command in ['setup', 'manage', 'teardown'] and not is_special_mode:
             if not form_data.get('host'):
                 flash("Host (--host) is required.", "warning")
                 return None
             if is_f5 and not form_data.get('class_number'):
                 flash("F5 Class Number (-cn) required for F5.", "warning")
                 return None
             if not is_f5 and (not form_data.get('start_pod') or
                               not form_data.get('end_pod')):
                 flash("Start Pod (-s) and End Pod (-e) required for non-F5.",
                       "warning")
                 return None
             # Pod range sanity check
             start_p = form_data.get('start_pod')
             end_p = form_data.get('end_pod')
             if start_p and end_p:
                  try:
                      if int(start_p) > int(end_p):
                           flash("Start Pod cannot be greater than End Pod.",
                                 "warning")
                           return None
                  except ValueError:
                       flash("Invalid number for Start/End Pod.", "warning")
                       return None

        app.logger.debug(f"Built arguments: {args}")
        return args
    except Exception as e:
        app.logger.error(f"Error building args from form: {e}", exc_info=True)
        flash("Internal error building command arguments.", "danger")
        return None

def build_args_from_dict(data: dict) -> Optional[List[str]]:
    """
    Converts a dictionary (from API JSON) to a list of CLI arguments
    for labbuild.py. Returns None if validation fails.
    Performs basic validation.
    """
    command = data.get('command')
    if not command:
        app.logger.error("API Error: 'command' field is missing.")
        return None, "'command' field is required."

    args = [command] # Command first

    # --- Map standard arguments to flags ---
    # Use .get() with default None to handle missing optional fields
    arg_map = {
        'vendor': '-v', 'course': '-g', 'host': '--host',
        'start_pod': '-s', 'end_pod': '-e', 'class_number': '-cn',
        'tag': '-t', 'component': '-c', 'operation': '-o',
        'memory': '-mem', 'prtg_server': '--prtg-server',
        'datastore': '-ds', 'thread': '-th'
    }
    for key, flag in arg_map.items():
        value = data.get(key)
        # Add arg only if value is provided (not None, maybe allow empty strings?)
        if value is not None: # Check for None explicitly, allow 0 or False
             # Special handling for tag default? API should probably provide tag if desired.
             if key == 'tag' and value == '': continue # Skip empty tag? Or use default? Let's skip.
             args.extend([flag, str(value)])

    # --- Map boolean flags ---
    # Expect boolean True/False in JSON request
    bool_flags = {
         'rebuild': '--re-build', # Note the key name change from form ('re_build')
         'full': '--full',
         'monitor_only': '--monitor-only',
         'db_only': '--db-only',
         'perm': '--perm',
         'verbose': '--verbose'
    }
    for key, flag in bool_flags.items():
        if data.get(key) is True: # Check for explicit True
            args.append(flag)

    # --- Basic Server-Side Validation ---
    vendor_val = data.get('vendor')
    if not vendor_val:
        return None, "'vendor' field is required."

    # Command-specific required fields
    if command in ['setup', 'manage', 'teardown']:
        if not data.get('course'):
             return None, "'course' field is required for setup/manage/teardown."
        if command == 'manage' and not data.get('operation'):
             return None, "'operation' field is required for manage command."

        # Core args validation based on mode and vendor (similar to form validation)
        is_f5 = vendor_val.lower() == 'f5'
        is_special_mode = (
            data.get('db_only') is True or
            data.get('monitor_only') is True or
            data.get('perm') is True or
            data.get('component') == '?' or
            data.get('course') == '?'
        )

        if not is_special_mode:
             if not data.get('host'):
                 return None, "'host' field is required for standard operations."
             if is_f5 and data.get('class_number') is None: # Check for None explicitly
                 return None, "'class_number' field is required for F5 operations."
             if not is_f5 and (data.get('start_pod') is None or data.get('end_pod') is None):
                 return None, "'start_pod' and 'end_pod' fields are required for non-F5 operations."
             # Pod range sanity check
             start_p = data.get('start_pod')
             end_p = data.get('end_pod')
             if start_p is not None and end_p is not None:
                  try:
                      if int(start_p) > int(end_p):
                           return None, "start_pod cannot be greater than end_pod."
                  except (ValueError, TypeError):
                       return None, "Invalid non-integer value for start_pod or end_pod."

    app.logger.debug(f"Built arguments from API data: {args}")
    return args, None # Return args and no error message

def build_log_filter_query(request_args):
    """Builds a MongoDB filter dictionary based on request arguments."""
    mongo_filter = {}
    string_filters = {
        'run_id': 'run_id', 'command': 'command', 'status': 'overall_status',
        'vendor': 'args.vendor', 'host': 'args.host', 'course': 'args.course'
    }
    for form_key, mongo_key in string_filters.items():
        filter_form_key = f"filter_{form_key}"
        value = request_args.get(filter_form_key, '').strip()
        if value:
            escaped_value = re.escape(value)
            mongo_filter[mongo_key] = {'$regex': escaped_value, '$options': 'i'}

    # Date range filters
    start_after_str = request_args.get('filter_start_after', '').strip()
    start_before_str = request_args.get('filter_start_before', '').strip()
    date_filter = {}
    try:
        if start_after_str:
            dt_start = datetime.datetime.strptime(start_after_str, '%Y-%m-%d')
            date_filter['$gte'] = pytz.utc.localize(dt_start)
        if start_before_str:
            dt_end = datetime.datetime.strptime(start_before_str, '%Y-%m-%d')
            date_filter['$lt'] = pytz.utc.localize(
                dt_end + datetime.timedelta(days=1)
            )
        if date_filter:
            mongo_filter['start_time'] = date_filter
            app.logger.debug(f"Applying date filter: {date_filter}")
    except ValueError:
        if start_after_str or start_before_str:
             flash("Invalid date format. Use YYYY-MM-DD.", "warning")
             app.logger.warning(
                 f"Invalid date filter format: start='{start_after_str}', "
                 f"end='{start_before_str}'"
             )
    app.logger.debug(f"Constructed MongoDB filter: {mongo_filter}")
    return mongo_filter


def format_datetime(dt_utc):
    """Formats UTC datetime as an ISO 8601 string for JavaScript."""
    if not dt_utc or not isinstance(dt_utc, datetime.datetime):
        return None
    if dt_utc.tzinfo is None:
        dt_utc = pytz.utc.localize(dt_utc)
    else:
        dt_utc = dt_utc.astimezone(pytz.utc)
    return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def parse_command_line(line: str) -> Optional[List[str]]:
    """
    Parses a full command line string (including script path) into
    a list of arguments suitable for labbuild.py.
    Returns None if parsing fails or line is invalid.
    """
    line = line.strip()
    if not line or line.startswith('#'): # Ignore empty lines and comments
        return None

    try:
        # Split using shlex for handling quotes etc.
        parts = shlex.split(line, posix=(os.name != 'nt')) # Use POSIX mode for Unix-like paths

        # Find the index of 'labbuild.py'
        script_name = 'labbuild.py'
        script_index = -1
        for i, part in enumerate(parts):
            if part.endswith(script_name):
                script_index = i
                break

        if script_index == -1:
            app.logger.warning(f"Could not find '{script_name}' in command line: {line}")
            return None

        # Extract arguments *after* labbuild.py
        args = parts[script_index + 1:]

        # Basic validation: Should have at least a command (e.g., 'setup')
        if not args:
             app.logger.warning(f"No arguments found after '{script_name}' in line: {line}")
             return None

        # --- Argument Order Correction (Heuristic) ---
        # The user provided file might have incorrect order like "--vendor pa setup"
        # labbuild.py's argparse *should* handle this if args are otherwise correct.
        # However, if we need to enforce "command first", we can add logic here.
        # Let's assume argparse handles it for now based on the original requirement.
        # Example correction (if needed):
        potential_commands = ['setup', 'teardown', 'manage', '-l', '--list-allocations']
        command_found = None
        command_index = -1
        for i, arg in enumerate(args):
            if arg in potential_commands:
                command_found = arg
                command_index = i
                break
        if command_found and command_index > 0:
            # Move command to the beginning
            args.pop(command_index)
            args.insert(0, command_found)
        elif not command_found and args and args[0] not in potential_commands:
             # If no command is found, and first arg isn't a command, flag as error?
             app.logger.warning(f"Could not identify command in arguments: {args}")
             return None # Or try to proceed assuming first arg is command

        app.logger.debug(f"Parsed arguments from line '{line}': {args}")
        return args

    except Exception as e:
        app.logger.error(f"Error parsing command line '{line}': {e}", exc_info=True)
        return None

def format_job_args(job_args: tuple) -> str:
    """Formats the job arguments tuple into a readable command string."""
    if not job_args or not isinstance(job_args, tuple) or not job_args[0]:
        return "[No Args]"
    try:
        # Expecting args to be like ([arg1, arg2, ...],)
        args_list = job_args[0]
        if isinstance(args_list, list):
            # Quote arguments containing spaces for better readability
            return ' '.join(shlex.quote(str(arg)) for arg in args_list)
        else:
            # Fallback if format is unexpected
            return str(job_args)
    except Exception as e:
        app.logger.error(f"Error formatting job args '{job_args}': {e}")
        return "[Error Formatting Args]"
    

@app.route('/api/v1/labbuild', methods=['POST'])
def api_run_labbuild():
    """
    API endpoint to trigger labbuild commands asynchronously.
    Accepts JSON POST requests.
    Requires Authentication (TODO: Implement proper auth).
    """
    # --- TODO: Implement Authentication/Authorization ---
    # Example: Check for an API key in headers
    # api_key = request.headers.get('X-API-Key')
    # if not api_key or not validate_api_key(api_key): # Implement validate_api_key
    #     return jsonify({"error": "Unauthorized"}), 401
    # ----------------------------------------------------

    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 415 # Unsupported Media Type

    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data received"}), 400

    app.logger.info(f"API request received: {data}")

    # Build and validate arguments
    args_list, error_msg = build_args_from_dict(data)

    if error_msg:
        app.logger.error(f"API validation failed: {error_msg}")
        return jsonify({"error": f"Invalid input: {error_msg}"}), 400
    if not args_list: # Should be caught by error_msg check, but belt-and-suspenders
         app.logger.error("API argument building failed silently.")
         return jsonify({"error": "Failed to process arguments."}), 500


    # Execute the command asynchronously
    try:
        thread = threading.Thread(
            target=run_labbuild_task, args=(args_list,), daemon=True
        )
        thread.start()
        app.logger.info(f"Submitted API task: {' '.join(args_list)}")
        # Return 202 Accepted - Task submitted but not completed yet
        return jsonify({
            "status": "submitted",
            "message": "LabBuild command submitted for asynchronous execution.",
            "submitted_command": args_list
            # TODO: Ideally return a run_id here for tracking later
        }), 202
    except Exception as e:
        app.logger.error(f"Failed to start background thread for API task: {e}", exc_info=True)
        return jsonify({"error": "Failed to start background task."}), 500

# --- Flask Routes ---
@app.route('/')
def index():
    """Main dashboard page."""
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
            distinct_vendors = course_config_collection.distinct(
                "vendor_shortcode"
            )
            vendors = sorted([v for v in distinct_vendors if v])
        else: flash("Course config collection unavailable.", "warning")
    except PyMongoError as e:
        app.logger.error(f"Error fetching vendors: {e}")
        flash("Error fetching vendor list.", "danger")
    try:
        if host_collection is not None:
            distinct_hosts = host_collection.distinct("host_name")
            hosts = sorted([h for h in distinct_hosts if h])
        else: flash("Host collection unavailable.", "warning")
    except PyMongoError as e:
        app.logger.error(f"Error fetching hosts: {e}")
        flash("Error fetching host list.", "danger")

    # Fetch Recent Runs
    try:
        if op_logs_collection is not None:
            cursor = op_logs_collection.find().sort(
                'start_time', DESCENDING
            ).limit(5)
            docs = list(cursor)
            for run in docs:
                run['_id'] = str(run['_id'])
                run['start_time_iso'] = format_datetime(run.get('start_time'))
                run['end_time_iso'] = format_datetime(run.get('end_time'))
                run['args_display'] = {
                    k: str(v) for k, v in run.get('args', {}).items()
                }
                recent_runs.append(run)
        else: flash("Operation logs collection unavailable.", "warning")
    except PyMongoError as e:
        app.logger.error(f"Error fetching operation logs: {e}")
        flash("Error fetching recent runs.", "danger")
    except Exception as e:
        app.logger.error(f"Error processing recent runs: {e}", exc_info=True)
        flash("Server error processing recent runs.", "danger")

    # Fetch Scheduled Jobs
    try:
        if scheduler.running:
            jobs = scheduler.get_jobs()
            jobs.sort(key=lambda j: j.next_run_time or
                      datetime.datetime.max.replace(tzinfo=pytz.utc))
            for job in jobs:
                trigger_info = 'Unknown'
                next_run = format_datetime(job.next_run_time)
                if isinstance(job.trigger, DateTrigger):
                    trigger_info = f"Once @ {format_datetime(job.trigger.run_date)}"
                elif isinstance(job.trigger, CronTrigger):
                    trigger_info = f"Cron: {job.trigger}"
                elif isinstance(job.trigger, IntervalTrigger):
                    trigger_info = f"Every {job.trigger.interval}"
                
                args_display_str = format_job_args(job.args)

                jobs_display.append({
                    'id': job.id, 'name': job.name,
                    'next_run_time_iso': next_run, 'args_display_str': args_display_str,
                    'trigger_info': trigger_info
                })
        else: flash("Scheduler is not running.", "warning")
    except Exception as e:
        app.logger.error(f"Error fetching jobs: {e}", exc_info=True)
        flash("Error fetching scheduled jobs.", "danger")

    return render_template(
        'index.html', recent_runs=recent_runs, jobs=jobs_display,
        vendors=vendors, hosts=hosts, current_theme=current_theme
    )


@app.route('/api/courses')
def api_courses():
    """Returns course name suggestions."""
    query = request.args.get('q', '').strip()
    vendor = request.args.get('vendor', '').strip()
    suggestions = []
    if not query or db is None or course_config_collection is None:
        return jsonify(suggestions)
    try:
        mongo_filter = {
            'course_name': {'$regex': f'^{re.escape(query)}', '$options': 'i'}
        }
        if vendor:
            mongo_filter['vendor_shortcode'] = {
                '$regex': f'^{re.escape(vendor)}$', '$options': 'i'
            }
        cursor = course_config_collection.find(
            mongo_filter, {'course_name': 1, '_id': 0}
        ).limit(15)
        suggestions = [doc['course_name'] for doc in cursor if 'course_name' in doc]
    except PyMongoError as e:
        app.logger.error(f"Error fetching course suggestions: {e}")
    except Exception as e:
        app.logger.error(f"Unexpected error fetching courses: {e}", exc_info=True)
    return jsonify(suggestions)


@app.route('/allocations')
def view_allocations():
    """
    Fetches allocation data, processes it into a flat list with group headers
    suitable for server-side pagination and grouping display. Includes power status.

    Handles filtering based on request arguments:
    - filter_tag (regex contains, case-insensitive)
    - filter_vendor (regex exact match, case-insensitive)
    - filter_course (regex contains, case-insensitive)
    - filter_host (substring contains, case-insensitive, post-processing)
    - filter_number (exact match integer, post-processing)

    Returns:
        Flask Response: Renders the 'allocations.html' template with:
            - allocation_list: The SLICED flat list for the current page.
            - total_data_items: Count of actual pod/class data items matching filters.
            - current_page: The current page number being displayed.
            - total_pages: The total number of pages calculated.
            - pagination_args: Dictionary of active filters for pagination links.
            - current_filters: Dictionary of active filters for form persistence.
            - current_theme: Current theme setting.
    """
    # Simulate request context if running standalone
    # Example: request = type('obj', (object,), {'args': {'filter_tag': 'test', 'page': '1'}, 'cookies': {'theme': 'light'}})()
    current_theme = request.cookies.get('theme', 'light')
    current_filters = {k: v for k, v in request.args.items() if k != 'page'} # Exclude page from filters
    try:
        page = int(request.args.get('page', 1))
        if page < 1: page = 1
    except ValueError:
        page = 1
    per_page = 50 # Rows per page (including headers) - adjust as needed

    mongo_filter = {}
    flat_display_list_all = [] # Store the full flattened list temporarily
    total_data_items = 0 # Count only actual pod/class data items

    # --- Build Mongo Filter ---
    if current_filters.get('filter_tag'):
        mongo_filter['tag'] = {'$regex': re.escape(current_filters["filter_tag"]), '$options': 'i'}
    if current_filters.get('filter_vendor'):
        mongo_filter['courses.vendor'] = {'$regex': f'^{re.escape(current_filters["filter_vendor"])}$', '$options': 'i'}
    if current_filters.get('filter_course'):
        mongo_filter['courses.course_name'] = {'$regex': re.escape(current_filters['filter_course']), '$options': 'i'}
    # Host and Number filters are applied after fetching

    try:
        # --- Database Availability Check ---
        # Replace 'db' and 'alloc_collection' with actual access method
        global db, alloc_collection # Example if using globals
        if db is None or alloc_collection is None:
            flash("DB connection or allocation collection unavailable.", "danger") # Replace flash if needed
            app.logger.error("DB connection or allocation collection is None.")
            raise ConnectionError("Database not available") # Raise error to be caught below

        # --- Fetch and Pre-Group Data (Temporary Grouping) ---
        temp_grouped_allocations = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        alloc_cursor = alloc_collection.find(mongo_filter).sort("tag", 1)

        for tag_doc in alloc_cursor:
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
                    # Fetch Power Status - Treat 'True' string/boolean as True, others as False
                    power_on_status = str(pod_detail.get("poweron", False)).lower() == 'true'


                    # Post-processing Host Filter
                    filter_host_val = current_filters.get('filter_host', '').strip().lower()
                    if filter_host_val and filter_host_val not in host.lower():
                        continue

                    # Post-processing Number Filter
                    num_filter_str = current_filters.get('filter_number', '').strip()
                    num_filter_int = None
                    is_num_filter_active = False
                    if num_filter_str:
                        try:
                            num_filter_int = int(num_filter_str)
                            is_num_filter_active = True
                        except ValueError:
                            is_num_filter_active = False # Ignore invalid number filter

                    # Prepare item data dictionary
                    item_data = {
                        "pod_number": pod_num, "class_number": class_num,
                        "prtg_url": prtg_url, "vendor": vendor,
                        "course_name": course_name, "tag": tag, "host": host,
                        "type": "unknown", "number": None,
                        "poweron": power_on_status # Store boolean power status
                    }

                    # F5 Class Logic
                    if is_f5 and class_num is not None:
                        item_data["type"] = "f5_class"; item_data["number"] = class_num
                        nested_pods_filtered = []
                        nested_power_states = {} # Store power states of nested pods
                        for np in pod_detail.get("pods", []):
                             if isinstance(np, dict):
                                  np_num = np.get("pod_number")
                                  np_power = str(np.get("poweron", False)).lower() == 'true'
                                  if np_num is not None:
                                      nested_power_states[np_num] = np_power
                                      if not is_num_filter_active or np_num == num_filter_int:
                                          nested_pods_filtered.append(np_num)

                        class_matches_num = (not is_num_filter_active) or (class_num == num_filter_int)

                        if class_matches_num or nested_pods_filtered:
                             item_data["nested_pods"] = sorted(list(set(nested_pods_filtered)))
                             item_data["nested_power_states"] = nested_power_states # Include nested states
                             # Check for duplicates before adding to temp grouping
                             group_list = temp_grouped_allocations[tag][course_name][host]
                             if not any(d.get("type") == "f5_class" and d.get("number") == class_num for d in group_list):
                                  group_list.append(item_data)

                    # Pod Logic
                    elif pod_num is not None:
                         item_data["type"] = "pod"; item_data["number"] = pod_num
                         pod_matches_num = (not is_num_filter_active) or (pod_num == num_filter_int)
                         if pod_matches_num:
                             temp_grouped_allocations[tag][course_name][host].append(item_data)

        # --- Flatten the Grouped Data with Headers ---
        current_tag, current_course, current_host = None, None, None
        for tag, courses in sorted(temp_grouped_allocations.items()):
            if tag != current_tag:
                flat_display_list_all.append({"row_type": "tag_header", "tag": tag})
                current_tag, current_course, current_host = tag, None, None
            for course_name, hosts_data in sorted(courses.items()):
                if course_name != current_course:
                     flat_display_list_all.append({"row_type": "course_header", "course_name": course_name, "tag": tag})
                     current_course, current_host = course_name, None
                for host, items in sorted(hosts_data.items()):
                    if host != current_host:
                         flat_display_list_all.append({"row_type": "host_header", "host": host, "course_name": course_name, "tag": tag})
                         current_host = host
                    items.sort(key=lambda x: (x.get("type", ""), x.get("number", 0)))
                    for item in items:
                        item["row_type"] = "data"
                        flat_display_list_all.append(item)
                        total_data_items += 1

    # --- Exception Handling ---
    except (PyMongoError, ConnectionError) as db_e: # Catch DB specific errors
        app.logger.error(f"Database error fetching allocations: {db_e}")
        flash("Error fetching allocation data from database.", "danger")
        # Reset lists on error
        flat_display_list_all = []
        total_data_items = 0
    except Exception as e: # Catch other processing errors
         app.logger.error(f"Error processing allocation data: {e}", exc_info=True)
         flash("An error occurred while processing allocation data.", "danger")
         # Reset lists on error
         flat_display_list_all = []
         total_data_items = 0

    # --- Server-Side Pagination Logic ---
    total_rows = len(flat_display_list_all) # Total display rows (headers + data)
    total_pages = math.ceil(total_rows / per_page) if per_page > 0 and total_rows > 0 else 1
    # Validate current page number against calculated total pages
    page = max(1, min(page, total_pages))
    # Calculate slice indices
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    # Slice the list to get only rows for the current page
    paginated_list = flat_display_list_all[start_index:end_index]

    # --- Group-Aware Pagination Adjustment (on the sliced list) ---
    # If the first item on the page is NOT a tag header, we might need to prepend headers
    # This adds complexity back. Simpler approach: Ensure first item IS a tag header if possible,
    # or accept that a page might start mid-group with server-side pagination.
    # For now, we'll stick with the simple slice. Advanced group-aware server-side
    # pagination requires more complex index calculation or different data structures.

    # --- Render Template ---
    return render_template(
        'allocations.html',
        allocation_list=paginated_list,  # Pass ONLY the slice for the current page
        total_data_items=total_data_items, # Pass count of actual pod/class items
        current_page=page,               # Pass validated current page number
        total_pages=total_pages,         # Pass total number of pages
        pagination_args=current_filters, # Pass filters for pagination links
        current_filters=current_filters,   # Pass filters for form persistence
        current_theme=current_theme
    )


# --- NEW Route to Handle Power Toggle ---
@app.route('/toggle-power', methods=['POST'])
def toggle_power():
    """
    Triggers 'labbuild manage' start or stop based on the requested scope
    (tag or individual item) and the OPPOSITE of its current DB state.
    Attempts to update the DB state after submitting the command.
    """
    scope = request.form.get('scope')
    tag = request.form.get('tag')
    # current_state_is_on = request.form.get('current_state') == 'on' # No longer needed for tag scope

    # Data for individual items (still needed if scope=item)
    item_type = request.form.get('item_type')
    item_number_str = request.form.get('item_number')
    host = request.form.get('host')
    vendor = request.form.get('vendor')
    course = request.form.get('course')
    item_class_number_str = request.form.get('item_class_number') # For F5 pods context

    if not scope or not tag:
        flash("Missing scope or tag for power toggle.", "danger")
        query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
        return redirect(url_for('view_allocations', **query_params))

    app.logger.info(f"Power toggle request received: Scope='{scope}', Tag='{tag}'")

    tasks_to_run = [] # List of tuples: (args_list, description, db_update_info)
                      # db_update_info = (course_name, host, number, item_type, new_power_state_bool)

    try:
        if db is None or alloc_collection is None:
            raise ConnectionError("Database connection unavailable.")

        # --- Logic for Tag Scope ---
        if scope == 'tag':
            tag_doc = alloc_collection.find_one({"tag": tag})
            if not tag_doc: raise ValueError(f"Tag '{tag}' not found.")

            courses_in_tag = tag_doc.get("courses", [])
            if not isinstance(courses_in_tag, list): courses_in_tag = []

            for course_item in courses_in_tag:
                if not isinstance(course_item, dict): continue
                c_name = course_item.get("course_name")
                c_vendor = course_item.get("vendor")
                pod_details = course_item.get("pod_details", [])
                if not c_name or not c_vendor or not isinstance(pod_details, list): continue

                for pd in pod_details:
                    if not isinstance(pd, dict): continue
                    pd_host = pd.get("host", pd.get("pod_host"))
                    pd_pod = pd.get("pod_number")
                    pd_class = pd.get("class_number")
                    # Read current DB state, default to False (treat unknowns as off)
                    current_power_on = str(pd.get("poweron", False)).lower() == 'true'
                    action = 'stop' if current_power_on else 'start' # Desired action is opposite
                    new_power_state_bool = not current_power_on # Intended state after action

                    if not pd_host: continue # Skip if no host

                    is_f5 = c_vendor.lower() == 'f5'
                    item_detail = None # To store info for DB update

                    if is_f5 and pd_class is not None:
                        # Handle F5 Class
                        args = ['manage', '-v', c_vendor, '-g', c_name, '--host', pd_host, '-t', tag, '-o', action, '-cn', str(pd_class)]
                        desc = f"Tag '{tag}' - Manage {action} Class {pd_class} ({c_name} on {pd_host})"
                        item_detail = (c_name, pd_host, pd_class, "f5_class", new_power_state_bool)
                        tasks_to_run.append((args, desc, item_detail))
                        # TODO: Optionally iterate pd.get("pods", []) here if you want separate manage commands AND DB updates for nested F5 pods.

                    elif pd_pod is not None:
                        # Handle non-F5 Pod (or potentially flat F5 pod - needs schema check)
                        args = ['manage', '-v', c_vendor, '-g', c_name, '--host', pd_host, '-t', tag, '-o', action, '-s', str(pd_pod), '-e', str(pd_pod)]
                        # Add F5 class context if needed for manage command
                        if is_f5 and pd_class is not None:
                            args.extend(['-cn', str(pd_class)])
                        desc = f"Tag '{tag}' - Manage {action} Pod {pd_pod} ({c_name} on {pd_host})"
                        item_detail = (c_name, pd_host, pd_pod, "pod", new_power_state_bool)
                        tasks_to_run.append((args, desc, item_detail))

        # --- Logic for Item Scope ---
        elif scope == 'item':
            if not all([item_type, item_number_str, host, vendor, course]):
                raise ValueError("Missing item details for power toggle.")
            item_number = int(item_number_str)
            # Get current state passed from form (based on the icon clicked)
            current_state_is_on = request.form.get('current_state') == 'on'
            action = 'stop' if current_state_is_on else 'start'
            new_power_state_bool = not current_state_is_on

            args = ['manage', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-o', action]
            item_detail = None # Info for DB update

            if item_type == 'f5_class':
                args.extend(['-cn', str(item_number)])
                desc = f"Item - Manage {action} Class {item_number} ({course} on {host})"
                item_detail = (course, host, item_number, "f5_class", new_power_state_bool)
            elif item_type == 'pod':
                args.extend(['-s', str(item_number), '-e', str(item_number)])
                item_class_num = None
                if vendor.lower() == 'f5' and item_class_number_str:
                     item_class_num = int(item_class_number_str) # Parse if needed
                     args.extend(['-cn', item_class_number_str])
                desc = f"Item - Manage {action} Pod {item_number} ({course} on {host})"
                item_detail = (course, host, item_number, "pod", new_power_state_bool)
                # Add class_num to item_detail if F5 pod for accurate DB update query
                if item_class_num is not None:
                    item_detail = (course, host, item_number, "pod", new_power_state_bool, item_class_num)

            else:
                raise ValueError(f"Invalid item_type: {item_type}")

            tasks_to_run.append((args, desc, item_detail))

        else:
             raise ValueError(f"Invalid scope: {scope}")

        # --- Execute Tasks and Update DB Sequentially ---
        if not tasks_to_run:
             flash(f"No valid manage tasks found for scope '{scope}', Tag '{tag}'.", "warning")
        else:
            app.logger.info(f"Found {len(tasks_to_run)} manage tasks for Scope '{scope}', Tag '{tag}'. Submitting sequentially...")

            def run_sequential_manage_and_update(tasks):
                 total = len(tasks)
                 for i, (args, desc, db_info) in enumerate(tasks):
                     app.logger.info(f"Starting manage task {i+1}/{total}: {desc} (Args: {' '.join(args)})")
                     run_labbuild_task(args) # Execute the manage command
                     app.logger.info(f"Finished manage task {i+1}/{total}: {desc}")

                     # --- Attempt to Update DB Power State ---
                     if db_info:
                         try:
                             update_power_state_in_db(tag, *db_info) # Pass tag and unpacked db_info tuple
                         except Exception as db_update_err:
                              app.logger.error(f"Failed to update DB power state for {desc}: {db_update_err}")
                     # --- End DB Update ---

                 app.logger.info(f"All {total} manage tasks for Scope '{scope}', Tag '{tag}' submitted.")

            thread = threading.Thread(target=run_sequential_manage_and_update, args=(tasks_to_run,), daemon=True)
            thread.start()
            flash(f"Submitted {len(tasks_to_run)} power toggle tasks for Scope '{scope}', Tag '{tag}'. DB state will be updated optimistically.", "info")

    except ConnectionError as e:
        app.logger.error(f"DB Connection Error during power toggle for {tag}: {e}")
        flash("Database connection error.", "danger")
    except ValueError as e: # Catch validation errors
         app.logger.error(f"Value Error during power toggle for {tag}: {e}")
         flash(f"Error: {e}", "warning")
    except Exception as e:
        app.logger.error(f"Error processing power toggle for Scope '{scope}', Tag '{tag}': {e}", exc_info=True)
        flash(f"Error submitting power toggle for Scope '{scope}', Tag '{tag}': {e}", 'danger')

    # Redirect back to allocations page, preserving filters
    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('view_allocations', **query_params))


# --- NEW Helper function to update power state in DB ---
def update_power_state_in_db(tag, course_name, host, item_number, item_type, new_state_bool, f5_class_num_context=None):
    """
    Updates the 'poweron' field for a specific pod/class within the nested DB structure.
    """
    if alloc_collection is None:
        app.logger.error("DB update skipped: alloc_collection is None.")
        return

    power_state_str = str(new_state_bool) # Store as string 'True' or 'False'
    target_id = item_number

    # Construct the query to find the specific item
    query = {"tag": tag, "courses.course_name": course_name}
    update = None

    if item_type == 'f5_class':
        # Update the poweron status of the main F5 class entry
        query["courses.pod_details"] = {
            "$elemMatch": {"class_number": target_id, "host": host}
        }
        update = {
            "$set": {"courses.$[courseElem].pod_details.$[podElem].poweron": power_state_str}
        }
        array_filters = [
            {"courseElem.course_name": course_name},
            {"podElem.class_number": target_id, "podElem.host": host}
        ]
    elif item_type == 'pod':
        if f5_class_num_context is not None:
            # Update a nested pod within an F5 class entry
            query["courses.pod_details"] = {
                "$elemMatch": {"class_number": f5_class_num_context, "host": host}
            }
            update = {
                 # This uses positional operator $ to update the matched class_detail
                 # Then it needs another positional operator $[podItem] for the nested pod
                 # This might require MongoDB 4.2+ and careful array filter definition
                 # Simpler alternative might be to fetch, modify, and update the whole courses array (less efficient)
                 # Let's try the arrayFilters approach (requires MongoDB 3.6+)
                 "$set": { "courses.$[courseElem].pod_details.$[classElem].pods.$[podElem].poweron": power_state_str }
            }
            array_filters = [
                {"courseElem.course_name": course_name},
                {"classElem.class_number": f5_class_num_context, "classElem.host": host},
                {"podElem.pod_number": target_id} # Filter the nested pod by number
            ]
        else:
            # Update a non-F5 pod entry
            query["courses.pod_details"] = {
                "$elemMatch": {"pod_number": target_id, "host": host}
            }
            update = {
                "$set": {"courses.$[courseElem].pod_details.$[podElem].poweron": power_state_str}
            }
            array_filters = [
                 {"courseElem.course_name": course_name},
                 {"podElem.pod_number": target_id, "podElem.host": host}
            ]
    else:
        app.logger.error(f"DB Update Error: Unknown item_type '{item_type}' for tag '{tag}', item {target_id}")
        return

    if update:
        try:
            result = alloc_collection.update_one(
                query,
                update,
                array_filters=array_filters
             )
            if result.matched_count > 0:
                 if result.modified_count > 0:
                      app.logger.info(f"DB power state updated for Tag '{tag}', Course '{course_name}', Host '{host}', {item_type} {target_id} to {power_state_str}")
                 else:
                      app.logger.warning(f"DB power state potentially already '{power_state_str}' for Tag '{tag}', {item_type} {target_id}. No change made.")
            else:
                 app.logger.error(f"DB power state update FAILED: No matching document found for Tag '{tag}', Course '{course_name}', Host '{host}', {item_type} {target_id} (Class Context: {f5_class_num_context})")
        except PyMongoError as e:
             app.logger.error(f"PyMongoError updating DB power state for Tag '{tag}', {item_type} {target_id}: {e}")
        except Exception as e:
             app.logger.error(f"Unexpected error updating DB power state for Tag '{tag}', {item_type} {target_id}: {e}", exc_info=True)


@app.route('/teardown-item', methods=['POST'])
def teardown_item():
    """Triggers teardown OR direct DB deletion for various levels."""
    try:
        # Extract common identifiers
        tag = request.form.get('tag')
        host = request.form.get('host') # Needed for VM teardown
        vendor = request.form.get('vendor') # Needed for VM teardown
        course = request.form.get('course')
        pod_num_str = request.form.get('pod_number')
        class_num_str = request.form.get('class_number')
        delete_level = request.form.get('delete_level') # e.g., 'pod', 'class', 'pod_db', 'class_db', 'tag_db'

        # --- Basic Validation ---
        if not delete_level:
            flash("Missing delete level.", "danger")
            return redirect(url_for('view_allocations'))
        if not tag: # Tag is required for almost all actions here
            flash("Tag identifier is missing.", "danger")
            return redirect(url_for('view_allocations'))

        # Parse numbers if present
        pod_num = int(pod_num_str) if pod_num_str else None
        class_num = int(class_num_str) if class_num_str else None

        # --- Handle DB Deletion Levels ---
        if delete_level.endswith('_db'):
            app.logger.info(
                f"DB delete request: Level='{delete_level}', Tag='{tag}', "
                f"Course='{course}', Pod='{pod_num}', Class='{class_num}'"
            )
            success, item_desc = False, "Unknown Item"
            try:
                # --- NEW: Handle Tag DB Deletion ---
                if delete_level == 'tag_db':
                    # Call delete_from_database with only the tag specified
                    success = delete_from_database(tag=tag)
                    item_desc = f"Entire Tag '{tag}'"
                # --- End New ---
                elif delete_level == 'course_db':
                    if not course: flash("Course name missing for DB deletion.", "warning"); return redirect(url_for('view_allocations'))
                    success = delete_from_database(tag=tag, course_name=course)
                    item_desc = f"Course '{course}' (in Tag '{tag}')"
                elif delete_level == 'class_db':
                    if not course or class_num is None: flash("Course or Class# missing for DB deletion.", "warning"); return redirect(url_for('view_allocations'))
                    success = delete_from_database(tag=tag, course_name=course, class_number=class_num)
                    item_desc = f"Class {class_num} (in Course '{course}', Tag '{tag}')"
                elif delete_level == 'pod_db':
                    if not course or pod_num is None: flash("Course or Pod# missing for DB deletion.", "warning"); return redirect(url_for('view_allocations'))
                    # Pass class_num context if available (for F5 nested pods)
                    success = delete_from_database(tag=tag, course_name=course, pod_number=pod_num, class_number=class_num)
                    item_desc = f"Pod {pod_num}" + (f" (Class {class_num})" if class_num else "") + f" (in Course '{course}', Tag '{tag}')"
                else:
                    flash("Invalid DB delete level specified.", "warning")
                    return redirect(url_for('view_allocations'))

                flash(f"Removed DB entry for {item_desc}." if success else f"Failed DB removal for {item_desc}.", 'success' if success else 'danger')

            except Exception as e_db:
                flash(f"Error during DB delete: {e_db}", 'danger')
                app.logger.error(f"DB delete error: {e_db}", exc_info=True)

            # Redirect back to allocations page after DB action
            query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')} # Preserve filters
            return redirect(url_for('view_allocations', **query_params))

        # --- Handle Full Teardown (Run labbuild.py) ---
        # (Keep existing logic for 'pod' and 'class' levels)
        elif delete_level in ['class', 'pod']:
            if not all([vendor, course, host]):
                flash("Vendor, Course, Host required for infrastructure teardown.", "danger")
                return redirect(url_for('view_allocations'))

            args_list, item_desc = [], ""
            if delete_level == 'class' and vendor.lower() == 'f5' and class_num is not None:
                args_list = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-cn', str(class_num)]
                item_desc = f"F5 Class {class_num}"
            elif delete_level == 'pod' and pod_num is not None:
                args_list = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]
                item_desc = f"Pod {pod_num}"
                # Add class context for F5 pods during teardown
                if vendor.lower() == 'f5' and class_num is not None:
                    args_list.extend(['-cn', str(class_num)])
                    item_desc += f" (Class {class_num})"
            else:
                flash("Invalid teardown level or missing identifiers.", "danger")
                return redirect(url_for('view_allocations'))

            if args_list:
                thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True)
                thread.start()
                flash(f"Submitted infrastructure teardown for {item_desc}.", 'info')
            else:
                flash("Failed build teardown command.", "danger")
        else:
            # Handle unsupported levels like 'tag' or 'course' for full teardown if desired,
            # otherwise just show error. Currently not supported.
            flash(f"Unsupported teardown level: '{delete_level}'. Only 'pod' and 'class' infrastructure teardown supported.", "warning")

    except Exception as e:
        app.logger.error(f"Error processing teardown/delete request: {e}", exc_info=True)
        flash(f"Error processing request: {e}", 'danger')

    # Redirect back to allocations page by default or on general error
    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')} # Preserve filters
    return redirect(url_for('view_allocations', **query_params))


@app.route('/teardown-tag', methods=['POST'])
def teardown_tag():
    """
    Finds all allocations for a given tag and triggers background
    labbuild teardown commands for each unique group (course/host/range/class).
    """
    tag = request.form.get('tag')
    if not tag:
        flash("Tag identifier is missing for teardown.", "danger")
        query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
        return redirect(url_for('view_allocations', **query_params))

    app.logger.info(f"Initiating FULL teardown for Tag: '{tag}'")
    tasks_to_run = [] # List to hold ([args_list], description) tuples

    try:
        if db is None or alloc_collection is None:
            raise Exception("Database connection or allocation collection unavailable.")

        # Find the tag document
        tag_doc = alloc_collection.find_one({"tag": tag})
        if not tag_doc:
            flash(f"Tag '{tag}' not found in database.", "warning")
            query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
            return redirect(url_for('view_allocations', **query_params))

        # --- Collect Teardown Arguments ---
        courses_in_tag = tag_doc.get("courses", [])
        if not isinstance(courses_in_tag, list): courses_in_tag = []

        for course in courses_in_tag:
            if not isinstance(course, dict): continue
            course_name = course.get("course_name")
            vendor = course.get("vendor")
            pod_details_list = course.get("pod_details", [])
            if not isinstance(pod_details_list, list) or not course_name or not vendor: continue

            # Group pods by host for non-F5, classes by host for F5
            items_by_host = defaultdict(lambda: {"pods": set(), "classes": set()})
            for pd in pod_details_list:
                if not isinstance(pd, dict): continue
                host = pd.get("host", pd.get("pod_host"))
                pod_num = pd.get("pod_number")
                class_num = pd.get("class_number")
                if not host: continue # Cannot teardown without host

                is_f5 = vendor.lower() == 'f5'
                if is_f5 and class_num is not None:
                     # If F5, teardown is by class number
                     items_by_host[host]["classes"].add(class_num)
                elif pod_num is not None:
                     # For non-F5, collect individual pod numbers per host
                     items_by_host[host]["pods"].add(pod_num)

            # --- Generate labbuild command args for each host group ---
            for host, items in items_by_host.items():
                # Teardown F5 Classes
                for class_num in sorted(list(items["classes"])):
                    args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-cn', str(class_num)]
                    desc = f"Tag '{tag}', Course '{course_name}', Host '{host}', Class '{class_num}'"
                    tasks_to_run.append((args, desc))

                # Teardown non-F5 Pods (or F5 pods if stored flat - current logic assumes class teardown for F5)
                if items["pods"]:
                    # Note: Teardown by individual pod range might be inefficient if many pods.
                    # Consider if your teardown script handles a list or if you need ranges.
                    # For now, generating individual teardown commands per pod found.
                    # TODO: Implement range generation if teardown script supports -s X -e Y efficiently for scattered pods.
                    for pod_num in sorted(list(items["pods"])):
                        args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]
                        # Add -cn if it's an F5 pod teardown potentially? Assumes flat structure if needed.
                        # This part might need adjustment based on how F5 pod teardown is invoked.
                        # Currently assumes F5 teardown happens only via class_num above.
                        desc = f"Tag '{tag}', Course '{course_name}', Host '{host}', Pod '{pod_num}'"
                        tasks_to_run.append((args, desc))


        # --- Execute Tasks Sequentially (Safer than Parallel for Teardown) ---
        if not tasks_to_run:
             flash(f"No valid teardown tasks found for Tag '{tag}'.", "warning")
        else:
            app.logger.info(f"Found {len(tasks_to_run)} teardown tasks for Tag '{tag}'. Submitting sequentially...")
            # Use a single background thread to run tasks one by one
            def run_sequential_tasks(tasks):
                 total = len(tasks)
                 for i, (args, desc) in enumerate(tasks):
                     app.logger.info(f"Starting teardown task {i+1}/{total}: {desc} (Args: {' '.join(args)})")
                     run_labbuild_task(args) # This function already runs subprocess.run
                     app.logger.info(f"Finished teardown task {i+1}/{total}: {desc}")
                 app.logger.info(f"All {total} teardown tasks for Tag '{tag}' submitted.")
                 # Optionally, delete the tag DB entry AFTER all tasks are submitted (or finished if run_labbuild_task was blocking)
                 # Be cautious with auto-deleting the DB entry here. Maybe leave it manual.
                 # delete_from_database(tag=tag)
                 # logger.info(f"DB entry for tag '{tag}' potentially removed after teardown tasks submitted.")

            thread = threading.Thread(target=run_sequential_tasks, args=(tasks_to_run,), daemon=True)
            thread.start()
            flash(f"Submitted {len(tasks_to_run)} teardown tasks for Tag '{tag}'. See logs for progress.", "info")

    except Exception as e:
        app.logger.error(f"Error processing teardown for Tag '{tag}': {e}", exc_info=True)
        flash(f"Error submitting teardown for Tag '{tag}': {e}", 'danger')

    # Redirect back to allocations page, preserving filters
    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('view_allocations', **query_params))


@app.route('/run', methods=['POST'])
def run_now():
    """Handle immediate run request."""
    form_data = request.form.to_dict()
    args_list = build_args_from_form(form_data)
    if not args_list:
        flash('Invalid form data or validation failed.', 'danger')
        return redirect(url_for('index'))
    try:
        thread = threading.Thread(
            target=run_labbuild_task, args=(args_list,), daemon=True
        )
        thread.start()
        flash(f"Submitted immediate run: {' '.join(args_list)}", 'info')
    except Exception as e:
        app.logger.error(f"Failed start thread run_now: {e}", exc_info=True)
        flash("Error starting task.", 'danger')
    return redirect(url_for('index'))


@app.route('/schedule', methods=['POST'])
def schedule_run():
    """Handle schedule run request."""
    form_data = request.form.to_dict()
    args_list = build_args_from_form(form_data)
    if not args_list:
        flash('Invalid form data or validation failed.', 'danger')
        return redirect(url_for('index'))
    if not scheduler.running:
        flash("Scheduler not running. Cannot schedule job.", "danger")
        return redirect(url_for('index'))

    schedule_type = form_data.get('schedule_type', 'date')
    schedule_time_str = form_data.get('schedule_time')
    cron_expression = form_data.get('cron_expression')
    interval_value = form_data.get('interval_value')
    interval_unit = form_data.get('interval_unit', 'minutes')

    try:
        job_name = (
            f"{form_data.get('command')}_{form_data.get('vendor')}_"
            f"{form_data.get('course', 'N/A')}"
        )
        trigger = None
        flash_msg = ""

        if schedule_type == 'date' and schedule_time_str:
            # --- Convert Naive Input -> Server Local -> UTC ---
            naive_dt = datetime.datetime.fromisoformat(schedule_time_str)
            # 1. Localize to Server TZ (Interpret input as server's wall-clock time)
            server_local_dt = SERVER_TIMEZONE.localize(naive_dt)
            # 2. Convert to equivalent UTC instant
            run_date_utc = server_local_dt.astimezone(pytz.utc)
            # 3. Schedule using the UTC datetime object
            trigger = DateTrigger(run_date=run_date_utc)
            # --- End Conversion ---

            flash_msg = f"for {server_local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')} (Server Time) / {run_date_utc.strftime('%Y-%m-%d %H:%M:%S %Z')} (UTC)"
            log_time_str = run_date_utc.isoformat()
            app.logger.info(f"Scheduling job for UTC instant: {run_date_utc} (interpreted from naive input as server time: {server_local_dt})")

        elif schedule_type == 'cron' and cron_expression:
            # CronTrigger uses the scheduler's timezone (now SERVER_TIMEZONE) by default
            parts = cron_expression.split()
            if len(parts) != 5: raise ValueError("Invalid cron format.")
            # Explicitly pass the server timezone for clarity, though it's the default now
            trigger = CronTrigger.from_crontab(cron_expression, timezone=SERVER_TIMEZONE)
            flash_msg = f"with cron: '{cron_expression}' ({SERVER_TIMEZONE_STR})"
            log_time_str = flash_msg
            app.logger.info(f"Scheduling cron job relative to server timezone: {log_time_str}")

        elif schedule_type == 'interval' and interval_value:
             # IntervalTrigger doesn't inherently use timezones for its interval,
             # but the *first* run might be influenced by start_date if set.
             # For simplicity, we assume interval starts relative to when it's added.
             interval_val_int = int(interval_value)
             kwargs = {interval_unit: interval_val_int}
             # Optional: Set a start_date in the server's timezone if needed
             # kwargs['start_date'] = SERVER_TIMEZONE.localize(datetime.datetime.now() + timedelta(seconds=5))
             trigger = IntervalTrigger(**kwargs)
             flash_msg = f"every {interval_val_int} {interval_unit}"
             app.logger.info(f"Scheduling interval job: every {interval_val_int} {interval_unit}")
        else:
             flash('Invalid schedule details.', 'danger')
             return redirect(url_for('index'))

        # Add job (keep misfire_grace_time)
        job = scheduler.add_job(
            run_labbuild_task, trigger=trigger, args=[args_list],
            name=job_name, misfire_grace_time=3600, replace_existing=False
        )
        flash(f"Scheduled job '{job.id}' {flash_msg}", 'success')
        app.logger.info(f"Job '{job.id}' ({job_name}) scheduled. Trigger time info: {log_time_str}")
    except ValueError as ve:
        app.logger.error(f"Invalid schedule input: {ve}", exc_info=True)
        flash(f"Invalid schedule input: {ve}", 'danger')
    except Exception as e:
        app.logger.error(f"Failed schedule job: {e}", exc_info=True)
        flash(f"Error scheduling job: {e}", 'danger')
    return redirect(url_for('index'))


# --- Route for Scheduling Batch (MODIFIED) ---
@app.route('/schedule-batch', methods=['POST'])
def schedule_batch():
    # ... (keep file handling and delay parsing) ...
    if 'batch_file' not in request.files: flash('No file part.', 'danger'); return redirect(url_for('index'))
    file = request.files['batch_file']
    if file.filename == '': flash('No file selected.', 'danger'); return redirect(url_for('index'))
    start_time_str = request.form.get('start_time')
    delay_minutes_str = request.form.get('delay_minutes', '30')

    if not start_time_str: flash('Start time required.', 'danger'); return redirect(url_for('index'))
    try:
        delay_minutes = int(delay_minutes_str)
        if delay_minutes < 1: raise ValueError("Delay >= 1 min.")
    except ValueError: flash('Invalid delay.', 'danger'); return redirect(url_for('index'))
    if not scheduler.running: flash("Scheduler not running.", "danger"); return redirect(url_for('index'))

    # --- Parse Start Time: Naive -> Server Local -> UTC ---
    try:
        naive_dt = datetime.datetime.fromisoformat(start_time_str)
        first_run_server_local_dt = SERVER_TIMEZONE.localize(naive_dt)
        first_run_utc = first_run_server_local_dt.astimezone(pytz.utc) # Schedule UTC time
        app.logger.info(f"Batch Schedule: First job UTC start: {first_run_utc} (interpreted from naive input as server time: {first_run_server_local_dt})")
    except Exception as e:
        app.logger.error(f"Err parse batch start: {e}", exc_info=True)
        flash("Err process time.", "danger")
        return redirect(url_for('index'))
    # --- End Parse Start Time ---

    scheduled_count = 0
    failed_lines = 0
    current_run_time_utc = first_run_utc # Use UTC for tracking
    job_delay = datetime.timedelta(minutes=int(request.form.get('delay_minutes', '30'))) # Ensure delay is int

    try:
        filename = werkzeug.utils.secure_filename(file.filename)
        app.logger.info(f"Processing batch file: {filename}")
        stream = io.TextIOWrapper(file.stream, encoding='utf-8')
        lines = stream.readlines()

        for i, line in enumerate(lines):
            args_list = parse_command_line(line)
            if args_list:
                # Calculate run time for this job (already timezone-aware)
                run_date_utc = current_run_time_utc
                job_name = f"batch_{filename}_{i+1}_{args_list[0]}"
                # --- Use the timezone-aware datetime directly ---
                trigger = DateTrigger(run_date=run_date_utc)

                try:
                    job = scheduler.add_job(
                        run_labbuild_task, trigger=trigger, args=[args_list],
                        name=job_name, misfire_grace_time=3600, replace_existing=False
                    )
                    # Log using the server's local time representation
                    run_date_server_local = run_date_utc.astimezone(SERVER_TIMEZONE)
                    log_time_str = f"{run_date_server_local.strftime('%Y-%m-%d %H:%M:%S %Z%z')} (Server) / {run_date_utc.strftime('%Y-%m-%d %H:%M:%S %Z')} (UTC)"
                    app.logger.info(f"Scheduled batch job {i+1}: ID={job.id}, Name='{job_name}', RunAt={log_time_str}")
                    scheduled_count += 1
                    current_run_time_utc += job_delay # Increment for next job
                except Exception as e_sched:
                    app.logger.error(f"Fail schedule line {i+1}: {e_sched}", exc_info=True)
                    failed_lines += 1
            elif line.strip() and not line.strip().startswith('#'):
                app.logger.warning(f"Skip invalid line {i+1}: {line.strip()}")
                failed_lines += 1

        flash(f"Scheduled {scheduled_count} jobs from '{filename}'. {failed_lines} lines failed/skipped.", 'success' if scheduled_count > 0 else 'warning')

    except Exception as e:
        app.logger.error(f"Error processing batch file '{filename}': {e}", exc_info=True)
        flash(f"Error processing batch file: {e}", 'danger')

    return redirect(url_for('index'))


@app.route('/logs/<run_id>')
def log_detail(run_id):
    """Display details and logs for a specific run."""

    redis_url_in_route = app.config.get("REDIS_URL") # Use .get for safety
    app.logger.info(f"--- DEBUG [/logs/{run_id}]: app.config['REDIS_URL'] = {redis_url_in_route} ---")

    current_theme = request.cookies.get('theme', 'light')
    op_log_data = None
    historical_log_messages = []
    std_log_count = 0
    sse_available = redis_url_in_route is not None # Base decision on logged value

    if db is None:
        flash("DB unavailable.", "danger")
        return render_template(
            'log_detail.html', log=None, historical_log_messages=[],
            std_log_count=0, run_id=run_id, current_theme=current_theme,
            sse_enabled=sse_available
        )

    # Fetch Op Log Summary
    try:
        if op_logs_collection is not None:
             op_log_data = op_logs_collection.find_one({'run_id': run_id})
             if op_log_data:
                op_log_data['_id'] = str(op_log_data['_id'])
                op_log_data['start_time_iso'] = format_datetime(op_log_data.get('start_time'))
                op_log_data['end_time_iso'] = format_datetime(op_log_data.get('end_time'))
                op_log_data['args_display'] = {
                    k: str(v) for k, v in op_log_data.get('args', {}).items()
                }
                for p_log in op_log_data.get('pod_statuses', []):
                    p_log['timestamp_iso'] = format_datetime(p_log.get('timestamp'))
        else: flash("Op logs collection unavailable.", "warning")
    except PyMongoError as e:
        app.logger.error(f"Error fetch op log {run_id}: {e}")
        flash("DB error fetch op log.", "danger")
    except Exception as e:
        app.logger.error(f"Unexpected error fetch op log {run_id}: {e}",
                         exc_info=True)
        flash("Server error.", "danger")

    # Fetch Detailed Logs
    try:
        if std_logs_collection is not None:
            log_doc = std_logs_collection.find_one({'run_id': run_id})
            if log_doc:
                messages = log_doc.get('messages', [])
                std_log_count = len(messages)
                for msg in messages:
                    if isinstance(msg, dict):
                        historical_log_messages.append({
                            'level': msg.get('level', 'N/A'),
                            'logger_name': msg.get('logger_name', 'N/A'),
                            'message': msg.get('message', ''),
                            'timestamp_iso': format_datetime(msg.get('timestamp'))
                        })
            elif op_log_data:
                 flash(f"Detailed log document not found for run {run_id}.",
                       "warning")
            elif not op_log_data:
                 flash(f"No logs found for run ID {run_id}.", 'warning')
        else: flash("Detailed logs collection unavailable.", "warning")
    except PyMongoError as e:
        app.logger.error(f"Error fetch detailed logs {run_id}: {e}")
        flash("DB error fetch detailed logs.", "danger")
    except Exception as e:
        app.logger.error(f"Unexpected error fetch detailed logs {run_id}: {e}",
                         exc_info=True)
        flash("Server error.", "danger")

    return render_template(
        'log_detail.html', log=op_log_data, historical_log_messages=historical_log_messages,
        std_log_count=std_log_count, run_id=run_id, current_theme=current_theme,
        sse_enabled=sse_available
    )

@app.route('/jobs/delete/<job_id>', methods=['POST'])
def delete_job(job_id):
    """Delete a single scheduled job."""
    if not scheduler.running:
        flash("Scheduler not running.", "danger")
        return redirect(url_for('index'))
    try:
        scheduler.remove_job(job_id)
        flash(f"Job {job_id} deleted.", 'success')
    except Exception as e:
        app.logger.error(f"Failed delete job {job_id}: {e}", exc_info=True)
        flash(f"Error deleting job: {e}", 'danger')
    return redirect(url_for('index'))

@app.route('/jobs/delete-bulk', methods=['POST'])
def delete_bulk_jobs():
    """Deletes multiple scheduled jobs based on selected IDs."""
    if not scheduler.running:
        flash("Scheduler not running.", "danger")
        return redirect(url_for('index'))

    job_ids_to_delete = request.form.getlist('job_ids') # Gets list of selected checkbox values

    if not job_ids_to_delete:
        flash("No jobs selected for deletion.", "warning")
        return redirect(url_for('index'))

    deleted_count = 0
    failed_count = 0
    for job_id in job_ids_to_delete:
        try:
            scheduler.remove_job(job_id)
            deleted_count += 1
        except Exception as e:
            app.logger.error(f"Failed delete job {job_id} during bulk operation: {e}", exc_info=True)
            failed_count += 1

    flash(f"Successfully deleted {deleted_count} job(s). Failed to delete {failed_count} job(s).",
          'success' if failed_count == 0 else 'warning')

    return redirect(url_for('index'))

@app.route('/logs/all')
def all_logs():
    """Displays all operation logs with filtering and pagination."""
    current_theme = request.cookies.get('theme', 'light')
    page = request.args.get('page', 1, type=int)
    per_page = 50
    current_filters = {k: v for k, v in request.args.items() if k != 'page'}
    mongo_filter = build_log_filter_query(request.args)
    logs, total_logs = [], 0

    if db is None or op_logs_collection is None:
        flash("DB or op_logs collection unavailable.", "danger")
        return render_template(
            'all_logs.html', logs=[], current_page=1, total_pages=0,
            total_logs=0, pagination_args={}, current_filters={},
            current_theme=current_theme
        )

    try:
        total_logs = op_logs_collection.count_documents(mongo_filter)
        skip = (page - 1) * per_page
        cursor = op_logs_collection.find(mongo_filter).sort(
            'start_time', DESCENDING
        ).skip(skip).limit(per_page)
        docs = list(cursor)
        for log in docs:
            log['_id'] = str(log['_id'])
            log['start_time_iso'] = format_datetime(log.get('start_time'))
            log['end_time_iso'] = format_datetime(log.get('end_time'))
            log['args_display'] = {
                k: str(v) for k, v in log.get('args', {}).items()
            }
            logs.append(log)
    except PyMongoError as e:
        app.logger.error(f"Error fetching all logs page {page}: {e}")
        flash("Error fetching logs.", "danger")
    except Exception as e:
        app.logger.error(f"Unexpected error processing all logs: {e}",
                         exc_info=True)
        flash("Server error.", "danger")

    total_pages = math.ceil(total_logs / per_page) if per_page > 0 else 1
    pagination_args = current_filters.copy()

    return render_template(
        'all_logs.html', logs=logs, current_page=page, total_pages=total_pages,
        total_logs=total_logs, pagination_args=pagination_args,
        current_filters=current_filters, current_theme=current_theme
    )


@app.route('/terminal')
def terminal():
    """Renders the pseudo-terminal page."""
    current_theme = request.cookies.get('theme', 'light')
    return render_template('terminal.html', current_theme=current_theme)

# --- NEW SSE Endpoint for log streaming ---
@app.route("/log-stream/<run_id>")
def log_stream(run_id):
    """SSE endpoint to stream logs for a specific run_id via Redis."""
    if not app.config.get("REDIS_URL"):
        msg = json.dumps({"error": "Real-time streaming not configured."})
        return Response(f"event: error\ndata: {msg}\n\n",
                        mimetype="text/event-stream")

    def generate_log_stream(run_id_local):
        redis_client = None
        pubsub = None
        channel = f"log_stream::{run_id_local}"
        try:
            redis_client = redis.Redis.from_url(
                app.config["REDIS_URL"], decode_responses=True
            )
            pubsub = redis_client.pubsub()
            pubsub.subscribe(channel)
            app.logger.info(f"SSE client subscribed to Redis channel: {channel}")
            yield f"event: connected\ndata: Subscribed to {run_id_local}\n\n"

            for message in pubsub.listen():
                if message['type'] == 'message':
                    log_data_json = message['data']
                    yield f"data: {log_data_json}\n\n"
                elif message['type'] == 'subscribe':
                    continue # Ignore confirmation

        except redis.exceptions.ConnectionError as e:
            app.logger.error(f"SSE Redis connection error {run_id_local}: {e}")
            yield f"event: error\ndata: Redis connection error\n\n"
        except Exception as e:
            app.logger.error(f"SSE generator error {run_id_local}: {e}",
                             exc_info=True)
            yield f"event: error\ndata: Internal stream error\n\n"
        finally:
            # Ensure resources are cleaned up
            if pubsub:
                try: pubsub.unsubscribe(channel); pubsub.close()
                except Exception as e_close: app.logger.error(f"Error closing pubsub {run_id_local}: {e_close}")
            if redis_client:
                try: redis_client.close()
                except Exception as e_close: app.logger.error(f"Error closing Redis {run_id_local}: {e_close}")
            app.logger.info(f"SSE stream closed for {run_id_local}")

    # Use stream_with_context to ensure Flask context is available if needed
    return Response(stream_with_context(generate_log_stream(run_id)),
                    mimetype="text/event-stream")

# --- SSE Route for Streaming ---
def stream_labbuild_process(full_command_list):
    """Generator runs labbuild.py and yields output line by line."""
    cmd_str = ' '.join(shlex.quote(arg) for arg in full_command_list)
    app.logger.info(f"Streaming command: {cmd_str}")
    process = None
    try:
        python_executable = sys.executable
        process = subprocess.Popen(
            full_command_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True, encoding='utf-8', errors='replace',
            cwd=project_root, bufsize=1  # Line buffered
        )
        yield f"event: start\ndata: Process started (PID: {process.pid})\n\n"
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                # Escape backslashes first, then newlines for SSE
                formatted_line = line.replace('\\', '\\\\').replace('\n', '\\n')
                yield f"data: {formatted_line}\n\n"
                time.sleep(0.01) # Allow browser to render
            process.stdout.close() # Ensure pipe is closed

        return_code = process.wait() # Wait for process to finish
        app.logger.info(f"Streaming command finished. RC: {return_code}")
        yield f"event: close\ndata: Process finished with exit code {return_code}\n\n"

    except FileNotFoundError:
        err_msg = f"Error: Command '{python_executable}' or script not found."
        app.logger.error(err_msg)
        yield f"event: error\ndata: {err_msg}\n\n"
        yield f"event: close\ndata: Process failed (file not found)\n\n"
    except Exception as e:
        err_msg = f"Subprocess error: {e}"
        app.logger.error(f"Stream error: {e}", exc_info=True)
        yield f"event: error\ndata: {err_msg}\n\n"
        yield f"event: close\ndata: Process failed (exception)\n\n"
    finally:
         # Ensure process is terminated if generator exits unexpectedly
         if process and process.poll() is None:
             try:
                 process.terminate()
                 time.sleep(0.2) # Give terminate a moment
                 if process.poll() is None: # Still running?
                    process.kill()
                    app.logger.warning(f"Killed streaming process {process.pid}")
             except Exception as kill_e:
                 app.logger.error(f"Error terminating stream process: {kill_e}")


@app.route('/stream-command', methods=['POST'])
def stream_command():
    """Handles command submission and returns SSE stream."""
    command_line = request.form.get('command', '')
    if not command_line.strip():
        return Response(
            "event: error\ndata: Empty command.\n\n"
            "event: close\ndata: Empty command\n\n",
            mimetype="text/event-stream"
        )

    # Basic prefix check - NOT comprehensive security
    allowed_prefixes = ('labbuild ', 'python labbuild.py ')
    if not command_line.strip().startswith(allowed_prefixes):
         return Response(
             "event: error\ndata: Invalid command prefix.\n\n"
             "event: close\ndata: Invalid command\n\n",
             mimetype="text/event-stream"
         )

    try:
        # Build the full command list safely
        if command_line.strip().startswith('python '):
             parts = shlex.split(command_line, posix=(os.name != 'nt'))
             if len(parts) < 2 or not parts[1].endswith('labbuild.py'):
                 raise ValueError("Invalid python command.")
             script_path = os.path.join(project_root, parts[1])
             cmd_list = [sys.executable, script_path] + parts[2:]
        else:
             parts = shlex.split(command_line, posix=(os.name != 'nt'))
             script_path = os.path.join(project_root, 'labbuild.py')
             cmd_list = [sys.executable, script_path] + parts[1:]

        if not os.path.exists(cmd_list[1]): # Check script path exists
             raise FileNotFoundError(f"Script not found: {cmd_list[1]}")

    except (ValueError, FileNotFoundError) as e:
        return Response(
            f"event: error\ndata: Error parsing command: {e}\n\n"
            f"event: close\ndata: Invalid command\n\n",
            mimetype="text/event-stream"
        )
    except Exception as e:
        app.logger.error(f"Error parsing command: {e}", exc_info=True)
        return Response(
            "event: error\ndata: Server error parsing command.\n\n"
            "event: close\ndata: Server error\n\n",
            mimetype="text/event-stream"
        )

    # Return the streaming response
    return Response(
        stream_with_context(stream_labbuild_process(cmd_list)),
        mimetype='text/event-stream'
    )

# --- NEW: Route for Upcoming Courses ---
@app.route('/upcoming-courses')
def view_upcoming_courses():
    current_theme = request.cookies.get('theme', 'light')
    # Initialize lists
    hosts_list = []
    course_configs_list = []
    mapping_rules = []
    courses_with_preselects = [] # Store final augmented data
    error_message = None

    # Fetch Hosts
    if host_collection is not None:
        try: # ... fetch hosts ...
             hosts_cursor = host_collection.find({}, {"host_name": 1, "_id": 0}).sort("host_name", 1)
             hosts_list = [host['host_name'] for host in hosts_cursor if 'host_name' in host]
        except PyMongoError as e: app.logger.error(f"Hosts fetch error: {e}"); flash("Error fetching hosts.", "warning")
    else: flash("Host collection unavailable.", "warning")

    # Fetch Course Configs
    if course_config_collection is not None:
        try: # ... fetch configs ...
             configs_cursor = course_config_collection.find({},{"course_name": 1, "vendor_shortcode": 1, "_id": 0}).sort([("vendor_shortcode", 1), ("course_name", 1)])
             course_configs_list = list(configs_cursor) # Keep as list of dicts
        except PyMongoError as e: app.logger.error(f"Configs fetch error: {e}"); flash("Error fetching lab build course list.", "warning")
    else: flash("Course config collection unavailable.", "warning")

    # Fetch Mapping Rules
    if course_mapping_rules_collection is not None:
        try: # ... fetch rules ...
             rules_cursor = course_mapping_rules_collection.find().sort("priority", 1)
             mapping_rules = list(rules_cursor) # Keep raw BSON types for backend logic
        except PyMongoError as e: app.logger.error(f"Rules fetch error: {e}"); flash("Error fetching automation rules.", "warning")
    else: flash("Course mapping rules collection unavailable.", "warning")

    # --- Fetch SF data AND Apply Rules ---
    try:
        # Call the orchestrator which now applies rules internally
        courses_with_preselects = get_upcoming_courses_data(
            mapping_rules, course_configs_list, hosts_list
        )

        if courses_with_preselects is None:
            flash("Failed to fetch or process Salesforce data. Check server logs.", "danger")
            courses_with_preselects = []
            error_message = "Error retrieving data."
        elif not courses_with_preselects:
             flash("No upcoming courses found for the next week.", "info")
             error_message = "No courses scheduled for upcoming week."

    except Exception as e:
        app.logger.error(f"Unexpected error in /upcoming-courses route: {e}", exc_info=True)
        flash("An unexpected error occurred.", "danger")
        error_message = "Server error."
        courses_with_preselects = []

    return render_template(
        'upcoming_courses.html',
        courses=courses_with_preselects, # Pass augmented data
        error_message=error_message,
        # Pass lists needed for dropdowns *in the template*
        hosts_list=hosts_list,
        course_configs_list=course_configs_list,
        # No need to pass mapping_rules to template anymore
        current_theme=current_theme
    )

@app.route('/current-courses')
def view_current_courses():
    """Displays CURRENT week's courses fetched from Salesforce."""
    current_theme = request.cookies.get('theme', 'light')
    courses_data = []
    hosts_list = []
    course_configs_list = []
    error_message = None

    # Fetch Hosts and Course Configs (same as for upcoming)
    if host_collection is not None:
        try:
            hosts_cursor = host_collection.find({}, {"host_name": 1, "_id": 0}).sort("host_name", 1)
            hosts_list = [host['host_name'] for host in hosts_cursor if 'host_name' in host]
        except PyMongoError as e: app.logger.error(f"Error fetching hosts list: {e}"); flash("Error fetching hosts.", "warning")
    else: flash("Host collection unavailable.", "warning")
    if course_config_collection is not None:
        try:
            configs_cursor = course_config_collection.find({}, {"course_name": 1, "vendor_shortcode": 1, "_id": 0}).sort([("vendor_shortcode", 1), ("course_name", 1)])
            course_configs_list = list(configs_cursor)
        except PyMongoError as e: app.logger.error(f"Error fetching course configs list: {e}"); flash("Error fetching lab build course list.", "warning")
    else: flash("Course config collection unavailable.", "warning")

    # Fetch and process Salesforce data for the CURRENT week
    try:
        courses_data = get_current_courses_data() # Calls the CURRENT week function

        if courses_data is None:
            flash("Failed to fetch or process Salesforce data. Check server logs.", "danger")
            courses_data = []
            error_message = "Error retrieving data."
        elif not courses_data:
             flash("No courses found running this week.", "info")
             error_message = "No courses scheduled for the current week."

    except Exception as e:
        app.logger.error(f"Unexpected error in /current-courses route: {e}", exc_info=True)
        flash("An unexpected error occurred while loading current courses.", "danger")
        error_message = "Server error."
        courses_data = []

    return render_template(
        'current_courses.html', # <<< Renders the NEW current_courses template
        courses=courses_data,
        error_message=error_message,
        hosts_list=hosts_list,
        course_configs_list=course_configs_list,
        current_theme=current_theme
    )

@app.route('/build-row', methods=['POST'])
def build_row():
    """Handles the 'Build' action triggered from a row in upcoming courses."""
    try:
        data = request.json # Expecting JSON data from frontend JS
        if not data:
            return jsonify({"status": "error", "message": "No data received."}), 400

        # Extract data sent from JavaScript
        labbuild_course = data.get('labbuild_course')
        start_pod = data.get('start_pod')
        end_pod = data.get('end_pod')
        host = data.get('host')
        vendor = data.get('vendor') # Derived vendor passed from JS
        sf_course_code = data.get('sf_course_code') # Optional: for logging/tagging

        # --- Basic Validation ---
        if not all([labbuild_course, start_pod, end_pod, host, vendor]):
             return jsonify({"status": "error", "message": "Missing required fields (Course, Pod Range, Host, Vendor)."}), 400
        try:
            # Validate pod numbers
            s_pod = int(start_pod)
            e_pod = int(end_pod)
            if s_pod < 0 or e_pod < 0 or s_pod > e_pod:
                 raise ValueError("Invalid pod range.")
        except ValueError:
             return jsonify({"status": "error", "message": "Invalid Start/End Pod numbers."}), 400

        # Construct args for labbuild setup command
        # Use a default tag or create one from course info
        tag = f"uc_{sf_course_code}" if sf_course_code else "uc_dashboard"
        tag = tag[:50] # Limit tag length if necessary

        args_list = [
            'setup',
            '-v', vendor,
            '-g', labbuild_course,
            '--host', host,
            '-s', str(s_pod),
            '-e', str(e_pod),
            '-t', tag
            # Add other default flags if needed, e.g., '-th', '8'
        ]

        # Run the task in the background
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True)
        thread.start()

        flash(f"Submitted build for {labbuild_course} (Pods {s_pod}-{e_pod}) on {host}.", "info")
        return jsonify({"status": "success", "message": "Build submitted."}), 200

    except Exception as e:
        app.logger.error(f"Error in /build-row: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Internal server error."}), 500

# --- Cleanup ---
def shutdown_resources():
    """Shutdown scheduler and close DB connections."""
    global mongo_client_scheduler, mongo_client_app
    if scheduler and scheduler.running:
        app.logger.info("Shutting down scheduler...")
        try: scheduler.shutdown(wait=False)
        except Exception as e: app.logger.error(f"Scheduler shutdown error: {e}", exc_info=True)
        finally: app.logger.info("Scheduler shutdown completed.")

    if mongo_client_scheduler:
        app.logger.info("Closing scheduler MongoDB client..."); mongo_client_scheduler.close()
        mongo_client_scheduler = None; app.logger.info("Scheduler MongoDB client closed.")
    if mongo_client_app:
        app.logger.info("Closing app MongoDB client..."); mongo_client_app.close()
        mongo_client_app = None; app.logger.info("App MongoDB client closed.")

atexit.register(shutdown_resources)

# --- Main Guard ---
if __name__ == '__main__':
    app.run(
        debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true',
        host=os.getenv('FLASK_HOST', '0.0.0.0'),
        port=int(os.getenv('FLASK_PORT', 5001))
    )

# --- END OF REFACTORED app.py ---