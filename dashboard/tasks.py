# dashboard/tasks.py

import os
import sys
import subprocess
import shlex
import logging
import time
import datetime
import pytz # For timezone-aware datetimes
from pymongo.errors import PyMongoError

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger('dashboard.tasks')

DEFAULT_LOG_RETENTION_DAYS = 90 # Keep logs for 90 days by default

def purge_old_mongodb_logs(retention_days: int = DEFAULT_LOG_RETENTION_DAYS):
    """
    Purges documents older than 'retention_days' from specified MongoDB log collections.
    """
    # It's better for scheduled tasks to manage their own DB connections
    # rather than relying on global Flask app extension objects if run in a separate thread.
    # However, if APScheduler is tightly integrated and jobs can access Flask's app context,
    # you *might* be able to use `current_app.extensions['pymongo'].db` or similar.
    # For robustness, let's create a new client instance here.

    from dashboard.extensions import mongo_client_app, DB_NAME, OPERATION_LOG_COLLECTION, LOG_COLLECTION

    if not mongo_client_app: # Check if the app's client was initialized
        logger.error("MongoDB client (mongo_client_app) not available for log purging.")
        # As a fallback, try to create a new one if MONGO_URI is available
        from db_utils import MONGO_URI as TOP_LEVEL_MONGO_URI # Get global MONGO_URI
        if TOP_LEVEL_MONGO_URI:
            try:
                from pymongo import MongoClient
                client = MongoClient(TOP_LEVEL_MONGO_URI, serverSelectionTimeoutMS=5000)
                client.admin.command('ping') # Test connection
                db = client[DB_NAME]
                logger.info("Log Purge: Successfully connected to MongoDB using new client.")
            except Exception as e:
                logger.error(f"Log Purge: Failed to create new MongoDB client: {e}")
                return
        else:
            logger.error("Log Purge: MONGO_URI not available, cannot create new client.")
            return
    else: # Use the app's client if available (e.g., if called within app context)
        db = mongo_client_app[DB_NAME]
        client = mongo_client_app # For closing later if we created it above

    op_logs_coll = db[OPERATION_LOG_COLLECTION]
    std_logs_coll = db[LOG_COLLECTION]
    
    collections_to_purge = {
        "operation_logs": op_logs_coll,
        "standard_logs (detailed)": std_logs_coll
    }

    cutoff_date = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=retention_days)
    logger.info(f"Starting log purge. Deleting documents older than {retention_days} days (before {cutoff_date.strftime('%Y-%m-%d %H:%M:%S %Z')}).")

    total_deleted_op_logs = 0
    total_deleted_std_logs = 0

    # Purge Operation Logs (based on 'start_time' or 'end_time')
    try:
        # Try to use 'end_time' first, as completed runs will have it.
        # For 'running' or very old entries without 'end_time', use 'start_time'.
        query_op_logs = {
            "$or": [
                {"end_time": {"$lt": cutoff_date}},
                {"end_time": None, "start_time": {"$lt": cutoff_date}} # Also purge very old 'running' logs
            ]
        }
        result = op_logs_coll.delete_many(query_op_logs)
        total_deleted_op_logs = result.deleted_count
        logger.info(f"Purged {total_deleted_op_logs} documents from '{OPERATION_LOG_COLLECTION}'.")
    except PyMongoError as e:
        logger.error(f"Error purging '{OPERATION_LOG_COLLECTION}': {e}")
    except Exception as e_gen:
        logger.error(f"Unexpected error purging '{OPERATION_LOG_COLLECTION}': {e_gen}", exc_info=True)


    # Purge Standard Detailed Logs (based on 'first_log_time' or 'last_log_time')
    # The 'logs' collection stores an array of messages per run_id.
    # We should delete entire documents where the run is old.
    try:
        # Delete documents where the 'last_log_time' is older than the cutoff,
        # OR if 'last_log_time' doesn't exist but 'first_log_time' is older (for incomplete runs).
        query_std_logs = {
             "$or": [
                {"last_log_time": {"$lt": cutoff_date}},
                {"last_log_time": None, "first_log_time": {"$lt": cutoff_date}}
            ]
        }
        result_std = std_logs_coll.delete_many(query_std_logs)
        total_deleted_std_logs = result_std.deleted_count
        logger.info(f"Purged {total_deleted_std_logs} documents from '{LOG_COLLECTION}'.")
    except PyMongoError as e:
        logger.error(f"Error purging '{LOG_COLLECTION}': {e}")
    except Exception as e_gen:
        logger.error(f"Unexpected error purging '{LOG_COLLECTION}': {e_gen}", exc_info=True)

    logger.info(f"Log purge completed. Total Operation Logs deleted: {total_deleted_op_logs}. Total Standard Log Documents deleted: {total_deleted_std_logs}.")

    # Close client only if we created it in this function
    if 'client' in locals() and client != mongo_client_app:
        try:
            client.close()
            logger.info("Log Purge: Closed new MongoDB client connection.")
        except Exception as e_close:
            logger.error(f"Log Purge: Error closing new MongoDB client: {e_close}")


# --- Core Task Execution Function ---
def run_labbuild_task(args_list):
    """Runs labbuild.py as a subprocess with the given arguments."""
    labbuild_script_path = os.path.join(project_root, 'labbuild.py')
    python_executable = sys.executable # Use the same python that runs Flask
    command = [python_executable, labbuild_script_path] + args_list
    command_str = ' '.join(shlex.quote(arg) for arg in command)
    logger.info(f"Executing background task: {command_str}")
    try:
        # Increased timeout
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False, # Don't raise exception on non-zero exit
            cwd=project_root, # Run from project root
            timeout=7200  # 2 hour timeout
        )
        logger.info(
            f"labbuild task '{' '.join(args_list)}' finished. "
            f"RC: {process.returncode}"
        )
        # Log output, especially on error
        if process.stdout:
             logger.debug(f"labbuild task stdout:\n{process.stdout}")
        if process.returncode != 0 and process.stderr:
            logger.error(f"labbuild task stderr:\n{process.stderr}")
        elif process.returncode != 0:
             logger.error(f"labbuild task failed with RC {process.returncode}, no stderr captured.")

    except subprocess.TimeoutExpired:
        logger.error(
            f"labbuild task timed out after 7200s: {command_str}"
        )
    except FileNotFoundError:
         logger.error(
             f"Error: Python executable '{python_executable}' or script "
             f"'{labbuild_script_path}' not found."
         )
    except Exception as e:
        logger.error(
            f"Failed run labbuild subprocess command '{command_str}': {e}",
            exc_info=True
        )

# --- Process Streaming Function ---
def stream_labbuild_process(full_command_list):
    """Generator runs labbuild.py and yields output line by line for SSE."""
    cmd_str = ' '.join(shlex.quote(arg) for arg in full_command_list)
    logger.info(f"Streaming command: {cmd_str}")
    process = None
    try:
        # Ensure using the correct python executable
        # full_command_list should already start with sys.executable
        process = subprocess.Popen(
            full_command_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Combine stdout/stderr for simplicity
            text=True, encoding='utf-8', errors='replace',
            cwd=project_root, # Run from project root
            bufsize=1  # Line buffered
        )
        yield f"event: start\ndata: Process started (PID: {process.pid})\n\n"
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                # Escape backslashes first, then newlines for SSE
                formatted_line = line.replace('\\', '\\\\').replace('\n', '\\n')
                yield f"data: {formatted_line}\n\n"
                time.sleep(0.01) # Slight pause for browser rendering
            process.stdout.close() # Ensure pipe is closed

        return_code = process.wait() # Wait for process to finish
        logger.info(f"Streaming command finished. RC: {return_code}")
        yield f"event: close\ndata: Process finished with exit code {return_code}\n\n"

    except FileNotFoundError:
        err_msg = f"Error: Command executable '{full_command_list[0]}' or script not found."
        logger.error(err_msg)
        yield f"event: error\ndata: {err_msg}\n\n"
        yield f"event: close\ndata: Process failed (file not found)\n\n"
    except Exception as e:
        err_msg = f"Subprocess error: {e}"
        logger.error(f"Stream error: {e}", exc_info=True)
        yield f"event: error\ndata: {err_msg}\n\n"
        yield f"event: close\ndata: Process failed (exception)\n\n"
    finally:
         # Ensure process is terminated if generator exits unexpectedly
         if process and process.poll() is None:
             try:
                 process.terminate()
                 time.sleep(0.1) # Give terminate a moment
                 if process.poll() is None: # Still running? Force kill.
                    process.kill()
                    logger.warning(f"Killed streaming process {process.pid}")
             except Exception as kill_e:
                 logger.error(f"Error terminating stream process: {kill_e}")

# --- END OF dashboard/tasks.py ---