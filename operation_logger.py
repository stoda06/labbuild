# --- START OF FILE operation_logger.py ---

# operation_logger.py
"""Contains the OperationLogger class for structured logging."""

import logging
import sys
import uuid
try: import shortuuid
except ImportError: shortuuid = None
import datetime
import threading
from typing import Optional, Dict, List, Any

import pymongo
from pymongo import UpdateOne
from pymongo.errors import PyMongoError, ConnectionFailure

# Import constants and MONGO_URI
from constants import DB_NAME, OPERATION_LOG_COLLECTION
try: from db_utils import MONGO_URI
except ImportError: # Fallback if db_utils isn't available at import time (e.g., during init)
    from dotenv import load_dotenv; from urllib.parse import quote_plus; import os
    load_dotenv(); MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user")); MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF")); MONGO_HOST = os.getenv("MONGO_HOST")
    if not MONGO_HOST: MONGO_URI = None
    else: MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"

# Import the functions to flush logs and clean up
try: from logger.log_config import flush_mongo_handler_for_run, remove_handler_reference
except ImportError:
    print("ERROR: Could not import log flushing functions from logger.log_config", file=sys.stderr)
    # Define dummy functions if import fails, allowing OperationLogger to mostly work
    def flush_mongo_handler_for_run(run_id, end_time=None): return False
    def remove_handler_reference(run_id): pass

logger = logging.getLogger('labbuild.oplogger') # Use specific logger

class OperationLogger:
    """
    Logs structured operation summary to MongoDB's operation_logs.
    Triggers flushing of buffered detailed logs.
    """
    def __init__(self, command: str, args_dict: Dict[str, Any]):
        """
        Initializes the OperationLogger.

        Args:
            command (str): The command being executed (e.g., 'setup', 'manage').
            args_dict (Dict[str, Any]): The arguments passed to the command.
        """
        self.run_id = shortuuid.uuid() if shortuuid else str(uuid.uuid4())
        self.command = command
        # Filter out any function references or potentially problematic types from args
        safe_args = self._sanitize_args(args_dict)
        self.args_dict = safe_args
        self.start_time = datetime.datetime.utcnow()
        self._pod_results: List[Dict[str, Any]] = []
        self._lock = threading.Lock() # Lock for accessing _pod_results
        self.client = None
        self.collection = None
        self.connection_failed = False # Track connection state
        self._is_finalized = False # Flag to prevent multiple finalizations
        self._connect_db() # Attempt initial connection
        if not self.connection_failed:
            self._log_initial_start()
        else:
            logger.error(f"OpLog initial start SKIPPED for {self.run_id}: DB connection failure.")

    def _sanitize_args(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Removes non-serializable items (like functions) from args for logging."""
        safe_args = {}
        for k, v in args_dict.items():
            if k == 'func': continue # Skip function reference from argparse
            # Add more checks here if other non-serializable types might exist
            if isinstance(v, (str, int, float, bool, list, dict, type(None))):
                safe_args[k] = v
            else:
                # Log skipped arg and try converting to string as fallback
                logger.warning(f"Sanitizing arg '{k}': Type {type(v)} not directly serializable, converting to string.")
                try: safe_args[k] = str(v)
                except Exception: logger.error(f"Could not convert arg '{k}' to string.")
        return safe_args


    def _connect_db(self):
        """Establishes the MongoDB connection for *this logger instance* (Operation Logs)."""
        if self.client and self.collection: return # Already connected
        if self.connection_failed: return # Don't retry if previously marked as failed indefinitely

        if not MONGO_URI:
            self.connection_failed = True
            logger.error(f"[OpLogger:{self.run_id}] MongoDB URI not set. Cannot connect.")
            return

        try:
            logger.debug(f"[OpLogger:{self.run_id}] Attempting OpLog DB connection...")
            # Increased timeout slightly
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
            # The ismaster command is cheap and does not require auth.
            self.client.admin.command('ismaster')
            db = self.client[DB_NAME]
            self.collection = db[OPERATION_LOG_COLLECTION]
            logger.info(f"[OpLogger:{self.run_id}] Connected to MongoDB for Operation Log.")
            self.connection_failed = False # Reset flag on successful connect
        except (PyMongoError, ConnectionFailure) as e:
            logger.error(f"[OpLogger:{self.run_id}] OpLogger MongoDB connection failed: {e}")
            self.connection_failed = True
            self.client = None
            self.collection = None
        except Exception as e: # Catch other potential errors
             logger.error(f"[OpLogger:{self.run_id}] OpLogger unexpected DB error on connect: {e}", exc_info=True)
             self.connection_failed = True
             self.client = None
             self.collection = None

    def _write_to_db(self, document: Optional[Dict] = None, filter_doc: Optional[Dict] = None, update_doc: Optional[Dict] = None):
        """Internal helper to write to the *operation_logs* collection with error handling."""
        if self.connection_failed or self.collection is None:
            logger.warning(f"[OpLogger:{self.run_id}] Cannot write OpLog - connection unavailable.")
            return False
        try:
            if document is not None:
                # Insert initial document
                self.collection.insert_one(document)
                logger.debug(f"[OpLogger:{self.run_id}] Inserted initial OpLog document.")
            elif filter_doc is not None and update_doc is not None:
                # Update existing document (e.g., finalize)
                result = self.collection.update_one(filter_doc, update_doc)
                if result.matched_count == 0:
                     logger.warning(f"[OpLogger:{self.run_id}] Final OpLog update did not match any document for filter: {filter_doc}")
                else:
                     logger.debug(f"[OpLogger:{self.run_id}] Updated final OpLog status. Matched={result.matched_count}, Modified={result.modified_count}")
            else:
                 logger.error(f"[OpLogger:{self.run_id}] Invalid arguments provided for OpLog DB write operation.")
                 return False
            return True
        except PyMongoError as e:
            logger.error(f"[OpLogger:{self.run_id}] Failed OpLog MongoDB write operation: {e}", exc_info=True)
            # Consider if connection should be marked failed here
            # self.connection_failed = True
            # self.close_connection() # Maybe close potentially broken connection
            return False
        except Exception as e:
             logger.error(f"[OpLogger:{self.run_id}] Unexpected error during OpLog write: {e}", exc_info=True)
             # self.connection_failed = True
             # self.close_connection()
             return False

    def _log_initial_start(self):
        """Logs the minimal start document for the operation."""
        start_data = {
            "run_id": self.run_id,
            "command": self.command,
            "args": self.args_dict,
            "start_time": self.start_time,
            "end_time": None, # Mark as null initially
            "duration_seconds": None, # Mark as null initially
            "overall_status": "running", # Initial status
            "summary": {"success_count": 0, "failure_count": 0},
            "pod_statuses": [] # Initialize as empty array
        }
        if self._write_to_db(document=start_data):
            logger.info(f"Operation '{self.command}' starting with run_id: {self.run_id}")
        else:
            # If initial write fails, log error but allow script to continue if possible
             logger.error(f"[OpLogger:{self.run_id}] FAILED to write initial operation log to database!")


    def log_pod_status(self, pod_id: Any, status: str, step: Optional[str] = None, error: Optional[str] = None, class_id: Optional[Any] = None):
        """
        Stores status updates for individual pods/classes during the operation.
        Logs errors immediately to the main application logger.

        Args:
            pod_id (Any): Identifier for the pod (e.g., pod number).
            status (str): The status ('success', 'failed', 'skipped').
            step (Optional[str]): The step where failure occurred (if status is 'failed').
            error (Optional[str]): Error message (if status is 'failed').
            class_id (Optional[Any]): Identifier for the class (e.g., F5 class number).
        """
        pod_data = {
            "timestamp": datetime.datetime.utcnow(),
            "identifier": str(pod_id), # Ensure string conversion
            "status": status,
            "failed_step": step,
            "error_message": error
        }
        if class_id is not None:
            pod_data["class_identifier"] = str(class_id) # Ensure string conversion

        with self._lock:
            self._pod_results.append(pod_data)

        # Log failures immediately to the main logger for visibility
        if status == "failed":
            error_context = f"RunID {self.run_id} - "
            if class_id is not None:
                 error_context += f"Class '{class_id}' "
            error_context += f"Pod/Item '{pod_id}' failed"
            if step: error_context += f" at step '{step}'"
            error_context += f": {error}"
            logging.getLogger('labbuild').error(error_context) # Use main logger


    def finalize(self, overall_status: str, success_count: int, failure_count: int):
        """
        Updates the operation log document with the final status, counts, duration,
        and pod statuses. Also triggers flushing of detailed logs.
        """
        # --- HIGHLIGHTED MODIFICATION: Prevent double finalization ---
        # Use self._is_finalized flag check - CORRECTED
        if self._is_finalized:
            logger.warning(f"[OpLogger:{self.run_id}] Finalize called more than once. Ignoring subsequent call.")
            return

        end_time = datetime.datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        # --- 1. Flush Detailed Logs ---
        logger.info(f"[OpLogger:{self.run_id}] Finalizing run. Attempting to flush detailed logs...")
        try:
            flush_success = flush_mongo_handler_for_run(self.run_id, end_time=end_time)
            if flush_success:
                logger.info(f"[OpLogger:{self.run_id}] Detailed logs flushed successfully.")
            else:
                logger.error(f"[OpLogger:{self.run_id}] Failed to flush detailed logs (handler might be missing or failed).")
        except Exception as flush_exc:
             logger.error(f"[OpLogger:{self.run_id}] Exception during detailed log flush: {flush_exc}", exc_info=True)

        # --- 2. Update Operation Summary Log ---
        logger.info(f"[OpLogger:{self.run_id}] Updating final operation status to '{overall_status}' in OpLog database...")
        log_filter = {'run_id': self.run_id}
        # Use lock to safely access _pod_results
        with self._lock:
            pod_statuses_to_write = self._pod_results[:] # Shallow copy

        log_update = {
            "$set": {
                "end_time": end_time,
                "duration_seconds": round(duration, 2),
                "overall_status": overall_status,
                "summary.success_count": success_count,
                "summary.failure_count": failure_count,
                "pod_statuses": pod_statuses_to_write # Add the collected pod statuses
            }
        }

        # --- Explicitly check/reconnect OpLogger's connection BEFORE final write ---
        oplog_update_success = False
        if self.connection_failed or self.collection is None:
             logger.warning(f"[OpLogger:{self.run_id}] Connection lost before final OpLog update. Attempting reconnect...")
             self._connect_db() # Try to re-establish connection

        # Now attempt the write if connection is okay
        if not self.connection_failed and self.collection is not None:
            if self._write_to_db(filter_doc=log_filter, update_doc=log_update):
                logger.info(f"Operation '{self.command}' (run_id: {self.run_id}) finalized. Status: {overall_status}. OpLog DB updated.")
                oplog_update_success = True
            else:
                # _write_to_db already logged the error
                logger.error(f"Operation '{self.command}' (run_id: {self.run_id}) final OpLog DB status update FAILED. Status might remain 'running'.")
        else:
             logger.error(f"Operation '{self.command}' (run_id: {self.run_id}) final OpLog DB status update SKIPPED - connection unavailable.")

        # --- 3. Set finalized flag ---
        # Set flag regardless of DB update success to prevent repeated attempts
        self._is_finalized = True
        logger.debug(f"[OpLogger:{self.run_id}] Finalized flag set.")

        # --- 4. Close OpLogger's connection ---
        self.close_connection()

        # --- 5. Clean up detailed log handler reference from global dict ---
        remove_handler_reference(self.run_id)
        logger.debug(f"[OpLogger:{self.run_id}] Removed detailed log handler reference.")


    def close_connection(self):
        """Closes the MongoDB client connection *for this OpLogger instance*."""
        if self.client:
            try:
                self.client.close()
                logger.debug(f"[OpLogger:{self.run_id}] Closed OpLogger MongoDB connection.")
            except Exception as e:
                logger.error(f"[OpLogger:{self.run_id}] Error closing OpLogger MongoDB connection: {e}")
            finally:
                 # Ensure references are cleared even if close fails
                 self.client = None
                 self.collection = None
                 # Mark as disconnected after close attempt
                 self.connection_failed = True


# --- END OF FILE operation_logger.py ---