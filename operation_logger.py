# operation_logger.py
"""Contains the OperationLogger class for structured logging."""

import logging
import sys # Import sys for shortuuid fallback check
import uuid
try:
    import shortuuid # Use if available
except ImportError:
    shortuuid = None # Flag not available
import datetime
import threading
from typing import Optional, Dict, List, Any

import pymongo
from pymongo import UpdateOne
from pymongo.errors import PyMongoError, ConnectionFailure

# Import constants and MONGO_URI
from constants import DB_NAME, OPERATION_LOG_COLLECTION
# Assuming MONGO_URI is defined in db_utils or constants, or directly here
# If MONGO_URI is needed, import it:
try:
    from db_utils import MONGO_URI
except ImportError:
    # Fallback or re-definition if db_utils doesn't exist or causes circular import
    from dotenv import load_dotenv
    from urllib.parse import quote_plus
    import os
    load_dotenv()
    MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
    MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
    MONGO_HOST = os.getenv("MONGO_HOST")
    if not MONGO_HOST:
        MONGO_URI = None
    else:
        MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"


logger = logging.getLogger('labbuild.oplogger')

class OperationLogger:
    """
    Logs structured operation and pod status to MongoDB.
    Writes an initial 'running' document, then updates it upon finalization.
    Manages its own persistent MongoDB client connection.
    """
    def __init__(self, command: str, args_dict: Dict[str, Any]):
        self.run_id = shortuuid.uuid() if shortuuid else str(uuid.uuid4()) # Use shortuuid if available
        self.command = command
        safe_args = args_dict.copy()
        self.args_dict = safe_args
        self.start_time = datetime.datetime.utcnow()
        self._pod_results: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

        # --- Connection Management ---
        self.client = None
        self.collection = None
        self.connection_failed = False
        self._connect_db() # Attempt connection on init
        # --- End Connection Management ---

        if not self.connection_failed:
            self._log_initial_start() # Log start only if connected
        else:
            logger.error(f"Operation '{self.command}' (run_id: {self.run_id}) initial start log SKIPPED due to DB connection failure.")

    def _connect_db(self):
        """Establishes the MongoDB connection for this logger instance."""
        if not MONGO_URI:
             logger.error(f"[OpLogger:{self.run_id}] MongoDB URI not set. Cannot connect.")
             self.connection_failed = True
             return
        try:
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping') # Verify connection
            db = self.client[DB_NAME]
            self.collection = db[OPERATION_LOG_COLLECTION]
            logger.debug(f"[OpLogger:{self.run_id}] Connected to MongoDB for logging.")
            self.connection_failed = False
        except (PyMongoError, ConnectionFailure) as e: # Catch ConnectionFailure here
            logger.error(f"[OpLogger:{self.run_id}] MongoDB connection failed on init: {e}")
            self.connection_failed = True
            if self.client: self.client.close(); self.client = None # Ensure closed on failure
        except Exception as e:
             logger.error(f"[OpLogger:{self.run_id}] Unexpected error connecting DB on init: {e}")
             self.connection_failed = True
             if self.client: self.client.close(); self.client = None

    def _write_to_db(self, document: Optional[Dict] = None, filter_doc: Optional[Dict] = None, update_doc: Optional[Dict] = None):
        """Internal helper to perform insert or update using the instance's connection."""
        if self.connection_failed or self.collection is None:
            logger.warning(f"[OpLogger:{self.run_id}] Skipping DB write - connection failed or not initialized.")
            return False

        try:
            if document is not None:
                 # --- Use self.collection ---
                 self.collection.insert_one(document)
                 # --- End Change ---
                 logger.debug(f"[OpLogger:{self.run_id}] Inserted initial log.")
            elif filter_doc is not None and update_doc is not None:
                 # --- Use self.collection ---
                 result = self.collection.update_one(filter_doc, update_doc)
                 # --- End Change ---
                 logger.debug(f"[OpLogger:{self.run_id}] Updated final log. Matched: {result.matched_count}, Mod: {result.modified_count}")
            else: logger.error(f"[OpLogger:{self.run_id}] Invalid DB write args."); return False
            return True
        except PyMongoError as e: logger.error(f"[OpLogger:{self.run_id}] Failed MongoDB write operation: {e}"); return False
        except Exception as e: logger.error(f"[OpLogger:{self.run_id}] Unexpected MongoDB write error: {e}"); return False
        # No finally block needed here

    def _log_initial_start(self):
        """Logs the minimal start document for the operation."""
        start_data = {"run_id": self.run_id, "command": self.command, "args": self.args_dict, "start_time": self.start_time, "end_time": None, "duration_seconds": None, "overall_status": "running", "summary": {"success_count": 0, "failure_count": 0}, "pod_statuses": []}
        if self._write_to_db(document=start_data): logger.info(f"Operation '{self.command}' starting with run_id: {self.run_id}")
        # Error logged by _write_to_db if it fails

    def log_pod_status(self, pod_id: Any, status: str, step: Optional[str] = None, error: Optional[str] = None, class_id: Optional[Any] = None):
        """Stores the success or failure status of a single pod/class internally."""
        pod_data = {"timestamp": datetime.datetime.utcnow(), "identifier": str(pod_id), "status": status, "failed_step": step, "error_message": error}
        if class_id is not None: pod_data["class_identifier"] = str(class_id)
        with self._lock: self._pod_results.append(pod_data)
        if status == "failed": logger.error(f"RunID {self.run_id} - Pod/Class '{pod_id}' failed step '{step}': {error}")

    def finalize(self, overall_status: str, success_count: int, failure_count: int):
        """Updates the existing log document with final status and buffered pod results."""
        end_time = datetime.datetime.utcnow(); duration = (end_time - self.start_time).total_seconds()
        log_filter = {'run_id': self.run_id}
        log_update = {"$set": {"end_time": end_time, "duration_seconds": round(duration, 2), "overall_status": overall_status, "summary.success_count": success_count, "summary.failure_count": failure_count, "pod_statuses": self._pod_results}}
        if self._write_to_db(filter_doc=log_filter, update_doc=log_update): logger.info(f"Op '{self.command}' (run_id: {self.run_id}) finalized status: {overall_status}. Log updated.")
        else: logger.error(f"Op '{self.command}' (run_id: {self.run_id}) final log update FAILED.")
        self.close_connection() # Close connection on finalize

    def close_connection(self):
        """Closes the MongoDB client connection if it's open."""
        if self.client:
            try:
                self.client.close(); logger.debug(f"[OpLogger:{self.run_id}] Closed persistent MongoDB connection."); self.client = None; self.collection = None
            except Exception as e: logger.error(f"[OpLogger:{self.run_id}] Error closing persistent MongoDB connection: {e}")