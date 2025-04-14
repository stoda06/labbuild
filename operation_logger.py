# operation_logger.py
"""Contains the OperationLogger class for structured logging."""

import logging
import sys
import uuid
try:
    import shortuuid
except ImportError:
    shortuuid = None
import datetime
import threading
from typing import Optional, Dict, List, Any

import pymongo
from pymongo import UpdateOne
from pymongo.errors import PyMongoError, ConnectionFailure

# Import constants and MONGO_URI
from constants import DB_NAME, OPERATION_LOG_COLLECTION
try:
    from db_utils import MONGO_URI
except ImportError:
    # Fallback or re-definition
    from dotenv import load_dotenv
    from urllib.parse import quote_plus
    import os
    load_dotenv()
    MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
    MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
    MONGO_HOST = os.getenv("MONGO_HOST")
    if not MONGO_HOST: MONGO_URI = None
    else: MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"

# --- Import the functions to flush logs and clean up ---
from logger.log_config import flush_mongo_handler_for_run, remove_handler_reference

logger = logging.getLogger('labbuild.oplogger')

class OperationLogger:
    """
    Logs structured operation summary to MongoDB's operation_logs.
    Triggers flushing of buffered detailed logs.
    """
    # --- __init__, _connect_db, _write_to_db, _log_initial_start, log_pod_status ---
    # (These methods remain unchanged from the previous BufferingMongoLogHandler version)
    def __init__(self, command: str, args_dict: Dict[str, Any]):
        self.run_id = shortuuid.uuid() if shortuuid else str(uuid.uuid4())
        self.command = command
        safe_args = {k: v for k, v in args_dict.items() if k != 'func'}
        self.args_dict = safe_args
        self.start_time = datetime.datetime.utcnow()
        self._pod_results: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self.client = None
        self.collection = None
        self.connection_failed = False
        # -----> CORRECTED FLAG NAME <-----
        self._is_finalized = False # Initialize flag to False
        self._connect_db()
        if not self.connection_failed: self._log_initial_start()
        else: logger.error(f"OpLog initial start SKIPPED for {self.run_id}: DB connection failure.")


        if not self.connection_failed:
            self._log_initial_start() # Log start only if connected
        else:
            logger.error(f"Operation '{self.command}' (run_id: {self.run_id}) initial start log SKIPPED due to DB connection failure.")

    def _connect_db(self):
        """Establishes the MongoDB connection for *this logger instance* (Operation Logs)."""
        if not MONGO_URI:
             logger.error(f"[OpLogger:{self.run_id}] MongoDB URI not set. Cannot connect.")
             self.connection_failed = True; return
        try:
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            db = self.client[DB_NAME]
            self.collection = db[OPERATION_LOG_COLLECTION]
            logger.debug(f"[OpLogger:{self.run_id}] Connected to MongoDB for Operation Log.")
            self.connection_failed = False
        except (PyMongoError, ConnectionFailure) as e:
            logger.error(f"[OpLogger:{self.run_id}] OpLogger MongoDB connection failed: {e}")
            self.connection_failed = True; self.client = None
        except Exception as e:
             logger.error(f"[OpLogger:{self.run_id}] OpLogger unexpected DB error: {e}")
             self.connection_failed = True; self.client = None

    def _write_to_db(self, document: Optional[Dict] = None, filter_doc: Optional[Dict] = None, update_doc: Optional[Dict] = None):
        """Internal helper to write to the *operation_logs* collection."""
        if self.connection_failed or self.collection is None:
            logger.warning(f"[OpLogger:{self.run_id}] Skipping OpLog DB write - connection failed/not initialized.")
            return False
        try:
            if document is not None:
                 self.collection.insert_one(document)
                 logger.debug(f"[OpLogger:{self.run_id}] Inserted initial OpLog.")
            elif filter_doc is not None and update_doc is not None:
                 result = self.collection.update_one(filter_doc, update_doc)
                 logger.debug(f"[OpLogger:{self.run_id}] Updated final OpLog. Matched: {result.matched_count}, Mod: {result.modified_count}")
            else: logger.error(f"[OpLogger:{self.run_id}] Invalid OpLog DB write args."); return False
            return True
        except PyMongoError as e: logger.error(f"[OpLogger:{self.run_id}] Failed OpLog MongoDB write: {e}"); return False
        except Exception as e: logger.error(f"[OpLogger:{self.run_id}] Unexpected OpLog write error: {e}"); return False

    def _log_initial_start(self):
        """Logs the minimal start document for the operation."""
        start_data = { "run_id": self.run_id, "command": self.command, "args": self.args_dict, "start_time": self.start_time, "end_time": None, "duration_seconds": None, "overall_status": "running", "summary": {"success_count": 0, "failure_count": 0}, "pod_statuses": [] }
        if self._write_to_db(document=start_data):
            logger.info(f"Operation '{self.command}' starting with run_id: {self.run_id}")

    def log_pod_status(self, pod_id: Any, status: str, step: Optional[str] = None, error: Optional[str] = None, class_id: Optional[Any] = None):
        """Stores pod status for OpLog summary and logs errors to main logger."""
        pod_data = { "timestamp": datetime.datetime.utcnow(), "identifier": str(pod_id), "status": status, "failed_step": step, "error_message": error }
        if class_id is not None: pod_data["class_identifier"] = str(class_id)
        with self._lock: self._pod_results.append(pod_data)
        if status == "failed":
            main_logger = logging.getLogger('labbuild') # Get main logger instance
            main_logger.error(f"RunID {self.run_id} - Pod/Class '{pod_id}' failed step '{step}': {error}")


    def finalize(self, overall_status: str, success_count: int, failure_count: int):
        """Updates the OpLog document and triggers flushing of detailed logs."""
        # -----> CORRECTED CHECK <-----
        if self._is_finalized:
            logger.warning(f"[OpLogger:{self.run_id}] Finalize called more than once. Ignoring.")
            return

        end_time = datetime.datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        logger.info(f"[OpLogger:{self.run_id}] Finalizing operation. Attempting to flush detailed logs...")
        flush_success = flush_mongo_handler_for_run(self.run_id, end_time=end_time)
        if flush_success: logger.info(f"[OpLogger:{self.run_id}] Detailed logs flushed successfully.")
        else: logger.error(f"[OpLogger:{self.run_id}] Failed to flush detailed logs for run.")

        log_filter = {'run_id': self.run_id}
        log_update = { "$set": { "end_time": end_time, "duration_seconds": round(duration, 2), "overall_status": overall_status, "summary.success_count": success_count, "summary.failure_count": failure_count, "pod_statuses": self._pod_results } }
        if self._write_to_db(filter_doc=log_filter, update_doc=log_update): logger.info(f"Op '{self.command}' (run_id: {self.run_id}) finalized status: {overall_status}. OpLog updated.")
        else: logger.error(f"Op '{self.command}' (run_id: {self.run_id}) final OpLog update FAILED.")

        self.close_connection() # Close OpLog connection FIRST
        # -----> SET CORRECT FLAG <-----
        self._is_finalized = True # Set flag at the end
        remove_handler_reference(self.run_id) # Clean up handler ref LAST


    def close_connection(self):
        """Closes the MongoDB client connection *for this OpLogger instance*."""
        # --- This function remains unchanged ---
        if self.client:
            try: self.client.close(); logger.debug(f"[OpLogger:{self.run_id}] Closed OpLogger MongoDB connection."); self.client = None; self.collection = None
            except Exception as e: logger.error(f"[OpLogger:{self.run_id}] Error closing OpLogger MongoDB connection: {e}")

# --- END OF FILE operation_logger.py ---