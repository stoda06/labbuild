# --- START OF logger/log_config.py ---

import logging
import sys
import os
from urllib.parse import quote_plus
import pymongo
from pymongo.errors import PyMongoError, ConnectionFailure
from dotenv import load_dotenv
import datetime
import atexit
import traceback
import threading

# --- Import the new handler ---
from .redis_handler import RedisStreamHandler

# --- MongoDB Configuration ---
load_dotenv()
MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
MONGO_HOST = os.getenv("MONGO_HOST")
DB_NAME = "labbuild_db"
LOG_COLLECTION = "logs" # For BufferingMongoLogHandler

MONGO_URI = None
if MONGO_HOST:
    MONGO_URI = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"
        "?serverSelectionTimeoutMS=5000"
    )
else:
    print("WARNING: MONGO_HOST not set. MongoDB logging disabled.",
          file=sys.stderr)

# --- Redis Configuration (Read directly for the handler) ---
REDIS_URL_CONFIG = os.getenv("REDIS_URL")
if not REDIS_URL_CONFIG:
     print("WARNING: REDIS_URL not set. Real-time log streaming disabled.",
           file=sys.stderr)


# --- BufferingMongoLogHandler Class (Unchanged - Keep as before) ---
# Global dictionary to store BufferingMongoLogHandler instances per run_id
_run_handlers = {}
_handler_lock = threading.Lock() # Lock for accessing the dictionary

class BufferingMongoLogHandler(logging.Handler):
    """
    Custom log handler that buffers logs for a specific run_id
    and flushes them to a single MongoDB document upon request.
    """
    def __init__(self, level=logging.NOTSET, mongo_uri=MONGO_URI,
                 db_name=DB_NAME, collection_name=LOG_COLLECTION, run_id=None):
        super().__init__(level)
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.run_id = run_id
        self.buffer = []
        self._lock = threading.Lock() # Lock for buffer access
        self.connection_failed = False # Tracks connection state
        self.client = None
        self.collection = None
        self.connect_lock = threading.Lock() # Lock for connection logic

        if not self.mongo_uri or not self.run_id:
            self.connection_failed = True
            print(f"ERROR: BufferingMongoLogHandler disabled for run {self.run_id}. "
                  "Missing URI or run_id.", file=sys.stderr)

    def _connect(self):
        """Connects to MongoDB if not already connected. Returns True on success."""
        if self.collection is not None: return True
        if self.connection_failed: return False

        with self.connect_lock:
            # Double check after acquiring lock
            if self.collection is not None: return True
            if self.connection_failed: return False

            print(f"DEBUG: BufferingMongoLogHandler attempting connection "
                  f"for run_id {self.run_id}...")
            try:
                self.client = pymongo.MongoClient(self.mongo_uri)
                self.client.admin.command('ismaster') # Check connection
                db = self.client[self.db_name]
                self.collection = db[self.collection_name]
                print(f"DEBUG: BufferingMongoLogHandler CONNECTED "
                      f"for run_id {self.run_id}")
                return True
            except (ConnectionFailure, PyMongoError) as e:
                self.connection_failed = True # Mark as failed
                print(f"ERROR: BufferingMongoLogHandler connection FAILED "
                      f"for run {self.run_id}: {e}", file=sys.stderr)
                if self.client:
                    try: self.client.close()
                    except Exception: pass
                self.client = None; self.collection = None
                return False
            except Exception as e:
                self.connection_failed = True
                print(f"ERROR: Unexpected error connecting BufferingMongoLogHandler "
                      f"run {self.run_id}: {e}", file=sys.stderr)
                if self.client:
                    try: self.client.close()
                    except Exception: pass
                self.client = None; self.collection = None
                return False

    def emit(self, record):
        """Appends the formatted log record to the internal buffer."""
        try:
            log_entry = {
                "timestamp": datetime.datetime.utcnow(),
                "level": record.levelname, "levelno": record.levelno,
                "message": self.format(record), # Use formatter
                "logger_name": record.name,
                "pathname": record.pathname, "filename": record.filename,
                "module": record.module, "lineno": record.lineno,
                "funcName": record.funcName,
                "process": record.process, "thread": record.thread,
                "threadName": record.threadName,
            }
            if record.exc_info:
                 try:
                     log_entry['exc_text'] = "".join(
                         traceback.format_exception(*record.exc_info)
                     )
                 except Exception as format_exc:
                     log_entry['exc_text'] = f"Error formatting exception: {format_exc}"
            with self._lock:
                self.buffer.append(log_entry)
        except Exception as e:
            print(f"ERROR: Failed to buffer log record: {e}", file=sys.stderr)
            self.handleError(record)

    def flush_logs(self, end_time=None):
        """Writes the buffered logs to the MongoDB document for the run_id."""
        logs_to_flush = []
        with self._lock:
            if not self.buffer: return True # Nothing to flush
            logs_to_flush = self.buffer[:] # Copy buffer
            self.buffer.clear() # Clear original buffer
        if not logs_to_flush: return True

        if not self._connect():
            print(f"ERROR: Cannot flush logs for run {self.run_id}, "
                  "MongoDB connection failed.", file=sys.stderr)
            # Put logs back in buffer if connection failed
            with self._lock:
                self.buffer = logs_to_flush + self.buffer
            return False

        try:
            # Prepare the base update operation
            set_on_insert_dict = {
                'run_id': self.run_id,
                'first_log_time': logs_to_flush[0]['timestamp']
            }
            set_dict = {
                'last_log_time': logs_to_flush[-1]['timestamp']
            }
            if end_time:
                set_dict['end_time'] = end_time # Add end_time if provided

            update_operation = {
                '$push': {'messages': {'$each': logs_to_flush}},
                '$setOnInsert': set_on_insert_dict,
                '$set': set_dict,
                '$inc': {'log_count': len(logs_to_flush)}
            }
            result = self.collection.update_one(
                {'run_id': self.run_id},
                update_operation,
                upsert=True
            )
            print(f"DEBUG: Flushed {len(logs_to_flush)} logs for run {self.run_id}. "
                  f"UpsertedId: {result.upserted_id}, Matched: {result.matched_count}, "
                  f"Modified: {result.modified_count}")
            return True
        except PyMongoError as e:
            print(f"ERROR: Failed writing buffered logs to MongoDB "
                  f"for run {self.run_id}: {e}", file=sys.stderr)
            with self._lock: self.buffer = logs_to_flush + self.buffer # Re-add on failure
            return False
        except Exception as e:
             print(f"ERROR: Unexpected error flushing logs "
                   f"for run {self.run_id}: {e}", file=sys.stderr)
             with self._lock: self.buffer = logs_to_flush + self.buffer # Re-add on failure
             return False

    def close(self):
        """Flushes any remaining logs and closes the connection."""
        self.flush_logs()
        client_to_close = None
        with self.connect_lock:
             if self.client:
                 client_to_close = self.client
                 self.client = None
                 self.collection = None
                 self.connection_failed = True # Mark as disconnected
        if client_to_close:
            try:
                client_to_close.close()
            except Exception as e:
                print(f"ERROR: Exception closing MongoClient for run {self.run_id}: {e}",
                      file=sys.stderr)
        super().close()


# --- Management Functions ---
def remove_handler_reference(run_id):
     """Removes the BufferingMongoLogHandler reference for a run_id."""
     with _handler_lock:
         handler = _run_handlers.pop(run_id, None)

def flush_mongo_handler_for_run(run_id, end_time=None):
    """Flushes the BufferingMongoLogHandler for a specific run_id."""
    handler_instance = None
    with _handler_lock:
        handler_instance = _run_handlers.get(run_id)
    if handler_instance and isinstance(handler_instance, BufferingMongoLogHandler):
        return handler_instance.flush_logs(end_time=end_time)
    else:
        print(f"WARNING: No active BufferingMongoLogHandler found "
              f"for run_id {run_id} to flush.")
        return False

def _cleanup_all_handlers():
    """Cleans up BufferingMongo and RedisStream handlers on exit."""
    print("DEBUG: atexit cleanup: Cleaning up logging handlers...")

    # 1. Close BufferingMongo Handlers from global dict
    print(f"DEBUG: Closing BufferingMongo handlers: {list(_run_handlers.keys())}")
    with _handler_lock: mongo_items = list(_run_handlers.items())
    closed_mongo_ids = set()
    for run_id, handler in mongo_items:
        try:
            if handler: handler.close()
            closed_mongo_ids.add(run_id)
        except Exception as e:
            print(f"ERROR: Exception during atexit mongo handler close "
                  f"for run {run_id}: {e}")
    # Remove closed handlers from the global dict
    with _handler_lock:
        for run_id in closed_mongo_ids:
            if run_id in _run_handlers: del _run_handlers[run_id]

    # 2. Close any attached RedisStream Handlers by iterating loggers
    print("DEBUG: Checking loggers for attached RedisStream handlers...")
    loggers_to_check = list(logging.Logger.manager.loggerDict.keys())
    # Include the root logger
    loggers_to_check.append(None) # None represents the root logger

    for logger_name in loggers_to_check:
        logger_instance = logging.getLogger(logger_name)
        # Ensure it's a real logger, not a placeholder
        if not isinstance(logger_instance, logging.PlaceHolder):
            for handler in list(logger_instance.handlers): # Iterate copy
                if isinstance(handler, RedisStreamHandler):
                    run_id_redis = getattr(handler, 'run_id', 'unknown')
                    logger_id = logger_name if logger_name else "root"
                    print(f"DEBUG: Closing RedisStreamHandler for run {run_id_redis} "
                          f"found on logger '{logger_id}'")
                    try:
                        handler.close()
                        # Attempt to remove the handler cleanly
                        logger_instance.removeHandler(handler)
                    except Exception as e:
                        print(f"ERROR: Exception during atexit redis handler close "
                              f"for run {run_id_redis} on logger '{logger_id}': {e}")

    print("DEBUG: Logging handler cleanup finished.")

atexit.register(_cleanup_all_handlers)


# --- Setup Logger ---
def setup_logger(run_id=None):
    """Sets up the main application logger ('labbuild') with handlers."""
    logger = logging.getLogger('labbuild')
    if not logger.level or logger.level > logging.DEBUG:
        logger.setLevel(logging.DEBUG) # Capture everything

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s [%(levelname)s] %(message)s', # Slightly refined format
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # --- Ensure only one set of handlers per logger instance ---
    # Remove existing handlers of the types we are adding to avoid duplication
    # This is safer if setup_logger might be called multiple times
    for handler in list(logger.handlers):
        if isinstance(handler, (logging.StreamHandler,
                                BufferingMongoLogHandler, RedisStreamHandler)):
             try:
                  # Attempt to close before removing, especially file/network handlers
                  handler.close()
             except Exception:
                  pass # Ignore errors during cleanup closing
             logger.removeHandler(handler)
    # Reset the global dict entry if logger is reconfigured without a run_id
    if run_id is None:
         with _handler_lock:
              # Clean up any previous run_id associations if run_id is now None
              _run_handlers.clear() # Or selectively remove based on logger name?

    # 1. Stream Handler (Console Output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO) # Console typically shows INFO+
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # 2. Buffering Mongo Handler (Historical Storage)
    if run_id and MONGO_URI:
        with _handler_lock:
            # Ensure no handler exists for this run_id before adding
            if run_id not in _run_handlers:
                mongo_handler = BufferingMongoLogHandler(
                    level=logging.DEBUG, run_id=run_id
                )
                if not mongo_handler.connection_failed:
                    mongo_handler.setFormatter(formatter)
                    logger.addHandler(mongo_handler)
                    _run_handlers[run_id] = mongo_handler
                else:
                    logger.warning(f"Failed to create BufferingMongoLogHandler "
                                   f"for run {run_id}.")
                    mongo_handler.close() # Cleanup failed handler
            # If handler exists, it should have been added/kept above

    # 3. Redis Stream Handler (Real-time Streaming)
    if run_id and REDIS_URL_CONFIG:
        # Check if already attached (less likely with cleanup, but safe)
        has_redis_handler = any(
            isinstance(h, RedisStreamHandler) and getattr(h, 'run_id', None) == run_id
            for h in logger.handlers
        )
        if not has_redis_handler:
            redis_handler = RedisStreamHandler(run_id=run_id, level=logging.DEBUG)
            if redis_handler.redis_client is not None: # Check connection success
                 redis_handler.setFormatter(formatter)
                 logger.addHandler(redis_handler)
                 # logger.info(f"Redis log streaming ENABLED for run_id: {run_id}")
            else:
                 logger.warning(f"Failed to create RedisStreamHandler for run {run_id}.")
                 redis_handler.close() # Cleanup failed handler

    # Configure propagation and sub-loggers
    logger.propagate = False # Prevent root logger duplication
    loggers_to_configure = [
        'labbuild.commands', 'labbuild.orchestrator', 'labbuild.db',
        'labbuild.config', 'labbuild.vcenter', 'labbuild.listing',
        'labbuild.utils', 'labbuild.oplogger', 'labbuild.vmops',
        'VmManager', 'NetworkManager', 'ResourcePoolManager',
        'FolderManager', 'PermissionManager', 'DatastoreManager', # Added managers
        'PRTGManager', 'labbuild.redis_handler', # Added handler logger
    ]
    for name in loggers_to_configure:
        sub_logger = logging.getLogger(name)
        sub_logger.setLevel(logging.DEBUG) # Ensure sub-loggers capture DEBUG
        sub_logger.propagate = True # Propagate to 'labbuild' logger
        # Clear potentially duplicated handlers from previous runs if any
        for h in list(sub_logger.handlers):
            sub_logger.removeHandler(h)

    return logger

# --- END OF logger/log_config.py ---