# --- START OF FILE logger/log_config.py ---

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

# --- MongoDB Configuration ---
load_dotenv()
MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
MONGO_HOST = os.getenv("MONGO_HOST")
DB_NAME = "labbuild_db"
# Collection now stores one document per run_id, containing an array of messages
LOG_COLLECTION = "logs"

MONGO_URI = None
if MONGO_HOST:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}?serverSelectionTimeoutMS=5000"
else:
    print("WARNING: MONGO_HOST not set. MongoDB logging disabled.", file=sys.stderr)

# === DEFINE GLOBALS AND CLASS *BEFORE* FUNCTIONS THAT USE THEM ===

_run_handlers = {} # Dictionary to store {run_id: handler_instance}
_handler_lock = threading.Lock() # Lock for accessing the dictionary

class BufferingMongoLogHandler(logging.Handler):
    """
    Custom log handler that buffers logs for a specific run_id
    and flushes them to a single MongoDB document upon request.
    """
    def __init__(self, level=logging.NOTSET, mongo_uri=MONGO_URI, db_name=DB_NAME, collection_name=LOG_COLLECTION, run_id=None):
        super().__init__(level)
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.run_id = run_id
        self.buffer = []
        self._lock = threading.Lock() # Lock for buffer access
        self.connection_failed = False # Tracks if connection setup ever failed
        self.client = None
        self.collection = None
        self.connect_lock = threading.Lock() # Lock for connection logic

        if not self.mongo_uri or not self.run_id:
            self.connection_failed = True
            print(f"ERROR: BufferingMongoLogHandler disabled for run {self.run_id}. Missing URI or run_id.", file=sys.stderr)
            # No connection attempt needed if config is missing

    def _connect(self):
        """Connects to MongoDB if not already connected. Returns True on success."""
        # Use explicit check for None
        if self.collection is not None: return True # <--- CORRECT CHECK
        if self.connection_failed: return False

        with self.connect_lock:
            # Double check after acquiring lock
            if self.collection is not None: return True # <--- CORRECT CHECK
            if self.connection_failed: return False

            print(f"DEBUG: BufferingMongoLogHandler attempting connection for run_id {self.run_id}...")
            try:
                self.client = pymongo.MongoClient(self.mongo_uri) # Rely on URI timeout
                self.client.admin.command('ismaster')
                db = self.client[self.db_name]
                self.collection = db[self.collection_name]
                print(f"DEBUG: BufferingMongoLogHandler CONNECTED for run_id {self.run_id}")
                return True
            except (ConnectionFailure, PyMongoError) as e:
                self.connection_failed = True # Mark as failed
                print(f"ERROR: BufferingMongoLogHandler connection FAILED for run {self.run_id}: {e}", file=sys.stderr)
                if self.client: 
                    try: self.client.close() 
                    except Exception: pass
                self.client = None; self.collection = None; self.db = None
                return False
            except Exception as e:
                self.connection_failed = True
                print(f"ERROR: Unexpected error connecting BufferingMongoLogHandler run {self.run_id}: {e}", file=sys.stderr)
                if self.client: 
                    try: self.client.close() 
                    except Exception: pass
                self.client = None; self.collection = None; self.db = None
                return False

    def emit(self, record):
        """Appends the formatted log record to the internal buffer."""
        try:
            log_entry = {
                "timestamp": datetime.datetime.utcnow(), "level": record.levelname, "levelno": record.levelno,
                "message": record.getMessage(), "logger_name": record.name, "pathname": record.pathname,
                "filename": record.filename, "module": record.module, "lineno": record.lineno,
                "funcName": record.funcName, "process": record.process, "thread": record.thread,
                "threadName": record.threadName,
            }
            if record.exc_info:
                 try: log_entry['exc_text'] = "".join(traceback.format_exception(*record.exc_info))
                 except Exception as format_exc: log_entry['exc_text'] = f"Error formatting exception: {format_exc}"
            with self._lock: self.buffer.append(log_entry)
        except Exception as e: print(f"ERROR: Failed to buffer log record: {e}", file=sys.stderr); self.handleError(record)

    def flush_logs(self, end_time=None):
        """Writes the buffered logs to the MongoDB document for the run_id."""
        logs_to_flush = []
        with self._lock:
            if not self.buffer: return True
            logs_to_flush = self.buffer[:]; self.buffer.clear()
        if not logs_to_flush: return True

        if not self._connect():
            print(f"ERROR: Cannot flush logs for run {self.run_id}, MongoDB connection failed.", file=sys.stderr)
            with self._lock: self.buffer = logs_to_flush + self.buffer; return False

        try:
            # Prepare the base update operation
            set_on_insert_dict = {
                'run_id': self.run_id,
                'first_log_time': logs_to_flush[0]['timestamp']
            }
            set_dict = {
                'last_log_time': logs_to_flush[-1]['timestamp']
            }
            # Add end_time to $set if provided
            if end_time:
                set_dict['end_time'] = end_time

            update_operation = {
                '$push': {'messages': {'$each': logs_to_flush}},
                '$setOnInsert': set_on_insert_dict,
                '$set': set_dict,
                '$inc': {'log_count': len(logs_to_flush)}
            }

            # Perform the update operation with upsert=True
            result = self.collection.update_one(
                {'run_id': self.run_id},
                update_operation,
                upsert=True
            )
            print(f"DEBUG: Flushed {len(logs_to_flush)} logs for run {self.run_id}. UpsertedId: {result.upserted_id}, Matched: {result.matched_count}, Modified: {result.modified_count}")
            return True
        except PyMongoError as e:
            print(f"ERROR: Failed writing buffered logs to MongoDB for run {self.run_id}: {e}", file=sys.stderr)
            with self._lock: self.buffer = logs_to_flush + self.buffer
            return False
        except Exception as e:
             print(f"ERROR: Unexpected error flushing logs for run {self.run_id}: {e}", file=sys.stderr)
             with self._lock: self.buffer = logs_to_flush + self.buffer
             return False

    def close(self):
        """Flushes any remaining logs and closes the connection."""
        # print(f"DEBUG: close() called for BufferingMongoLogHandler run_id {self.run_id}") # Less verbose
        self.flush_logs()
        client_to_close = None
        with self.connect_lock:
             if self.client: client_to_close = self.client; self.client = None; self.collection = None; self.db = None; self.connection_failed = True
        if client_to_close:
            try: client_to_close.close(); # print(f"DEBUG: BufferingMongoClient closed for {self.run_id}") # Less verbose
            except Exception as e: print(f"ERROR: Exception closing MongoClient for run {self.run_id}: {e}", file=sys.stderr)
        super().close()

# === END OF CLASS DEFINITION ===

# --- Management functions ---
def remove_handler_reference(run_id):
     with _handler_lock: handler = _run_handlers.pop(run_id, None); #if handler: print(f"DEBUG: Removed handler ref for {run_id}")

def flush_mongo_handler_for_run(run_id, end_time=None):
    handler_instance = None;
    with _handler_lock: handler_instance = _run_handlers.get(run_id)
    if handler_instance and isinstance(handler_instance, BufferingMongoLogHandler):
        # print(f"DEBUG: Flushing logs via OpLogger for {run_id}")
        return handler_instance.flush_logs(end_time=end_time)
    else: print(f"WARNING: No active BufferingMongoLogHandler found for run_id {run_id} to flush."); return False

def _cleanup_all_mongo_handlers():
    print(f"DEBUG: atexit cleanup: Closing handlers in _run_handlers: {list(_run_handlers.keys())}")
    with _handler_lock: items = list(_run_handlers.items())
    closed_ids = set()
    for run_id, handler in items:
        # print(f"DEBUG: atexit attempting to close handler for run_id {run_id}")
        try:
            if handler: handler.close()
            closed_ids.add(run_id)
        except Exception as e: print(f"ERROR: Exception during atexit handler close for run {run_id}: {e}")
    with _handler_lock:
        for run_id in closed_ids:
            if run_id in _run_handlers: del _run_handlers[run_id]

atexit.register(_cleanup_all_mongo_handlers)

# --- Setup Logger ---
def setup_logger(run_id=None):
    logger = logging.getLogger('labbuild')
    if not logger.level or logger.level > logging.DEBUG: logger.setLevel(logging.DEBUG)

    stream_handler_exists = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    mongo_handler_for_this_run = _run_handlers.get(run_id) if run_id else None
    is_handler_attached = mongo_handler_for_this_run in logger.handlers if mongo_handler_for_this_run else False

    if stream_handler_exists and ((run_id and is_handler_attached) or not run_id): return logger

    if logger.hasHandlers():
        needs_clear = False
        for h in logger.handlers:
             if isinstance(h, BufferingMongoLogHandler) and getattr(h, 'run_id', None) != run_id: needs_clear = True; break
        if needs_clear or not stream_handler_exists or (run_id and not is_handler_attached):
            # print(f"DEBUG: Clearing/Reconfiguring logger handlers for run_id: {run_id}.") # Less verbose
            for h in list(logger.handlers):
                 if isinstance(h, BufferingMongoLogHandler) and getattr(h, 'run_id', None) != run_id: h.close()
                 logger.removeHandler(h)
            stream_handler_exists = False

    if not stream_handler_exists:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        stream_handler = logging.StreamHandler(sys.stdout); stream_handler.setLevel(logging.INFO); stream_handler.setFormatter(formatter); logger.addHandler(stream_handler); #print("DEBUG: StreamHandler added.")

    if run_id and MONGO_URI:
        with _handler_lock:
            current_handler = _run_handlers.get(run_id)
            if current_handler is None:
                #print(f"DEBUG: Creating BufferingMongoLogHandler for run_id: {run_id}")
                mongo_handler = BufferingMongoLogHandler(level=logging.DEBUG, run_id=run_id)
                if not mongo_handler.connection_failed:
                    logger.addHandler(mongo_handler); _run_handlers[run_id] = mongo_handler
                    # logger.info(f"MongoDB buffered logging ENABLED for run_id: {run_id}") # Logged by main now
                else: logger.warning(f"Failed to create BufferingMongoLogHandler for run {run_id}")
            elif current_handler not in logger.handlers:
                 #print(f"DEBUG: Re-adding existing BufferingMongoLogHandler for run {run_id}")
                 logger.addHandler(current_handler)

    elif not run_id: pass # logger.info("run_id not provided, MongoDB detailed logging disabled.")
    elif not MONGO_URI: logger.warning("MongoDB URI not set, MongoDB detailed logging disabled.")

    logger.propagate = True
    loggers_to_check = ['labbuild.commands', 'labbuild.orchestrator', 'labbuild.db', 'labbuild.config', 'labbuild.vcenter', 'labbuild.listing', 'labbuild.utils', 'labbuild.oplogger', 'VmManager', 'NetworkManager', 'ResourcePoolManager', 'FolderManager', 'PRTGManager']
    for name in loggers_to_check: logging.getLogger(name).propagate = True

    return logger

# --- END OF FILE logger/log_config.py ---