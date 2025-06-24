# --- START OF FINAL REVISED logger/log_config.py (PEP 8 Compliant) ---

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

# --- Import the Redis handler ---
try:
    from .redis_handler import RedisStreamHandler
except ImportError:
    print("WARNING: logger/redis_handler.py not found. "
          "Real-time streaming disabled.", file=sys.stderr)
    # Dummy class if Redis handler is missing
    class RedisStreamHandler:
        def __init__(self, *args, **kwargs):
            self.redis_client = None
        def setFormatter(self, *args, **kwargs):
            pass
        def close(self):
            pass

# --- Configuration ---
load_dotenv()
MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
MONGO_HOST = os.getenv("MONGO_HOST")
DB_NAME = "labbuild_db"
LOG_COLLECTION = "logs"  # Target collection for BufferingMongoLogHandler

MONGO_URI = None
if MONGO_HOST:
    MONGO_URI = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"
        "?serverSelectionTimeoutMS=5000"
    )
else:
    print("WARNING: MONGO_HOST not set. MongoDB logging disabled.",
          file=sys.stderr)

REDIS_URL_CONFIG = os.getenv("REDIS_URL")
if not REDIS_URL_CONFIG:
    print("WARNING: REDIS_URL not set. Real-time log streaming disabled.",
          file=sys.stderr)

# --- Globals for BufferingMongoLogHandler Management ---
_run_handlers = {}  # Stores {run_id: BufferingMongoLogHandler instance}
_handler_lock = threading.Lock()


# --- BufferingMongoLogHandler Class ---
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
        self._lock = threading.Lock()
        self.connection_failed = False
        self.client = None
        self.collection = None
        self.connect_lock = threading.Lock()

        if not self.mongo_uri or not self.run_id:
            self.connection_failed = True
            print(f"ERROR: BufferingMongoLogHandler disabled for run {self.run_id}. "
                  f"Missing URI or run_id.", file=sys.stderr)

    def _connect(self):
        """Connects to MongoDB if not already connected. Returns True on success."""
        if self.collection is not None:
            return True
        if self.connection_failed:
            return False

        with self.connect_lock:
            # Double check after acquiring lock
            if self.collection is not None:
                return True
            if self.connection_failed:
                return False

            # print(f"DEBUG: BufferingMongoHandler connecting for run {self.run_id}...")
            try:
                self.client = pymongo.MongoClient(self.mongo_uri)
                self.client.admin.command('ismaster')  # Check connection
                db_conn = self.client[self.db_name]
                self.collection = db_conn[self.collection_name]
                # print(f"DEBUG: BufferingMongoHandler CONNECTED run {self.run_id}")
                return True
            except (ConnectionFailure, PyMongoError) as e:
                self.connection_failed = True
                print(f"ERROR: BufferingMongoLogHandler connection FAILED "
                      f"run {self.run_id}: {e}", file=sys.stderr)
                if self.client:
                    try:
                        self.client.close()
                    except Exception:
                        pass  # Ignore errors during close on failure
                self.client = None
                self.collection = None
                return False
            except Exception as e:
                self.connection_failed = True
                print(f"ERROR: Unexpected connect BufferingMongoLogHandler "
                      f"run {self.run_id}: {e}", file=sys.stderr)
                if self.client:
                    try:
                        self.client.close()
                    except Exception:
                        pass
                self.client = None
                self.collection = None
                return False

    def emit(self, record):
        """Appends the formatted log record to the internal buffer."""
        try:
            log_entry = {
                "timestamp": datetime.datetime.utcnow(),
                "level": record.levelname,
                "levelno": record.levelno,
                "message": self.format(record), # Use formatter
                "logger_name": record.name,
                "pathname": record.pathname,
                "filename": record.filename,
                "module": record.module,
                "lineno": record.lineno,
                "funcName": record.funcName,
                "process": record.process,
                "thread": record.thread,
                "threadName": record.threadName,
            }
            if record.exc_info:
                try:
                    log_entry['exc_text'] = "".join(
                        traceback.format_exception(*record.exc_info)
                    )
                except Exception as fmt_e:
                    log_entry['exc_text'] = f"Error formatting exception: {fmt_e}"
            with self._lock:
                self.buffer.append(log_entry)
        except Exception as e:
            print(f"ERROR: Failed to buffer log record: {e}", file=sys.stderr)
            self.handleError(record)

    def flush_logs(self, end_time=None):
        """Writes the buffered logs to the MongoDB document for the run_id."""
        logs_to_flush = []
        with self._lock:
            if not self.buffer:
                return True  # Nothing to flush
            logs_to_flush = self.buffer[:]  # Copy buffer
            self.buffer.clear()  # Clear original buffer

        if not logs_to_flush:
            return True

        if not self._connect():
            print(f"ERROR: Cannot flush logs for run {self.run_id}, "
                  f"MongoDB connection failed.", file=sys.stderr)
            # Put logs back in buffer if connection failed
            with self._lock:
                self.buffer = logs_to_flush + self.buffer
            return False

        try:
            set_on_insert_dict = {
                'run_id': self.run_id,
                'first_log_time': logs_to_flush[0]['timestamp']
            }
            set_dict = {
                'last_log_time': logs_to_flush[-1]['timestamp']
            }
            if end_time:
                set_dict['end_time'] = end_time

            update_operation = {
                '$push': {'messages': {'$each': logs_to_flush}},
                '$setOnInsert': set_on_insert_dict,
                '$set': set_dict,
                '$inc': {'log_count': len(logs_to_flush)}
            }
            # print(f"--- DEBUG [Mongo Flush]: Attempting update for {self.run_id}...")
            result = self.collection.update_one(
                {'run_id': self.run_id},
                update_operation,
                upsert=True
            )
            # print(f"--- DEBUG [Mongo Flush]: Update result: UpsertedId="
            #       f"{result.upserted_id}, Matched={result.matched_count}, "
            #       f"Modified={result.modified_count} ---")
            # print(f"DEBUG: Flushed {len(logs_to_flush)} logs run {self.run_id}.")
            return True
        except PyMongoError as e:
            # print(f"--- DEBUG [Mongo Flush]: PyMongoError flush {self.run_id}: {e} ---")
            print(f"ERROR: Failed writing logs Mongo run {self.run_id}: {e}",
                  file=sys.stderr)
            with self._lock:
                self.buffer = logs_to_flush + self.buffer # Re-add on failure
            return False
        except Exception as e:
            # print(f"--- DEBUG [Mongo Flush]: General Exception flush {self.run_id}: {e} ---")
            print(f"ERROR: Unexpected flush logs run {self.run_id}: {e}",
                  file=sys.stderr)
            with self._lock:
                self.buffer = logs_to_flush + self.buffer # Re-add on failure
            return False

    def close(self):
        """Flushes remaining logs and closes the MongoDB connection."""
        self.flush_logs()
        client_to_close = None
        with self.connect_lock:
            if self.client:
                client_to_close = self.client
                self.client = None
                self.collection = None
                self.connection_failed = True
        if client_to_close:
            try:
                client_to_close.close()
            except Exception as e:
                print(f"ERROR: Exc closing MongoClient run {self.run_id}: {e}",
                      file=sys.stderr)
        super().close()


# --- Management Functions ---
def remove_handler_reference(run_id):
     """Removes the BufferingMongoLogHandler reference."""
     with _handler_lock:
         _run_handlers.pop(run_id, None)

def flush_mongo_handler_for_run(run_id, end_time=None):
    """Flushes the BufferingMongoLogHandler for a specific run_id."""
    with _handler_lock:
        handler = _run_handlers.get(run_id)
    if handler and isinstance(handler, BufferingMongoLogHandler):
        return handler.flush_logs(end_time=end_time)
    return False

def _cleanup_all_handlers():
    """Cleans up BufferingMongo and RedisStream handlers on exit."""
    print("--- DEBUG [atexit]: Cleaning up logging handlers ---")

    # 1. Close tracked BufferingMongo Handlers
    mongo_handlers_to_close = []
    with _handler_lock:
        if _run_handlers:
             mongo_handlers_to_close = list(_run_handlers.values())
             _run_handlers.clear()
    for handler in mongo_handlers_to_close:
        run_id_mongo = getattr(handler, 'run_id', 'unknown')
        try:
            handler.close()
        except Exception as e:
            print(f"ERROR [atexit]: Exc closing Mongo handler {run_id_mongo}: {e}")

    # 2. Close attached RedisStream Handlers
    loggers_to_check = [ # List includes root (None) and known app loggers
        None, 'labbuild', 'labbuild.commands', 'labbuild.orchestrator',
        'labbuild.db', 'labbuild.config', 'labbuild.vcenter',
        'labbuild.listing', 'labbuild.utils', 'labbuild.oplogger',
        'labbuild.vmops', 'VmManager', 'NetworkManager',
        'ResourcePoolManager', 'FolderManager', 'PermissionManager',
        'DatastoreManager', 'PRTGManager', 'labbuild.redis_handler',
        'labs.setup.checkpoint', 'labs.setup.palo', 'labs.setup.f5',
        'labs.setup.avaya', 'labs.setup.pr', 'labs.setup.nu',
        'labs.manage.vm_operations'
    ]
    for logger_name in loggers_to_check:
        logger_instance = logging.getLogger(logger_name)
        if not isinstance(logger_instance, logging.PlaceHolder):
            for handler in list(logger_instance.handlers): # Iterate copy
                if isinstance(handler, RedisStreamHandler):
                    run_id_redis = getattr(handler, 'run_id', 'unknown')
                    logger_id = logger_name if logger_name else "root"
                    try:
                        handler.close()
                        logger_instance.removeHandler(handler)
                    except Exception as e:
                        print(f"ERROR [atexit]: Exc closing Redis handler "
                              f"{run_id_redis} logger '{logger_id}': {e}")
    print("--- DEBUG [atexit]: Logging handler cleanup finished ---")

atexit.register(_cleanup_all_handlers)


# --- Setup Logger ---
def setup_logger(run_id=None):
    """
    Configures the main 'labbuild' logger.

    Should be called ONCE per run_id after OperationLogger initialization.
    Other modules should use `logging.getLogger(__name__)`.
    """
    logger = logging.getLogger('labbuild')
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # --- Clean ONLY existing handlers attached DIRECTLY to 'labbuild' ---
    # Prevents duplicates if called multiple times for 'labbuild' logger.
    for handler in list(logger.handlers):
        if isinstance(handler, (logging.StreamHandler,
                                BufferingMongoLogHandler, RedisStreamHandler)):
             try:
                 handler.close()
             except Exception:
                 pass # Ignore cleanup close errors
             logger.removeHandler(handler)

    # 1. Stream Handler (Console) - Always add
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO) # Console level
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # 2. Buffering Mongo Handler - Add if run_id and config exist
    if run_id and MONGO_URI:
        with _handler_lock:
            if run_id not in _run_handlers:
                mongo_handler = BufferingMongoLogHandler(
                    level=logging.DEBUG, run_id=run_id
                )
                if not mongo_handler.connection_failed:
                    mongo_handler.setFormatter(formatter)
                    logger.addHandler(mongo_handler)
                    _run_handlers[run_id] = mongo_handler
                else:
                    logger.warning(f"Failed create Mongo handler for {run_id}.")
                    mongo_handler.close() # Cleanup failed handler
            else:
                # Handler already tracked, ensure attached to logger
                existing_handler = _run_handlers[run_id]
                if existing_handler not in logger.handlers:
                    logger.addHandler(existing_handler)

    # 3. Redis Stream Handler - Add if run_id and config exist
    if run_id and REDIS_URL_CONFIG:
        if not any(isinstance(h, RedisStreamHandler) and
                   getattr(h, 'run_id', None) == run_id for h in logger.handlers):
            redis_handler = RedisStreamHandler(run_id=run_id, level=logging.DEBUG)
            if redis_handler.redis_client is not None: # Check connection
                 redis_handler.setFormatter(formatter)
                 logger.addHandler(redis_handler)
            else:
                 logger.warning(f"Failed create Redis handler for {run_id}.")
                 redis_handler.close() # Cleanup failed handler

    # Configure propagation - 'labbuild' is top-level
    logger.propagate = False

    # Ensure sub-loggers propagate and clear their specific handlers
    loggers_to_configure = [
        'labbuild.commands', 'labbuild.orchestrator', 'labbuild.db',
        'labbuild.config', 'labbuild.vcenter', 'labbuild.listing',
        'labbuild.utils', 'labbuild.oplogger', 'labbuild.vmops',
        'VmManager', 'NetworkManager', 'ResourcePoolManager',
        'FolderManager', 'PermissionManager', 'DatastoreManager',
        'PRTGManager', 'labbuild.redis_handler',
        'labs.setup.checkpoint', 'labs.setup.palo', 'labs.setup.f5',
        'labs.setup.avaya', 'labs.setup.pr', 'labs.setup.nu',
        'labs.manage.vm_operations'
    ]
    for name in loggers_to_configure:
        sub_logger = logging.getLogger(name)
        sub_logger.setLevel(logging.DEBUG)
        sub_logger.propagate = True
        for h in list(sub_logger.handlers): # Remove direct handlers
            sub_logger.removeHandler(h)

    return logger