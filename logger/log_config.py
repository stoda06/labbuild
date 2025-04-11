# --- START OF FILE logger/log_config.py ---

import logging
# from logging.handlers import TimedRotatingFileHandler # Keep commented if not using file logging
import sys
import os
from urllib.parse import quote_plus
import pymongo
from dotenv import load_dotenv
import datetime
import atexit
import traceback

# --- MongoDB Configuration ---
load_dotenv()
MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
MONGO_HOST = os.getenv("MONGO_HOST")
DB_NAME = "labbuild_db"
LOG_COLLECTION = "logs" # Standard logs collection

MONGO_URI = None
if MONGO_HOST:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"
else:
    print("WARNING: MONGO_HOST not set. MongoDB logging disabled.", file=sys.stderr)

_mongo_handlers = []

class MongoLogHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET, mongo_uri=MONGO_URI, db_name=DB_NAME, collection_name=LOG_COLLECTION, run_id=None): # Add run_id
        super().__init__(level)
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.run_id = run_id # Store run_id
        self.client = None
        self.db = None
        self.collection = None
        self.connection_failed = False

        if not self.mongo_uri:
            self.connection_failed = True
            print("ERROR: MongoDB URI not configured. Cannot initialize MongoLogHandler.", file=sys.stderr)
            return

        try:
            self.client = pymongo.MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ismaster')
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            _mongo_handlers.append(self)
        except pymongo.errors.ConnectionFailure as e:
            self.connection_failed = True
            print(f"ERROR: Failed to connect to MongoDB for logging: {e}", file=sys.stderr)
        except Exception as e:
            self.connection_failed = True
            print(f"ERROR: Unexpected error initializing MongoLogHandler: {e}", file=sys.stderr)

    def emit(self, record):
        if self.connection_failed or self.collection is None:
            return

        try:
            log_entry = {
                "timestamp": datetime.datetime.utcnow(),
                "level": record.levelname,
                "levelno": record.levelno,
                "message": self.format(record),
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
            # --- Add run_id if available ---
            if self.run_id:
                log_entry['run_id'] = self.run_id
            # --- End Add run_id ---

            if record.exc_info:
                log_entry['exc_info'] = self.formatException(record.exc_info)
                log_entry['exc_text'] = traceback.format_exception(*record.exc_info)

            self.collection.insert_one(log_entry)

        except pymongo.errors.PyMongoError as e:
            print(f"ERROR: Failed write log to MongoDB: {e}", file=sys.stderr)
            self.handleError(record)
        except Exception as e:
            print(f"ERROR: Unexpected error during log emit: {e}", file=sys.stderr)
            self.handleError(record)

    def close(self):
        if self.client:
            try: self.client.close(); self.client = None; self.collection = None # Clear refs
            except Exception as e: print(f"ERROR: Error closing MongoLogHandler connection: {e}", file=sys.stderr)
        super().close()

def _cleanup_mongo_handlers():
    for handler in _mongo_handlers:
        handler.close()

atexit.register(_cleanup_mongo_handlers)

# --- Updated setup_logger ---
def setup_logger(run_id=None): # Accept run_id
    """
    Returns logger with StreamHandler (INFO+) and MongoLogHandler (DEBUG+).
    Passes run_id to MongoLogHandler.
    """
    logger = logging.getLogger('labbuild')
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers to prevent duplication if called multiple times (e.g., during testing)
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console Handler (INFO+)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # MongoDB Handler (DEBUG+)
    if MONGO_URI:
        # Pass run_id to the handler
        mongo_handler = MongoLogHandler(level=logging.DEBUG, run_id=run_id)
        if not mongo_handler.connection_failed: # Only add if connected
            mongo_handler.setFormatter(formatter)
            logger.addHandler(mongo_handler)
            # Log confirmation only once or use debug level
            # logger.info("MongoDB logging enabled.")
        else:
            logger.warning("MongoDB logging disabled due to connection failure.")
    else:
        logger.warning("MongoDB logging is disabled because MONGO_HOST is not set.")

    # Update run_id if logger instance already existed (less critical now with handler clearing)
    # elif run_id:
    #     for handler in logger.handlers:
    #          if isinstance(handler, MongoLogHandler):
    #              handler.run_id = run_id

    return logger
# --- END OF FILE logger/log_config.py ---