# dashboard/extensions.py

import os
import sys
import logging
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from dotenv import load_dotenv
import pytz

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"--- DEBUG [Dashboard/extensions]: Added '{project_root}' to sys.path ---")

# Import constants AFTER adding project root
from constants import (
    DB_NAME, OPERATION_LOG_COLLECTION, LOG_COLLECTION,
    COURSE_CONFIG_COLLECTION, HOST_COLLECTION, ALLOCATION_COLLECTION,
    COURSE_MAPPING_RULES_COLLECTION, INTERIM_ALLOCATION_COLLECTION,
    BUILD_RULES_COLLECTION, TRAINER_EMAIL_COLLECTION
)

# --- Configuration ---
load_dotenv(os.path.join(project_root, '.env')) # Load .env from project root

# --- MongoDB Connection ---
MONGO_USER = os.getenv("MONGO_USER", "labbuild_user")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF")
MONGO_HOST = os.getenv("MONGO_HOST")
SCHEDULE_COLLECTION = "scheduled_jobs" # Keep this specific to scheduler

logger = logging.getLogger('dashboard.extensions') # Specific logger

mongo_client_app = None
mongo_client_scheduler = None
db = None
op_logs_collection = None
std_logs_collection = None
course_config_collection = None
host_collection = None
alloc_collection = None
course_mapping_rules_collection = None
scheduler = None
interim_alloc_collection = None
build_rules_collection = None
trainer_email_collection = None

if not MONGO_HOST:
    logger.critical("MONGO_HOST environment variable not set. Database/Scheduler unavailable.")
else:
    MONGO_URI = (
        f"mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASSWORD)}"
        f"@{MONGO_HOST}:27017/{DB_NAME}"
    )
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
        interim_alloc_collection = db[INTERIM_ALLOCATION_COLLECTION]
        build_rules_collection = db[BUILD_RULES_COLLECTION]
        trainer_email_collection = db[TRAINER_EMAIL_COLLECTION]
        logger.info("Successfully connected App MongoDB client.")

        # Scheduler client
        mongo_client_scheduler = MongoClient(
            MONGO_URI, serverSelectionTimeoutMS=5000, appname="LabBuildScheduler"
        )
        mongo_client_scheduler.admin.command('ping')
        logger.info("Successfully connected Scheduler MongoDB client.")

        # --- Scheduler Initialization ---
        SERVER_TIMEZONE_STR = os.getenv('SERVER_TIMEZONE', 'Australia/Sydney')
        try:
            SERVER_TIMEZONE = pytz.timezone(SERVER_TIMEZONE_STR)
            logger.info(f"APScheduler configured to use timezone: {SERVER_TIMEZONE_STR}")
        except pytz.UnknownTimeZoneError:
            logger.error(f"Unknown timezone '{SERVER_TIMEZONE_STR}'. Defaulting to UTC.")
            SERVER_TIMEZONE = pytz.utc

        jobstores = {
            'default': MongoDBJobStore(
                database=DB_NAME,
                collection=SCHEDULE_COLLECTION,
                client=mongo_client_scheduler
            )
        }
        scheduler = BackgroundScheduler(jobstores=jobstores, timezone=SERVER_TIMEZONE)
        # Don't start scheduler here, start it in the app factory (__init__.py)

    except ConnectionFailure as e:
        logger.critical(f"MongoDB connection failed: {e}")
        # Ensure clients are None if connection fails
        mongo_client_app = None
        mongo_client_scheduler = None
        db = None
        scheduler = None
        trainer_email_collection = None
    except Exception as e:
        logger.critical(f"Unexpected error during MongoDB/Scheduler initialization: {e}", exc_info=True)
        mongo_client_app = None
        mongo_client_scheduler = None
        db = None
        scheduler = None
        trainer_email_collection = None

def shutdown_resources():
    """Shutdown scheduler and close DB connections."""
    global mongo_client_scheduler, mongo_client_app, scheduler
    logger.info("Executing shutdown_resources...")
    if scheduler and scheduler.running:
        logger.info("Shutting down scheduler...")
        try:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler shutdown initiated.")
        except Exception as e:
            logger.error(f"Scheduler shutdown error: {e}", exc_info=True)

    if mongo_client_scheduler:
        logger.info("Closing scheduler MongoDB client...")
        try: mongo_client_scheduler.close()
        except Exception as e: logger.error(f"Error closing scheduler Mongo client: {e}")
        mongo_client_scheduler = None
        logger.info("Scheduler MongoDB client closed.")

    if mongo_client_app:
        logger.info("Closing app MongoDB client...")
        try: mongo_client_app.close()
        except Exception as e: logger.error(f"Error closing app Mongo client: {e}")
        mongo_client_app = None
        logger.info("App MongoDB client closed.")
    logger.info("shutdown_resources finished.")


# --- END OF dashboard/extensions.py ---