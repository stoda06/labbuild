# dashboard/__init__.py

import os
import sys
import logging
import atexit
from flask import Flask
from flask.json.provider import DefaultJSONProvider
from bson import ObjectId
import datetime
import pytz
from flask_cors import CORS

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"--- DEBUG [Dashboard/__init__]: Added '{project_root}' to sys.path ---")

# Import extensions AFTER adding project root to path
from .extensions import (
    mongo_client_app, mongo_client_scheduler, scheduler, db,
    op_logs_collection, std_logs_collection, course_config_collection,
    host_collection, alloc_collection, course_mapping_rules_collection,
    shutdown_resources # Import the cleanup function
)
from .utils import format_datetime # Import needed utils

try:
    from .tasks import purge_old_mongodb_logs # If in tasks.py
except ImportError:
    # Fallback if you create a separate scheduled_tasks.py or similar
    # from .scheduled_tasks import purge_old_mongodb_logs
    # For now, let's define a dummy if not found, to prevent app crash
    def purge_old_mongodb_logs(retention_days=90):
        logging.getLogger('dashboard.init').error("purge_old_mongodb_logs function not found!")

LOG_RETENTION_DAYS_CONFIG = int(os.getenv("LOG_RETENTION_DAYS", "90"))


# --- Custom JSON Provider (Keep Here or Move to utils?) ---
class BsonJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            # Ensure timezone awareness before formatting if possible
            if obj.tzinfo is None:
                 # Attempt to localize to UTC if naive, assuming UTC storage
                 # This might need adjustment based on how datetimes are stored
                 try:
                     obj = pytz.utc.localize(obj)
                 except Exception: # Handle potential errors
                     pass # Keep naive if localization fails
            return format_datetime(obj) # Use consistent formatting helper
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

def create_app():
    """Flask application factory."""
    app = Flask(__name__, instance_relative_config=True)

    app.config.from_mapping(
        SECRET_KEY=os.getenv("FLASK_SECRET_KEY", "dev-secret-key"),
        REDIS_URL=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )
    
    app.logger.setLevel(logging.INFO) 
    app.json = BsonJSONProvider(app)
    CORS(app) 
    
    try:
        if scheduler and not scheduler.running:
            scheduler.start()
            app.logger.info("Scheduler started successfully via app factory.")

            # --- ADD THE LOG PURGE JOB ---
            job_id_purge = 'purge_mongo_logs_weekly'
            if not scheduler.get_job(job_id_purge): # Add job only if it doesn't exist
                scheduler.add_job(
                    id=job_id_purge,
                    func=purge_old_mongodb_logs,
                    trigger='cron',
                    day_of_week='thu', # Run every Thursday
                    hour=2,            # At 2 AM (server time, as per scheduler's timezone)
                    minute=30,
                    args=[LOG_RETENTION_DAYS_CONFIG], # Pass retention period
                    misfire_grace_time=3600 # Allow 1 hour for misfires
                )
                app.logger.info(f"Scheduled weekly MongoDB log purge job (ID: {job_id_purge}) "
                                f"to run every Thursday at 2:30 AM (server time), "
                                f"retaining logs for {LOG_RETENTION_DAYS_CONFIG} days.")
            else:
                app.logger.info(f"Weekly MongoDB log purge job (ID: {job_id_purge}) already scheduled.")

        elif not scheduler:
             app.logger.error("Scheduler object not initialized. Cannot schedule log purge.")
    except Exception as e:
        app.logger.error(f"Error starting scheduler or adding purge job in app factory: {e}", exc_info=True)

    from .routes import main, api, sse, settings
    from .routes import labbuild_actions, allocation_actions, build_planner_actions, email_actions, apm_actions
    app.register_blueprint(main.bp)
    app.register_blueprint(api.bp)
    app.register_blueprint(sse.bp)
    app.register_blueprint(settings.bp)

    app.register_blueprint(labbuild_actions.bp, url_prefix='/labbuild-actions')
    app.register_blueprint(allocation_actions.bp)
    app.register_blueprint(build_planner_actions.bp)
    app.register_blueprint(email_actions.bp, url_prefix='/email-actions')
    app.register_blueprint(apm_actions.bp)

    atexit.register(shutdown_resources) 
    app.logger.info("Flask app created successfully.")
    app.logger.info(f"--- DEBUG [App Factory]: REDIS_URL = {app.config['REDIS_URL']} ---")

    return app

# --- END OF dashboard/__init__.py ---