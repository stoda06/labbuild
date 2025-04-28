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

    # --- Configuration ---
    # Load default config and override with instance config if available
    app.config.from_mapping(
        SECRET_KEY=os.getenv("FLASK_SECRET_KEY", "dev-secret-key"),
        # Default REDIS_URL, can be overridden by instance config or env
        REDIS_URL=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        # Add other default configs if needed
    )
    # Optionally load from a config file if you create one (e.g., instance/config.py)
    # app.config.from_pyfile('config.py', silent=True)

    # --- Setup Logging ---
    app.logger.setLevel(logging.INFO) # Set default level
    # You might want more sophisticated logging setup here later

    # --- Apply Custom JSON Provider ---
    app.json = BsonJSONProvider(app)

    # --- Initialize Extensions (using objects from extensions.py) ---
    # The connections are already established in extensions.py
    # We just need to make sure the app context works if needed by extensions later.
    # For scheduler, ensure it's started
    try:
        if scheduler and not scheduler.running:
            scheduler.start()
            app.logger.info("Scheduler started successfully via app factory.")
        elif not scheduler:
             app.logger.error("Scheduler object not initialized.")
    except Exception as e:
        app.logger.error(f"Error starting scheduler in app factory: {e}")

    # --- Register Blueprints ---
    from .routes import main, actions, api, sse
    app.register_blueprint(main.bp)
    app.register_blueprint(actions.bp)
    app.register_blueprint(api.bp)
    app.register_blueprint(sse.bp)

    # --- Register Cleanup ---
    atexit.register(shutdown_resources) # Ensure cleanup happens on exit

    app.logger.info("Flask app created successfully.")
    app.logger.info(f"--- DEBUG [App Factory]: REDIS_URL = {app.config['REDIS_URL']} ---")

    return app

# --- END OF dashboard/__init__.py ---