#!/usr/bin/env python3
# run.py - Main entry point to run the Flask Dashboard

import os
import sys

# Add project root to sys.path to allow imports from dashboard package
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"--- DEBUG [run.py]: Added '{project_root}' to sys.path ---")


# Import the app factory from the dashboard package
try:
    from dashboard import create_app
except ImportError as e:
     print(f"\n--- CRITICAL ERROR ---")
     print(f"Failed to import 'create_app' from the 'dashboard' package.")
     print(f"Error: {e}")
     print(f"Current sys.path: {sys.path}")
     print(f"Project root calculated as: {project_root}")
     print(f"Please ensure the directory structure is correct and '__init__.py' exists in 'dashboard/'.")
     sys.exit(1)


# Create the Flask app instance
app = create_app()

if __name__ == '__main__':
    # Get host and port from environment variables or use defaults
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', 5001))
    # Debug mode based on FLASK_DEBUG env var (defaults to False if not set or invalid)
    debug = os.getenv('FLASK_DEBUG', 'False').lower() in ('true', '1', 't')

    print(f"--- Starting Flask App ---")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Debug Mode: {debug}")
    print(f"Redis URL: {app.config.get('REDIS_URL', 'Not Set')}")
    print(f"--------------------------")

    # Run the Flask development server
    # Use use_reloader=False if you encounter issues with scheduler/background threads
    app.run(host=host, port=port, debug=debug, use_reloader=debug)

# --- END OF run.py ---