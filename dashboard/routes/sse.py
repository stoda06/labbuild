# dashboard/routes/sse.py

import logging
import json
import redis # Need redis library
import shlex
import os
import sys
from flask import Blueprint, Response, stream_with_context, request, current_app

# Import task runner
from ..tasks import stream_labbuild_process

# Define Blueprint
bp = Blueprint('sse', __name__) # No prefix needed here, routes are specific
logger = logging.getLogger('dashboard.routes.sse')

# Ensure project root is in path for tasks.py import if run standalone
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


@bp.route("/log-stream/<run_id>")
def log_stream(run_id):
    """SSE endpoint to stream logs for a specific run_id via Redis."""
    # Logic moved from original app.py
    redis_url = current_app.config.get("REDIS_URL")
    if not redis_url:
        msg = json.dumps({"error": "Real-time streaming not configured."})
        return Response(f"event: error\ndata: {msg}\n\n", mimetype="text/event-stream")

    def generate_log_stream(run_id_local):
        redis_client, pubsub = None, None
        channel = f"log_stream::{run_id_local}"
        try:
            redis_client = redis.Redis.from_url(redis_url, decode_responses=True); redis_client.ping() # Test connection
            pubsub = redis_client.pubsub(); pubsub.subscribe(channel)
            logger.info(f"SSE client subscribed to Redis channel: {channel}")
            yield f"event: connected\ndata: Subscribed to logs for {run_id_local}\n\n" # Connection confirmation
            for message in pubsub.listen():
                if message['type'] == 'message': yield f"data: {message['data']}\n\n" # Send log data
                elif message['type'] == 'subscribe': continue # Ignore subscribe confirmation
        except redis.exceptions.ConnectionError as e: logger.error(f"SSE Redis connection error {run_id_local}: {e}"); yield f"event: error\ndata: Redis connection error\n\n"
        except Exception as e: logger.error(f"SSE generator error {run_id_local}: {e}", exc_info=True); yield f"event: error\ndata: Internal stream error\n\n"
        finally:
            if pubsub:
                try: pubsub.unsubscribe(channel); pubsub.close()
                except Exception as e_cls: logger.error(f"Error closing pubsub {run_id_local}: {e_cls}")
            if redis_client:
                try: redis_client.close()
                except Exception as e_cls: logger.error(f"Error closing Redis client {run_id_local}: {e_cls}")
            logger.info(f"SSE stream closed for {run_id_local}")

    return Response(stream_with_context(generate_log_stream(run_id)), mimetype="text/event-stream")

@bp.route('/stream-command', methods=['POST'])
def stream_command():
    """Handles command submission and returns SSE stream for terminal."""
    # Logic moved from original app.py
    command_line = request.form.get('command', '')
    if not command_line.strip(): return Response("event: error\ndata: Empty command.\n\n", mimetype="text/event-stream")

    # Basic prefix check (adjust allowed commands as needed)
    # Example: Only allow commands starting with 'labbuild '
    if not command_line.strip().startswith('labbuild '):
        return Response("event: error\ndata: Invalid command prefix. Must start with 'labbuild '.\n\n", mimetype="text/event-stream")

    try:
        # Parse the command line *after* 'labbuild '
        args_part = command_line.strip()[len('labbuild '):]
        args = shlex.split(args_part, posix=(os.name != 'nt'))

        # Construct the full command list for subprocess
        labbuild_script_path = os.path.join(project_root, 'labbuild.py')
        full_command_list = [sys.executable, labbuild_script_path] + args

        if not os.path.exists(full_command_list[1]): # Check script path
            raise FileNotFoundError(f"Script not found: {full_command_list[1]}")

    except (ValueError, FileNotFoundError) as e:
        return Response(f"event: error\ndata: Error parsing command: {e}\n\n"
                        f"event: close\ndata: Invalid command\n\n", mimetype="text/event-stream")
    except Exception as e:
        logger.error(f"Error parsing command for streaming: {e}", exc_info=True)
        return Response("event: error\ndata: Server error parsing command.\n\n"
                        "event: close\ndata: Server error\n\n", mimetype="text/event-stream")

    # Return the streaming response using the task function
    return Response(stream_with_context(stream_labbuild_process(full_command_list)), mimetype='text/event-stream')


# --- END OF dashboard/routes/sse.py ---