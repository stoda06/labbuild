# dashboard/tasks.py

import os
import sys
import subprocess
import shlex
import logging
import time

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger('dashboard.tasks')

# --- Core Task Execution Function ---
def run_labbuild_task(args_list):
    """Runs labbuild.py as a subprocess with the given arguments."""
    labbuild_script_path = os.path.join(project_root, 'labbuild.py')
    python_executable = sys.executable # Use the same python that runs Flask
    command = [python_executable, labbuild_script_path] + args_list
    command_str = ' '.join(shlex.quote(arg) for arg in command)
    logger.info(f"Executing background task: {command_str}")
    try:
        # Increased timeout
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False, # Don't raise exception on non-zero exit
            cwd=project_root, # Run from project root
            timeout=7200  # 2 hour timeout
        )
        logger.info(
            f"labbuild task '{' '.join(args_list)}' finished. "
            f"RC: {process.returncode}"
        )
        # Log output, especially on error
        if process.stdout:
             logger.debug(f"labbuild task stdout:\n{process.stdout}")
        if process.returncode != 0 and process.stderr:
            logger.error(f"labbuild task stderr:\n{process.stderr}")
        elif process.returncode != 0:
             logger.error(f"labbuild task failed with RC {process.returncode}, no stderr captured.")

    except subprocess.TimeoutExpired:
        logger.error(
            f"labbuild task timed out after 7200s: {command_str}"
        )
    except FileNotFoundError:
         logger.error(
             f"Error: Python executable '{python_executable}' or script "
             f"'{labbuild_script_path}' not found."
         )
    except Exception as e:
        logger.error(
            f"Failed run labbuild subprocess command '{command_str}': {e}",
            exc_info=True
        )

# --- Process Streaming Function ---
def stream_labbuild_process(full_command_list):
    """Generator runs labbuild.py and yields output line by line for SSE."""
    cmd_str = ' '.join(shlex.quote(arg) for arg in full_command_list)
    logger.info(f"Streaming command: {cmd_str}")
    process = None
    try:
        # Ensure using the correct python executable
        # full_command_list should already start with sys.executable
        process = subprocess.Popen(
            full_command_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Combine stdout/stderr for simplicity
            text=True, encoding='utf-8', errors='replace',
            cwd=project_root, # Run from project root
            bufsize=1  # Line buffered
        )
        yield f"event: start\ndata: Process started (PID: {process.pid})\n\n"
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                # Escape backslashes first, then newlines for SSE
                formatted_line = line.replace('\\', '\\\\').replace('\n', '\\n')
                yield f"data: {formatted_line}\n\n"
                time.sleep(0.01) # Slight pause for browser rendering
            process.stdout.close() # Ensure pipe is closed

        return_code = process.wait() # Wait for process to finish
        logger.info(f"Streaming command finished. RC: {return_code}")
        yield f"event: close\ndata: Process finished with exit code {return_code}\n\n"

    except FileNotFoundError:
        err_msg = f"Error: Command executable '{full_command_list[0]}' or script not found."
        logger.error(err_msg)
        yield f"event: error\ndata: {err_msg}\n\n"
        yield f"event: close\ndata: Process failed (file not found)\n\n"
    except Exception as e:
        err_msg = f"Subprocess error: {e}"
        logger.error(f"Stream error: {e}", exc_info=True)
        yield f"event: error\ndata: {err_msg}\n\n"
        yield f"event: close\ndata: Process failed (exception)\n\n"
    finally:
         # Ensure process is terminated if generator exits unexpectedly
         if process and process.poll() is None:
             try:
                 process.terminate()
                 time.sleep(0.1) # Give terminate a moment
                 if process.poll() is None: # Still running? Force kill.
                    process.kill()
                    logger.warning(f"Killed streaming process {process.pid}")
             except Exception as kill_e:
                 logger.error(f"Error terminating stream process: {kill_e}")

# --- END OF dashboard/tasks.py ---