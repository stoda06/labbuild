#!/usr/bin/env python3
"""
Lab Build Management Tool - Entry Point

Parses arguments and dispatches actions to command handlers.
"""

import argparse
import argcomplete
import logging
import sys
import time
from typing import Optional, Dict, List, Any, Callable, Tuple, Generator

from dotenv import load_dotenv

# Import local modules
from constants import * # Import constants
from logger.log_config import setup_logger # Setup logger function
from operation_logger import OperationLogger # Import logger class
from listing import list_vendor_courses, list_allocations # Import listing functions
from commands import setup_environment, teardown_environment, manage_environment # Import command handlers
from config_utils import fetch_and_prepare_course_config # Needed for arg validation

# -----------------------------------------------------------------------------
# Environment Setup
# -----------------------------------------------------------------------------
load_dotenv()
# Get placeholder logger instance early, setup called properly in main()
logger = logging.getLogger('labbuild')

# -----------------------------------------------------------------------------
# Main Entry Point and Argument Parsing
# -----------------------------------------------------------------------------
def main():
    """Main execution function."""
    # --- Debug: Print raw arguments ---
    # print(f"DEBUG: sys.argv = {sys.argv}")
    # ---
    global logger

    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    parser.add_argument('-l', '--list-allocations', action='store_true',
                        help='List current pod allocations for a vendor.')
    parser.add_argument('-v', '--vendor', required=True,
                        help='Vendor code (e.g., pa, cp, f5). Required for all operations.')
    parser.add_argument('-s', '--start-pod', type=int,
                        help='Starting pod/class number for filtering list or action range.')
    parser.add_argument('-e', '--end-pod', type=int,
                        help='Ending pod/class number for filtering list or action range.')
    parser.add_argument('--test', action='store_true',
                        help='Test allocation validity: check VR VM existence and remove invalid DB entries (use with -l).')
    argcomplete.autocomplete(parser)

    subparsers = parser.add_subparsers(dest='command', title='commands',
                                       help='Action command (setup, manage, teardown)')

    # --- Common Arguments for Subparsers ---
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-g', '--course', required=True, help='Course name or "?".')
    common_parser.add_argument('--host', help='Target host.') # Required checked later
    common_parser.add_argument('-t', '--tag', default="untagged", help='Tag.')
    common_parser.add_argument('--verbose', action='store_true', help='Debug logging.')
    common_parser.add_argument('-th', '--thread', type=int, default=4, help='Concurrency.')
    pod_range_parser = argparse.ArgumentParser(add_help=False);
    pod_range_parser.add_argument('-s', '--start-pod', type=int, help='Start pod # for action.') # Required checked later
    pod_range_parser.add_argument('-e', '--end-pod', type=int, help='End pod # for action.') # Required checked later
    f5_parser = argparse.ArgumentParser(add_help=False); f5_parser.add_argument('-cn', '--class_number', type=int) # Required checked later

    # --- Subparsers ---
    setup_parser = subparsers.add_parser('setup', help='Set up lab.', parents=[common_parser, pod_range_parser, f5_parser])
    setup_parser.add_argument('-c', '--component', help='Components or "?" to list.'); setup_parser.add_argument('-ds', '--datastore', default="vms"); setup_parser.add_argument('-r', '--re-build', action='store_true'); setup_parser.add_argument('-mem', '--memory', type=int); setup_parser.add_argument('--full', action='store_true'); setup_parser.add_argument('--monitor-only', action='store_true'); setup_parser.add_argument('--prtg-server'); setup_parser.add_argument('--perm', action='store_true'); setup_parser.add_argument('--db-only', action='store_true'); setup_parser.set_defaults(func=setup_environment)
    manage_parser = subparsers.add_parser('manage', help='Manage VM power.', parents=[common_parser, pod_range_parser, f5_parser])
    manage_parser.add_argument('-c', '--component', help='Components or "?".'); manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True); manage_parser.set_defaults(func=manage_environment)
    teardown_parser = subparsers.add_parser('teardown', help='Tear down lab.', parents=[common_parser, pod_range_parser, f5_parser])
    teardown_parser.add_argument('--monitor-only', action='store_true'); teardown_parser.add_argument('--db-only', action='store_true'); teardown_parser.set_defaults(func=teardown_environment)

    # --- Parse Arguments ---
    try:
        args = parser.parse_args()
        # print(f"\nDEBUG: Final parsed args = {vars(args)}\n") # Optional debug
    except SystemExit as e:
        # print(f"\nDEBUG: Argparse exited with code: {e.code}\n") # Optional debug
        sys.exit(e.code)

    # --- Mode Handling ---
    # Vendor required check done by argparse

    if args.list_allocations:
        # Setup logger minimally for listing
        global logger
        logger = setup_logger() # Setup without run_id
        log_level = logging.DEBUG if getattr(args,'verbose', False) else logging.INFO; logger.setLevel(log_level)
        # Call listing function
        list_allocations(args.vendor, args.start_pod, args.end_pod, test_mode=args.test)
        sys.exit(0)

    # If not listing, a command is required
    if not args.command:
        if args.test: parser.error("--test flag can only be used with --list-allocations (-l).")
        else: parser.error("A command (setup, manage, teardown) is required.")

    # Test cannot be used with commands
    if args.test: parser.error("--test flag cannot be used with commands.")

    # --- Initialize Operation Logger (ONLY for commands) ---
    operation_logger = None
    try:
        args_dict = vars(args).copy(); args_dict.pop('func', None) # Remove func before logging args
        operation_logger = OperationLogger(args.command, args_dict)
        # Reconfigure global logger with run_id
        logger = setup_logger(run_id=operation_logger.run_id)
    except Exception as e:
        logging.basicConfig(level=logging.CRITICAL, format='%(levelname)s: %(message)s')
        logging.critical(f"Failed init OpLogger/Logger: {e}", exc_info=True); print(f"CRITICAL: {e}", file=sys.stderr); sys.exit(1)

    # --- Logging Level ---
    log_level = logging.DEBUG if args.verbose else logging.INFO; logger.setLevel(log_level)
    try: # Set levels for other loggers
        logging.getLogger("VmManager").setLevel(log_level); logging.getLogger("NetworkManager").setLevel(log_level); logging.getLogger("ResourcePoolManager").setLevel(log_level); logging.getLogger("FolderManager").setLevel(log_level); logging.getLogger("PRTGManager").setLevel(log_level)
    except Exception: pass

    # --- Handle Course/Component Listing (within commands) ---
    if args.course == "?": list_vendor_courses(args.vendor) # Exits
    if getattr(args, 'component', None) == "?":
        if args.command not in ['setup', 'manage']: logger.error(f"Cmd '{args.command}' invalid for component list."); sys.exit(1)
        # Command function handles '?'

    # --- Validate Core Arguments for Action Commands ---
    needs_core_args = args.command in ['setup', 'manage', 'teardown'] and not getattr(args, 'db_only', False) and not getattr(args, 'monitor_only', False) and getattr(args, 'component', '') != '?'
    is_setup_perm_only = args.command == 'setup' and getattr(args, 'perm', False)
    if needs_core_args or is_setup_perm_only:
        missing_args = [];
        if not args.host: missing_args.append("--host")
        is_f5_class_only_op = args.vendor.lower() == 'f5' and getattr(args, 'start_pod', None) is None and getattr(args, 'end_pod', None) is None
        is_f5_class_teardown = args.command == 'teardown' and is_f5_class_only_op; is_f5_class_setup = args.command == 'setup' and is_f5_class_only_op
        if not (is_f5_class_teardown or is_f5_class_setup):
            if getattr(args, 'start_pod', None) is None: missing_args.append("--start-pod (for command)")
            if getattr(args, 'end_pod', None) is None: missing_args.append("--end-pod (for command)")
        if args.vendor.lower() == "f5":
             course_config_temp = None;
             if args.course != '?':
                  try: course_config_temp = fetch_and_prepare_course_config(args.course)
                  except: pass
             if getattr(args, 'class_number', None) is None: # Check attribute safely
                  if course_config_temp and "f5" in course_config_temp.get("course_name","").lower(): missing_args.append("--class_number (F5 courses)")
                  elif args.vendor.lower() == "f5": missing_args.append("--class_number (F5 vendor)")
        if missing_args: err_msg = f"Missing required args for '{args.command}': {', '.join(missing_args)}"; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg); operation_logger.finalize("failed", 0, 0); sys.exit(1)
        cmd_start_pod = getattr(args, 'start_pod', None); cmd_end_pod = getattr(args, 'end_pod', None)
        if cmd_start_pod is not None and cmd_end_pod is not None and cmd_start_pod > cmd_end_pod: err_msg = "Error: start-pod > end-pod."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="invalid_pod_range", error=err_msg); operation_logger.finalize("failed", 0, 0); sys.exit(1)

    # --- Execute Command ---
    start_time = time.perf_counter(); overall_status = "failed"; total_success = 0; total_failure = 0; all_pod_results = []
    try:
        logger.info(f"Executing command: {args.command} (Run ID: {operation_logger.run_id})")
        all_pod_results = args.func(args, operation_logger) # Call function assigned by set_defaults
        if isinstance(all_pod_results, list): # Process results
            total_success = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "success")
            total_failure = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "failed")
            total_skipped = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "skipped")
            if total_failure > 0: overall_status = "completed_with_errors"
            elif total_success > 0: overall_status = "completed"
            elif total_skipped > 0 and total_success == 0 and total_failure == 0: overall_status = "completed"
            else: overall_status = "completed_no_tasks"
        else: logger.warning("Cmd func non-list result. Summary inaccurate."); overall_status = "unknown"
    except KeyboardInterrupt: print("\nTerminated by user."); logger.warning("Terminated by user."); overall_status = "terminated_by_user"; sys.exit(0)
    except Exception as e: logger.critical(f"Unhandled error: {e}", exc_info=True); overall_status = "failed_exception"; operation_logger.log_pod_status(pod_id="main_exception", status="failed", error=f"{type(e).__name__}: {e}"); sys.exit(1)
    finally:
        end_time = time.perf_counter(); duration_minutes = (end_time - start_time) / 60
        logger.info(f"Cmd '{args.command or 'list'}' finished in {duration_minutes:.2f} min.") # Adjust log msg
        if operation_logger: operation_logger.finalize(overall_status, total_success, total_failure)

if __name__ == "__main__":
    main()