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
    global logger # Make sure we're using the global logger

    # --- Argument Parsing ---
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

    # Common Arguments for Subparsers
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

    # Subparsers
    setup_parser = subparsers.add_parser('setup', help='Set up lab.', parents=[common_parser, pod_range_parser, f5_parser])
    setup_parser.add_argument('-c', '--component', help='Components or "?" to list.'); setup_parser.add_argument('-ds', '--datastore', default="vms"); setup_parser.add_argument('-r', '--re-build', action='store_true'); setup_parser.add_argument('-mem', '--memory', type=int); setup_parser.add_argument('--full', action='store_true'); setup_parser.add_argument('--monitor-only', action='store_true'); setup_parser.add_argument('--prtg-server'); setup_parser.add_argument('--perm', action='store_true'); setup_parser.add_argument('--db-only', action='store_true'); setup_parser.set_defaults(func=setup_environment)
    manage_parser = subparsers.add_parser('manage', help='Manage VM power.', parents=[common_parser, pod_range_parser, f5_parser])
    manage_parser.add_argument('-c', '--component', help='Components or "?".'); manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True); manage_parser.set_defaults(func=manage_environment)
    teardown_parser = subparsers.add_parser('teardown', help='Tear down lab.', parents=[common_parser, pod_range_parser, f5_parser])
    teardown_parser.add_argument('--monitor-only', action='store_true'); teardown_parser.add_argument('--db-only', action='store_true'); teardown_parser.set_defaults(func=teardown_environment)

    try:
        args = parser.parse_args()
        # print(f"\nDEBUG: Final parsed args = {vars(args)}\n")
    except SystemExit as e:
        # print(f"\nDEBUG: Argparse exited with code: {e.code}\n")
        sys.exit(e.code)
    # --- End Argument Parsing ---


    # --- Mode Handling: Listing or Command ---
    if args.list_allocations:
        logger = setup_logger() # Minimal logger for listing
        log_level = logging.DEBUG if getattr(args,'verbose', False) else logging.INFO
        logger.setLevel(log_level)
        logger.info(f"Starting list_allocations for vendor: {args.vendor}")
        list_allocations(args.vendor, args.start_pod, args.end_pod, test_mode=args.test)
        sys.exit(0) # Exit after listing

    if not args.command:
        if args.test: parser.error("--test flag can only be used with --list-allocations (-l).")
        else: parser.error("A command (setup, manage, teardown) is required if not using -l.")

    if args.test: parser.error("--test flag cannot be used with commands (setup, manage, teardown).")


    # --- Initialize Operation Logger ---
    operation_logger = None
    run_id_for_logs = None
    try:
        args_dict = vars(args).copy(); args_dict.pop('func', None)
        operation_logger = OperationLogger(args.command, args_dict)
        run_id_for_logs = operation_logger.run_id
        print(f"DEBUG: OperationLogger initialized. Run ID: {run_id_for_logs}")
        # Setup logger AFTER getting run_id
        logger = setup_logger(run_id=run_id_for_logs)
    except Exception as e:
        logging.basicConfig(level=logging.CRITICAL, format='%(levelname)s: %(message)s')
        logging.critical(f"CRITICAL FAILURE during logger/OpLogger initialization: {e}", exc_info=True)
        print(f"CRITICAL: Failed init OpLogger/Logger: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Logging Level ---
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger.setLevel(log_level)
    print(f"DEBUG: Logger level set to {logging.getLevelName(log_level)}")
    try: # Set levels for other known loggers
        logging.getLogger("VmManager").setLevel(log_level)
        logging.getLogger("NetworkManager").setLevel(log_level)
        # Add others as needed
    except Exception: pass


    # --- Handle Course Listing "?" ---
    # The actual listing and exit happens inside the command functions now
    if args.course == "?":
        # Check if a command function is set, otherwise it's an error state
        if hasattr(args, 'func'):
            logger.info(f"Passing '?' for course to command '{args.command}'")
            # Let the command function handle the "?" for course (like list_vendor_courses does)
            # The command function itself should call sys.exit() after listing.
            args.func(args, operation_logger) # Call the function to handle "?"
            # If the function didn't exit, something is wrong.
            logger.error("Command function did not exit after handling course='?'.")
            sys.exit(1) # Exit here just in case
        else:
             logger.error("No command specified when using course='?'.")
             parser.print_usage()
             sys.exit(1)


    # --- Validate Core Arguments ---
    # Note: Component listing '?' is handled inside the command functions now
    is_component_list_request = getattr(args, 'component', None) == "?"
    needs_core_args = args.command in ['setup', 'manage', 'teardown'] and \
                      not getattr(args, 'db_only', False) and \
                      not getattr(args, 'monitor_only', False) and \
                      not is_component_list_request # Don't validate core args if just listing components

    is_setup_perm_only = args.command == 'setup' and getattr(args, 'perm', False)

    if needs_core_args or is_setup_perm_only:
        logger.debug("Validating core arguments...")
        missing_args = []
        if not args.host: missing_args.append("--host")

        is_f5_vendor = args.vendor.lower() == 'f5'
        has_pod_range = getattr(args, 'start_pod', None) is not None and getattr(args, 'end_pod', None) is not None
        has_class_number = getattr(args, 'class_number', None) is not None

        # F5 validation
        if is_f5_vendor and not has_class_number:
            # F5 setup/teardown/manage almost always need class number
             if args.command in ['setup', 'manage', 'teardown']:
                 missing_args.append("--class_number (Required for F5 operations)")
        # Non-F5 validation
        elif not is_f5_vendor and not has_pod_range:
             # Standard ops for other vendors usually need a pod range
             if args.command in ['setup', 'manage', 'teardown']:
                 missing_args.append("--start-pod / --end-pod (Required for non-F5 operations)")

        # Pod range sanity check
        cmd_start_pod = getattr(args, 'start_pod', None); cmd_end_pod = getattr(args, 'end_pod', None)
        if cmd_start_pod is not None and cmd_end_pod is not None and cmd_start_pod > cmd_end_pod:
            err_msg = "Error: start-pod cannot be greater than end-pod."; logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="invalid_pod_range", error=err_msg)
            operation_logger.finalize("failed", 0, 0); sys.exit(1)

        if missing_args:
            err_msg = f"Missing required args for '{args.command}': {', '.join(missing_args)}"; logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
            operation_logger.finalize("failed", 0, 0); sys.exit(1)
        logger.debug("Core arguments validated successfully.")


    # --- Execute Command ---
    start_time = time.perf_counter()
    overall_status = "failed" # Default status
    total_success = 0; total_failure = 0
    all_pod_results = []

    logger.info(f"Executing command: {args.command} (Run ID: {run_id_for_logs})")

    try:
        command_func = args.func
        # The command function (e.g., setup_environment) will handle '?' component listing and exit if needed
        all_pod_results = command_func(args, operation_logger)

        # Process results if the function returned normally (didn't exit)
        if isinstance(all_pod_results, list):
            total_success = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "success")
            total_failure = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "failed")
            total_skipped = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "skipped")
            if total_failure > 0: overall_status = "completed_with_errors"
            elif total_success > 0: overall_status = "completed"
            elif total_skipped > 0 and total_success == 0 and total_failure == 0: overall_status = "completed"
            elif total_success == 0 and total_failure == 0 and total_skipped == 0: overall_status = "completed_no_tasks"
            else: overall_status = "completed" # Default to completed if no clear success/fail/skip
            logger.info(f"Command '{args.command}' result summary: Success={total_success}, Failed={total_failure}, Skipped={total_skipped}")
        else:
            # This case should ideally not happen if command functions return lists or exit
            logger.warning(f"Command function for '{args.command}' did not return a list or exit as expected. Result summary inaccurate.")
            overall_status = "unknown"

    except KeyboardInterrupt:
        print("\nTerminated by user.")
        logger.warning("Terminated by user.")
        overall_status = "terminated_by_user"
        # Finalize will be called in finally block
    except Exception as e:
        logger.critical(f"Unhandled error during command execution: {e}", exc_info=True)
        overall_status = "failed_exception"
        if operation_logger: operation_logger.log_pod_status(pod_id="main_exception", status="failed", error=f"Unhandled: {type(e).__name__}: {e}")
        # Finalize will be called in finally block
    finally:
        # Ensure finalize is called ONCE, unless it was already called internally
        # (e.g., by component listing exit)
        if operation_logger and not operation_logger.finalize:
            end_time = time.perf_counter(); duration_minutes = (end_time - start_time) / 60
            logger.info(f"Cmd '{args.command}' finished in {duration_minutes:.2f} min. Overall status: {overall_status}")
            operation_logger.finalize(overall_status, total_success, total_failure)
        elif not operation_logger:
             logger.error("OperationLogger not initialized, cannot finalize run summary.")
        # else: Finalize was called internally within the command function (e.g., during '?')

if __name__ == "__main__":
    main()

# --- END OF FILE labbuild.py ---