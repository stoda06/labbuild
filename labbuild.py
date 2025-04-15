# --- START OF FILE labbuild.py ---

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
# Import command handlers and the special status constant
from commands import setup_environment, teardown_environment, manage_environment, COMPONENT_LIST_STATUS
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

    # --- Arguments for Listing Mode (-l) ---
    # These are defined here because they are used WITHOUT a subcommand
    parser.add_argument('-l', '--list-allocations', action='store_true',
                        help='List current pod allocations for a vendor.')
    parser.add_argument('-v', '--vendor', # Vendor is required even for listing
                        help='Vendor code (e.g., pa, cp, f5). Required.')
    # Add -s and -e specifically for the listing use case here
    parser.add_argument('--list-start-pod', type=int, metavar='START_POD',
                        help='Starting pod/class number for filtering list.')
    parser.add_argument('--list-end-pod', type=int, metavar='END_POD',
                        help='Ending pod/class number for filtering list.')
    parser.add_argument('--test', action='store_true',
                        help='Test allocation validity: check VR VM existence and remove invalid DB entries (use with -l).')
    # Global verbose flag
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging.')

    # --- Subparsers for Commands ---
    subparsers = parser.add_subparsers(dest='command', title='commands',
                                       help='Action command (setup, manage, teardown)')

    # --- Common Arguments for Subparsers ---
    # These are inherited by setup, manage, teardown
    common_parser = argparse.ArgumentParser(add_help=False)
    # Make course required directly here as it's needed by all commands
    common_parser.add_argument('-g', '--course', required=True, help='Course name or "?".')
    # Host is not universally required (e.g., db_only), validated later
    common_parser.add_argument('--host', help='Target host.')
    common_parser.add_argument('-t', '--tag', default="untagged", help='Tag.')
    # common_parser.add_argument('--verbose', action='store_true', help='Enable debug logging.') # Moved to main parser
    common_parser.add_argument('-th', '--thread', type=int, default=4, help='Concurrency thread count.')
    # Add vendor here too, as it's required for all commands
    common_parser.add_argument('-v', '--vendor', required=True, help='Vendor code (e.g., pa, cp, f5). Required.')

    # --- Pod Range Arguments (Parent for Commands) ---
    # Defines -s and -e specifically for commands (setup, manage, teardown)
    pod_range_parser = argparse.ArgumentParser(add_help=False)
    pod_range_parser.add_argument('-s', '--start-pod', type=int, help='Start pod # for action.')
    pod_range_parser.add_argument('-e', '--end-pod', type=int, help='End pod # for action.')

    # --- F5 Specific Arguments (Parent for Commands) ---
    f5_parser = argparse.ArgumentParser(add_help=False)
    f5_parser.add_argument('-cn', '--class_number', type=int, help='Class number (required for F5).')

    # --- Define Subparsers with Parents ---
    setup_parser = subparsers.add_parser('setup', help='Set up lab environment.',
                                         parents=[common_parser, pod_range_parser, f5_parser])
    setup_parser.add_argument('-c', '--component', help='Specify components or use "?" to list.')
    setup_parser.add_argument('-ds', '--datastore', default="vms", help='Target datastore.')
    setup_parser.add_argument('-r', '--re-build', action='store_true', help='Force delete existing components before build.')
    setup_parser.add_argument('-mem', '--memory', type=int, help='Specify memory for specific components (e.g., F5 pod).')
    setup_parser.add_argument('--full', action='store_true', help='Perform a full clone instead of linked.')
    setup_parser.add_argument('--monitor-only', action='store_true', help='Only create/update monitoring entries.')
    setup_parser.add_argument('--prtg-server', help='Specify target PRTG server for monitoring.')
    setup_parser.add_argument('--perm', action='store_true', help='Only apply permissions (specific vendors).')
    setup_parser.add_argument('--db-only', action='store_true', help='Only update the database allocation record.')
    setup_parser.set_defaults(func=setup_environment)

    manage_parser = subparsers.add_parser('manage', help='Manage VM power states.',
                                          parents=[common_parser, pod_range_parser, f5_parser])
    manage_parser.add_argument('-c', '--component', help='Specify components or use "?" to list.')
    manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True, help='Power operation to perform.')
    manage_parser.set_defaults(func=manage_environment)

    teardown_parser = subparsers.add_parser('teardown', help='Tear down lab environment.',
                                            parents=[common_parser, pod_range_parser, f5_parser])
    teardown_parser.add_argument('--monitor-only', action='store_true', help='Only remove monitoring entries.')
    teardown_parser.add_argument('--db-only', action='store_true', help='Only remove database allocation record.')
    teardown_parser.set_defaults(func=teardown_environment)

    argcomplete.autocomplete(parser)

    try:
        args = parser.parse_args()
    except SystemExit as e:
        sys.exit(e.code)
    # --- End Argument Parsing ---


    # --- Mode Handling: Listing or Command ---
    if args.list_allocations:
        if not args.vendor:
             parser.error("Listing allocations (-l) requires the vendor (-v) argument.")
        logger = setup_logger() # Minimal logger setup for listing
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logger.setLevel(log_level)
        logger.info(f"Starting list_allocations for vendor: {args.vendor}")
        # Use the specific args for listing
        list_allocations(args.vendor, args.list_start_pod, args.list_end_pod, test_mode=args.test)
        sys.exit(0) # Exit after listing

    # If not listing, a command is required
    if not args.command:
        if args.test:
            parser.error("--test flag can only be used with --list-allocations (-l).")
        else:
            # No command and not listing
            parser.error("A command (setup, manage, teardown) is required if not using -l.")

    # Ensure --test is not used with commands
    if args.test and args.command:
        parser.error("--test flag cannot be used with commands (setup, manage, teardown).")

    # --- Vendor is Required for Commands (also checked in common_parser parent) ---
    if args.command and not args.vendor:
         parser.error("The vendor argument (-v) is required for commands (setup, manage, teardown).")


    # --- Convert args Namespace to Dictionary *once* ---
    # Exclude list-specific args and 'func'
    args_dict = {k: v for k, v in vars(args).items() if k not in ['func', 'list_start_pod', 'list_end_pod'] and v is not None}

    # --- Initialize Operation Logger ---
    operation_logger = None
    run_id_for_logs = None
    try:
        # Pass the pre-filtered args_dict
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
    # Set levels for sub-loggers (no changes needed here)
    # ... (rest of logger level setting code) ...
    try: # Set levels for other known loggers used by managers/labs
        logging.getLogger("VmManager").setLevel(log_level)
        logging.getLogger("NetworkManager").setLevel(log_level)
        logging.getLogger("ResourcePoolManager").setLevel(log_level)
        logging.getLogger("FolderManager").setLevel(log_level)
        logging.getLogger("PRTGManager").setLevel(log_level)
        logging.getLogger("labbuild.commands").setLevel(log_level)
        logging.getLogger("labbuild.orchestrator").setLevel(log_level)
        logging.getLogger("labbuild.db").setLevel(log_level)
        logging.getLogger("labbuild.config").setLevel(log_level)
        logging.getLogger("labbuild.vcenter").setLevel(log_level)
        logging.getLogger("labbuild.listing").setLevel(log_level)
        logging.getLogger("labbuild.utils").setLevel(log_level)
        logging.getLogger("labbuild.oplogger").setLevel(log_level)
    except Exception as e:
        logger.warning(f"Could not set log level for all sub-loggers: {e}")


    # --- Handle Course Listing "?" ---
    if args.course == "?":
        logger.info(f"Request to list courses for vendor '{args.vendor}'.")
        # Call list_vendor_courses which handles printing and exiting
        list_vendor_courses(args.vendor)
        # The above function exits, so code below won't run in this case
        sys.exit(0) # Explicit exit just in case


    # --- Validate Core Arguments for Commands (Conditional) ---
    # Note: args_dict now holds the arguments relevant to the command
    is_listing_components = args_dict.get('component') == "?"
    # is_listing_courses handled above
    is_db_only = args_dict.get('db_only', False)
    is_monitor_only = args_dict.get('monitor_only', False)
    is_perm_only = args.command == 'setup' and args_dict.get('perm', False)

    # Core args needed if not listing components and not in special mode
    needs_core_args = not is_listing_components and \
                      not is_db_only and not is_monitor_only and not is_perm_only

    if needs_core_args:
        logger.debug("Validating core arguments for standard operation...")
        missing_args = []
        # Host is now defined in common_parser, check presence in args_dict
        if 'host' not in args_dict: missing_args.append("--host")

        # Access start/end pod and class number from args_dict for validation
        cmd_start_pod = args_dict.get('start_pod')
        cmd_end_pod = args_dict.get('end_pod')
        cmd_class_number = args_dict.get('class_number')

        is_f5_vendor = args.vendor.lower() == 'f5' # Use args.vendor as it's always present
        has_pod_range = cmd_start_pod is not None and cmd_end_pod is not None
        has_class_number = cmd_class_number is not None

        # F5 validation
        if is_f5_vendor and not has_class_number:
            missing_args.append("--class_number (Required for F5 operations)")
        # Non-F5 validation
        elif not is_f5_vendor and not has_pod_range:
            missing_args.append("--start-pod / --end-pod (Required for non-F5 operations)")

        # Pod range sanity check
        if cmd_start_pod is not None and cmd_end_pod is not None:
             try: # Ensure they are comparable (should be ints if parsing worked)
                 if int(cmd_start_pod) > int(cmd_end_pod):
                     err_msg = "Error: start-pod cannot be greater than end-pod."
                     logger.critical(err_msg)
                     operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="invalid_pod_range", error=err_msg)
                     if operation_logger and not operation_logger._is_finalized:
                          operation_logger.finalize("failed", 0, 0)
                     sys.exit(1)
             except ValueError:
                  # Should not happen if type=int is used, but defensive check
                  logger.critical("Invalid non-integer value received for start/end pod.")
                  if operation_logger and not operation_logger._is_finalized:
                       operation_logger.finalize("failed", 0, 0)
                  sys.exit(1)


        if missing_args:
            err_msg = f"Missing required args for '{args.command}': {', '.join(missing_args)}"; logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
            if operation_logger and not operation_logger._is_finalized:
                 operation_logger.finalize("failed", 0, 0)
            sys.exit(1)
        logger.debug("Core arguments validated successfully.")


    # --- Execute Command ---
    start_time = time.perf_counter()
    overall_status = "failed" # Default status
    total_success = 0; total_failure = 0; total_skipped = 0
    all_results = []

    logger.info(f"Executing command: {args.command} (Run ID: {run_id_for_logs})")

    try:
        if not hasattr(args, 'func'):
             logger.critical(f"Internal error: No function mapped for command '{args.command}'.")
             sys.exit(1)

        command_func = args.func
        # Pass the args_dict to the command function
        all_results = command_func(args_dict, operation_logger)

        # --- Process results (checking for component list status) ---
        component_list_result = next((r for r in all_results if isinstance(r,dict) and r.get("status") == COMPONENT_LIST_STATUS), None)
        if component_list_result:
            logger.info(f"Command returned component list for course '{args_dict.get('course')}'.")
            print("\nAvailable components:")
            for comp_name in component_list_result.get("components", []):
                print(f"  - {comp_name}")
            print("\nUse the -c flag with one or more component names (comma-separated).")
            overall_status = "completed" # Listing is considered complete
            total_success, total_failure, total_skipped = 0, 0, 0 # No pod actions taken
        elif isinstance(all_results, list):
            # Normal processing
            total_success = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "success")
            total_failure = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "failed")
            total_skipped = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "skipped")

            if total_failure > 0: overall_status = "completed_with_errors"
            elif total_success > 0: overall_status = "completed"
            elif total_skipped > 0: overall_status = "completed" # Includes skipped only case
            elif not all_results: overall_status = "completed_no_tasks"
            else: overall_status = "completed" # Default catch-all
            logger.info(f"Command '{args.command}' result summary: Success={total_success}, Failed={total_failure}, Skipped={total_skipped}")
        else:
            logger.warning(f"Command function '{args.command}' did not return a list. Result: {all_results}")
            overall_status = "unknown"

    except KeyboardInterrupt:
        print("\nTerminated by user.")
        logger.warning(f"Run {run_id_for_logs} terminated by user.")
        overall_status = "terminated_by_user"
    except Exception as e:
        logger.critical(f"Unhandled error during command execution (Run ID: {run_id_for_logs}): {e}", exc_info=True)
        overall_status = "failed_exception"
        if operation_logger:
            operation_logger.log_pod_status(pod_id="main_execution", status="failed", step="unhandled_exception", error=f"{type(e).__name__}: {e}")
    finally:
        # Finalize the operation log
        if operation_logger and not operation_logger._is_finalized:
            end_time = time.perf_counter()
            duration_minutes = (end_time - start_time) / 60
            logger.info(f"Cmd '{args.command}' (Run ID: {run_id_for_logs}) finished in {duration_minutes:.2f} min. Final Overall Status: {overall_status}")
            operation_logger.finalize(overall_status, total_success, total_failure)
        elif not operation_logger:
             logger.error("OperationLogger not initialized, cannot finalize run summary.")

if __name__ == "__main__":
    main()

# --- END OF FILE labbuild.py ---