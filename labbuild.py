#!/usr/bin/env python3
"""
Lab Build Management Tool - Main Entry Point
Parses arguments and dispatches actions to command handlers.
"""

import logging
import sys
import time
from collections import defaultdict
from dotenv import load_dotenv

from logger.log_config import setup_logger
from operation_logger import OperationLogger
from listing import list_vendor_courses, list_allocations
from commands import COMPONENT_LIST_STATUS
from arg_parser import create_parser
from batch_operations import perform_vendor_level_operation
from auto_operations import run_auto_lookup_operation

# --- Environment & Logger Setup ---
load_dotenv()
logger = logging.getLogger('labbuild')


def main():
    """Main execution function."""
    global logger
    
    parser = create_parser()
    try:
        args = parser.parse_args()
    except SystemExit as e:
        sys.exit(e.code)

    # --- Mode 1: Listing or Batch Operation ---
    if args.list_allocations:
        if not args.vendor:
            parser.error("Listing allocations (-l) requires the vendor (-v) argument.")
        
        logger = setup_logger()
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logger.setLevel(log_level)

        if args.vendor_operation:
            perform_vendor_level_operation(
                args.vendor, 
                args.vendor_operation, 
                args.verbose,
                start_pod_filter=args.list_start_pod,
                end_pod_filter=args.list_end_pod
            )
        else:
            list_allocations(
                args.vendor, 
                args.list_start_pod, 
                args.list_end_pod, 
                test_mode=args.test
            )
        sys.exit(0)

    # --- Mode 2: Command Execution ---
    if not args.command:
        parser.error("A command (setup, manage, teardown, test) is required if not using -l.")

    # --- Mode 2a: Auto-Lookup Command Execution ---
    # This mode is triggered when course/tag are omitted but a pod range is provided.
    is_auto_lookup_mode = (
        args.command in ['setup', 'manage', 'teardown'] and
        args.start_pod is not None and
        args.end_pod is not None and
        args.course is None and
        args.tag is None
    )

    if is_auto_lookup_mode:
        logger = setup_logger()
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logger.setLevel(log_level)
        run_auto_lookup_operation(vars(args))
        sys.exit(0)

    # --- Mode 2b: Standard (Manual) Command Execution ---
    args_dict = vars(args)
    operation_logger = OperationLogger(args.command, args_dict)
    run_id_for_logs = operation_logger.run_id
    logger = setup_logger(run_id=run_id_for_logs)
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger.setLevel(log_level)
    
    # Configure levels for all sub-loggers
    all_loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith('labbuild')]
    for sub_logger in all_loggers:
        sub_logger.setLevel(log_level)
    
    if hasattr(args, "course") and args.course == "?":
        list_vendor_courses(args.vendor)
        sys.exit(0)

    # --- Validation for Standard (Manual) Command Runs ---
    # At this point, we know it's not a list/batch operation or an auto-lookup operation.
    # Therefore, it must be a manual run, which has its own set of required arguments.
    if args.command in ['setup', 'manage', 'teardown', 'move', 'migrate']:
        
        # 1. Check for core identifiers: course and tag.
        # If we are in this part of the code, auto-lookup conditions were not met.
        # So, if course or tag are missing, the command is ambiguous.
        if not args.course or not args.tag:
            parser.error(
                f"For a manual '{args.command}' operation, you must provide both --course (-g) and --tag (-t).\n"
                f"To run on an existing allocation without knowing the course/tag, omit them and instead provide --start-pod and --end-pod to trigger auto-lookup."
            )
        
        # 2. Check for other required arguments for a standard run (not db-only, etc.)
        is_f5 = args.vendor.lower() == 'f5'
        is_special_mode = (
            args_dict.get('db_only') or
            args_dict.get('monitor_only') or
            (args.command == 'setup' and args_dict.get('perm')) or
            args_dict.get('component') == '?' or
            args_dict.get('course') == '?'
        )
        
        if not is_special_mode:
            if not args.host:
                parser.error(f"--host is required for a standard '{args.command}' operation.")
            if is_f5 and args.class_number is None:
                parser.error(f"--class_number is required for standard F5 '{args.command}' operations.")
            if not is_f5 and (args.start_pod is None or args.end_pod is None):
                parser.error(f"--start-pod and --end-pod are required for standard non-F5 '{args.command}' operations.")

    # --- Execute Command ---
    start_time = time.perf_counter()
    overall_status, total_success, total_failure, total_skipped = "failed", 0, 0, 0
    all_results = []
    
    logger.info(f"Executing command: {args.command} (Run ID: {run_id_for_logs})")
    try:
        if not hasattr(args, 'func'):
            logger.critical(f"Internal error: No function mapped for command '{args.command}'.")
            sys.exit(1)

        all_results = args.func(args_dict, operation_logger)
        
        # --- Process Results ---
        if any(r.get("status") == COMPONENT_LIST_STATUS for r in all_results if isinstance(r, dict)):
            print("\nUse the -c flag with one or more component names (comma-separated).")
        elif isinstance(all_results, list):
            total_success = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "success")
            total_failure = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "failed")
            total_skipped = sum(1 for r in all_results if isinstance(r, dict) and "skip" in r.get("status", "").lower())
        
        if total_failure > 0:
            overall_status = "completed_with_errors"
        elif total_success > 0 or total_skipped > 0:
            overall_status = "completed"
        elif not all_results:
            overall_status = "completed_no_tasks"

        logger.info(f"Summary: Success={total_success}, Failed={total_failure}, Skipped={total_skipped}")

    except KeyboardInterrupt:
        print("\nTerminated by user.")
        overall_status = "terminated_by_user"
    except Exception as e:
        logger.critical(f"Unhandled error during command execution: {e}", exc_info=True)
        overall_status = "failed_exception"
    finally:
        if operation_logger and not operation_logger._is_finalized:
            end_time = time.perf_counter()
            duration_minutes = (end_time - start_time) / 60
            logger.info(f"Cmd '{args.command}' finished in {duration_minutes:.2f} min. Final Status: {overall_status}")
            operation_logger.finalize(overall_status, total_success, total_failure)

if __name__ == "__main__":
    main()