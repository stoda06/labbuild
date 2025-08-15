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
import re
from collections import defaultdict
from typing import Optional, Dict, List, Any, Callable, Tuple, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

# Import local modules
from constants import * # Import constants
from logger.log_config import setup_logger # Setup logger function
from operation_logger import OperationLogger # Import logger class
from listing import list_vendor_courses, list_allocations # Import listing functions
# Import command handlers and the special status constant
from commands import setup_environment, teardown_environment, manage_environment, COMPONENT_LIST_STATUS, test_environment, move_environment, migrate_environment
from config_utils import fetch_and_prepare_course_config # Needed for arg validation
# --- NEW IMPORTS ---
from db_utils import mongo_client
from pymongo.errors import PyMongoError
# -----------------------------------------------------------------------------
# Environment Setup
# -----------------------------------------------------------------------------
load_dotenv()
# Get placeholder logger instance early, setup called properly in main()
logger = logging.getLogger('labbuild')

def _create_contiguous_ranges(item_numbers: set) -> List[Tuple[int, int]]:
    """Converts a set of numbers into a list of (start, end) tuples for contiguous ranges."""
    if not item_numbers:
        return []
    
    sorted_nums = sorted(list(item_numbers))
    ranges = []
    start_range = sorted_nums[0]
    end_range = sorted_nums[0]

    for i in range(1, len(sorted_nums)):
        if sorted_nums[i] == end_range + 1:
            end_range = sorted_nums[i]
        else:
            ranges.append((start_range, end_range))
            start_range = sorted_nums[i]
            end_range = sorted_nums[i]
    
    ranges.append((start_range, end_range))
    return ranges


# --- NEW ORCHESTRATION FUNCTION ---
def perform_vendor_level_operation(vendor: str, operation: str, verbose: bool):
    """
    Finds all valid allocations for a vendor, asks for confirmation,
    and then performs a batch operation.
    """
    print(f"--- Starting Vendor-Level Operation ---")
    print(f"Vendor: {vendor.upper()}")
    print(f"Operation: {operation.capitalize()}")
    print("---------------------------------------")
    
    try:
        with mongo_client() as client:
            if not client:
                print("Error: Could not connect to the database.", file=sys.stderr)
                return

            alloc_collection = client[DB_NAME][ALLOCATION_COLLECTION]
            query = {
                "courses.vendor": {"$regex": f"^{re.escape(vendor)}$", "$options": "i"},
                "tag": {"$ne": "untagged"},
                "extend": {"$ne": "true"}
            }
            
            allocations_cursor = alloc_collection.find(query)

            tasks_to_process = defaultdict(lambda: {"pods": set(), "f5_classes": set()})
            
            for doc in allocations_cursor:
                tag = doc.get("tag")
                if not tag: continue

                for course in doc.get("courses", []):
                    c_vendor = course.get("vendor")
                    c_name = course.get("course_name")
                    if not c_vendor or not c_name or c_vendor.lower() != vendor.lower():
                        continue

                    for pd in course.get("pod_details", []):
                        host = pd.get("host")
                        if not host: continue
                        
                        group_key = (tag, c_name, c_vendor, host)
                        
                        if pd.get("class_number") is not None and c_vendor.lower() == 'f5':
                            tasks_to_process[group_key]['f5_classes'].add(pd["class_number"])
                        elif pd.get("pod_number") is not None:
                            tasks_to_process[group_key]['pods'].add(pd["pod_number"])

            if not tasks_to_process:
                print("No valid, tagged, non-extended allocations found for this vendor.")
                return

            final_jobs = []
            for group_key, items in tasks_to_process.items():
                tag, course, c_vendor, host = group_key

                for class_num in sorted(list(items['f5_classes'])):
                    base_args = {'vendor': c_vendor, 'course': course, 'host': host, 'tag': tag, 'verbose': verbose, 'class_number': class_num}
                    if operation == 'rebuild':
                        final_jobs.append({'args': {**base_args, 'command': 'setup', 're_build': True}, 'desc': f"Rebuild Class {class_num} for course '{course}' on host '{host}' (Tag: {tag})"})
                    elif operation == 'teardown':
                        final_jobs.append({'args': {**base_args, 'command': 'teardown'}, 'desc': f"Teardown Class {class_num} for course '{course}' on host '{host}' (Tag: {tag})"})
                    elif operation in ['start', 'stop']:
                         final_jobs.append({'args': {**base_args, 'command': 'manage', 'operation': operation}, 'desc': f"{operation.capitalize()} Class {class_num} for course '{course}' on host '{host}' (Tag: {tag})"})

                pod_ranges = _create_contiguous_ranges(items['pods'])
                for start_pod, end_pod in pod_ranges:
                    range_str = f"{start_pod}" if start_pod == end_pod else f"{start_pod}-{end_pod}"
                    base_args = {'vendor': c_vendor, 'course': course, 'host': host, 'tag': tag, 'verbose': verbose, 'start_pod': start_pod, 'end_pod': end_pod}
                    if c_vendor.lower() == 'f5':
                        base_args['class_number'] = next(iter(items['f5_classes']), None)
                    
                    if operation == 'rebuild':
                        final_jobs.append({'args': {**base_args, 'command': 'setup', 're_build': True}, 'desc': f"Rebuild Pods {range_str} for course '{course}' on host '{host}' (Tag: {tag})"})
                    elif operation == 'teardown':
                        final_jobs.append({'args': {**base_args, 'command': 'teardown'}, 'desc': f"Teardown Pods {range_str} for course '{course}' on host '{host}' (Tag: {tag})"})
                    elif operation in ['start', 'stop']:
                        final_jobs.append({'args': {**base_args, 'command': 'manage', 'operation': operation}, 'desc': f"{operation.capitalize()} Pods {range_str} for course '{course}' on host '{host}' (Tag: {tag})"})
            
            # --- NEW: Confirmation Step ---
            if not final_jobs:
                print("No specific operations could be generated from the found allocations.")
                return

            print("\nThe following operations will be performed:\n")
            for job in sorted(final_jobs, key=lambda x: x['desc']):
                print(f"  - {job['desc']}")
            print(f"\nTotal operations to run: {len(final_jobs)}")
            
            confirm = input("\nAre you sure you want to proceed? (yes/no): ").lower().strip()
            if confirm != 'yes':
                print("\nOperation cancelled by user.")
                return
            # --- END NEW: Confirmation Step ---

            print(f"\nUser confirmed. Submitting {len(final_jobs)} operations now...")
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_job = {}
                for job in final_jobs:
                    print(f" -> Submitting: {job['desc']}")
                    op_logger = OperationLogger(job['args']['command'], job['args'])
                    
                    command_func = None
                    if job['args']['command'] == 'setup': command_func = setup_environment
                    elif job['args']['command'] == 'teardown': command_func = teardown_environment
                    elif job['args']['command'] == 'manage': command_func = manage_environment

                    if command_func:
                        future = executor.submit(command_func, job['args'], op_logger)
                        future_to_job[future] = job['desc']
                
                print("\n--- Waiting for all operations to complete ---")
                for future in as_completed(future_to_job):
                    desc = future_to_job[future]
                    try:
                        future.result()
                        print(f"  [SUCCESS] Operation completed: {desc}")
                    except Exception as exc:
                        print(f"  [FAILURE] Operation failed: {desc}. Error: {exc}")

            print("\n--- Vendor-Level Operation Finished ---")

    except PyMongoError as e:
        print(f"Database Error: Could not query allocations. {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()

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
    parser.add_argument('--vendor-operation',
                        dest='vendor_operation',
                        choices=['rebuild', 'teardown', 'start', 'stop'],
                        help='Perform a batch operation on all valid allocations for the specified vendor (use with -l).')
    # Global verbose flag
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging.')

    # --- Subparsers for Commands ---
    subparsers = parser.add_subparsers(dest='command', title='commands',
                                       help='Action command (setup, manage, teardown, test, move, migrate)')

    # --- Common Arguments for Subparsers ---
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-g', '--course', required=True, help='Course name or "?".')
    common_parser.add_argument('--host', help='Target host.')
    common_parser.add_argument('-t', '--tag', required=True, help='A unique tag for the allocation group (mandatory).')
    common_parser.add_argument('-th', '--thread', type=int, default=4, help='Concurrency thread count.')
    common_parser.add_argument('-v', '--vendor', required=True, help='Vendor code (e.g., pa, cp, f5). Required.')

    # --- Pod Range Arguments (Parent for Commands) ---
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
    setup_parser.add_argument('--clonefrom', type=int, metavar='SOURCE_POD',
                            help='Source pod number to clone VMs from (primarily for Checkpoint).')
    # --- NEW OPTIONAL ARGUMENTS FOR SETUP ---
    setup_parser.add_argument('--start-date', help='Course start date (YYYY-MM-DD).')
    setup_parser.add_argument('--end-date', help='Course end date (YYYY-MM-DD).')
    setup_parser.add_argument('--trainer-name', help='Name of the trainer.')
    setup_parser.add_argument('--username', help='APM username for the allocation.')
    setup_parser.add_argument('--password', help='APM password for the allocation.')
    # --- END NEW ARGUMENTS ---
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

    # -------------------------------
    # Test Subcommand
    # -------------------------------
    test_parser = subparsers.add_parser("test", help="Run test suite for labs", parents=[f5_parser])
    test_parser.add_argument("--db", action='store_true', help="List allocations from the database with test-related info.")
    test_parser.add_argument("-t", "--tag", help="Run tests for a specific allocation by tag name.")
    test_parser.add_argument("-v", "--vendor", help="Vendor name. Used for vendor-wide tests or to filter --db list.")
    test_parser.add_argument("-s", "--start_pod", type=int, help="Start pod/class number. Used for range tests or to filter --db list.")
    test_parser.add_argument("-e", "--end_pod", type=int, help="End pod/class number. Used for range tests or to filter --db list.")
    test_parser.add_argument("-H", "--host", help="ESXi host name (optional, used to filter manual range tests).")
    test_parser.add_argument("-g", "--group", help="Course group/section (optional, used to filter manual range tests).")
    test_parser.add_argument("-c", "--component", help="Test specific component(s), or '?' to list available.")
    test_parser.add_argument("-x", "--exclude", help="Exclude pods/classes from vendor-wide test. E.g., '1-5,10,22-25'")
    test_parser.set_defaults(func=test_environment)

    move_parser = subparsers.add_parser('move', help='Move pod VMs to correct folder and resource pool.',
                                        parents=[common_parser, pod_range_parser, f5_parser])
    move_parser.set_defaults(func=move_environment)

    migrate_parser = subparsers.add_parser('migrate', help='Migrate pod VMs from a source host to a destination host.',
                                           parents=[common_parser, pod_range_parser])
    migrate_parser.add_argument('-d', '--destination-host', required=True, help='The destination host for the migration.')
    migrate_parser.set_defaults(func=migrate_environment)

    argcomplete.autocomplete(parser)

    try:
        args = parser.parse_args()
    except SystemExit as e:
        sys.exit(e.code)
    # --- End Argument Parsing ---


    # --- Mode Handling: Listing or Command ---
    if args.list_allocations and args.vendor_operation:
        if not args.vendor:
            # --- MODIFICATION: Updated error message for clarity ---
            parser.error("Vendor-level operations (-l --vendor-operation) require the vendor (-v) argument.")
        logger = setup_logger() # Minimal setup
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logger.setLevel(log_level)
        perform_vendor_level_operation(args.vendor, args.vendor_operation, args.verbose)
        sys.exit(0)
    # --- END UPDATED LOGIC ---
    elif args.list_allocations: # Original listing logic
        if not args.vendor:
             parser.error("Listing allocations (-l) requires the vendor (-v) argument.")
        logger = setup_logger()
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logger.setLevel(log_level)
        logger.info(f"Starting list_allocations for vendor: {args.vendor}")
        list_allocations(args.vendor, args.list_start_pod, args.list_end_pod, test_mode=args.test)
        sys.exit(0)

    # If not listing, a command is required
    if not args.command:
        if args.test:
            parser.error("--test flag can only be used with --list-allocations (-l).")
        else:
            # No command and not listing
            parser.error("A command (setup, manage, teardown, test) is required if not using -l.")
    

    # Ensure --test is not used with commands
    if args.test and args.command:
        parser.error("--test flag cannot be used with commands (setup, manage, teardown).")
    
    # Ensure --operation is not used with commands
    if hasattr(args, 'vendor_operation') and args.vendor_operation and args.command:
         parser.error("--vendor-operation flag cannot be used with commands (setup, manage, teardown). Use with -l.")

    # --- Vendor is Required for Commands (also checked in common_parser parent) ---
    if args.command in ['setup', 'manage', 'teardown'] and not args.vendor:
         parser.error("The vendor argument (-v) is required for commands (setup, manage, teardown).")


    # --- Convert args Namespace to Dictionary *once* ---
    # Pass the full namespace as a dictionary for simplicity and robustness.
    args_dict = vars(args)

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
        # Add new logger for test utils
        logging.getLogger("labbuild.test.utils").setLevel(log_level)
    except Exception as e:
        logger.warning(f"Could not set log level for all sub-loggers: {e}")


    # --- Handle Course Listing "?" ---
    if hasattr(args, "course") and args.course == "?":
        logger.info(f"Request to list courses for vendor '{args.vendor}'.")
        list_vendor_courses(args.vendor)
        sys.exit(0)



    # --- Validate Core Arguments for Commands (Conditional) ---
    # Note: args_dict now holds the arguments relevant to the command
    # This section does not apply to the `test` command, which has its own validation
    if args.command in ['setup', 'manage', 'teardown', 'move', 'migrate']:
        is_listing_components = args_dict.get('component') == "?"
        is_db_only = args_dict.get('db_only', False)
        is_monitor_only = args_dict.get('monitor_only', False)
        is_perm_only = args.command == 'setup' and args_dict.get('perm', False)

        # Core args needed if not listing components and not in special mode
        needs_core_args = not is_listing_components and \
                        not is_db_only and not is_monitor_only and not is_perm_only

        missing_args = []  # Initialize here to avoid UnboundLocalError

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
        
        if args.command == 'migrate':
            if not args_dict.get('destination_host'):
                missing_args.append("--destination-host (Required for migrate operation)")

        if missing_args:
            err_msg = f"Missing required args for '{args.command}': {', '.join(missing_args)}"
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
            if operation_logger and not operation_logger._is_finalized:
                 operation_logger.finalize("failed", 0, 0)
            sys.exit(1)
        logger.debug("Core arguments validated successfully.")
    # --- END OF CORRECTED LOGIC ---


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
        
        # --- THIS IS THE CORRECTED LOGIC FOR CALCULATING TEST SUMMARY ---
        if args.command == 'test':
            if isinstance(all_results, list):
                # Group results by the job identifier (pod or class)
                # A "job" is one run of the test script, e.g., for one pod or one F5 class.
                results_by_job = defaultdict(list)
                for r in all_results:
                    if isinstance(r, dict):
                        # For F5, the job is the class. For others, it's the pod.
                        # The `host` key helps differentiate jobs on different hosts (e.g., prod vs. tp)
                        job_id = (r.get('class', r.get('pod')), r.get('host'))
                        if job_id[0] is not None:
                            results_by_job[job_id].append(r)
                
                # Determine final status for each job
                for job_id, results in results_by_job.items():
                    # A job fails if ANY of its component checks fail (are not UP/SUCCESS/OPEN/SKIPPED)
                    # Note: The status 'FAILED' is also a failure condition.
                    if any(res.get('status', '').upper() not in ['UP', 'SUCCESS', 'OPEN'] and not res.get('status', '').upper().startswith('SKIPPED') for res in results):
                        total_failure += 1
                    else:
                        total_success += 1
            else: # Fallback if test function returns unexpected data type
                total_failure = 1
        
        elif component_list_result:
            logger.info(f"Command returned component list for course '{args_dict.get('course')}'.")
            print("\nAvailable components:")
            for comp_name in component_list_result.get("components", []):
                print(f"  - {comp_name}")
            print("\nUse the -c flag with one or more component names (comma-separated).")
            # No pod actions taken for component listing
            total_success, total_failure, total_skipped = 0, 0, 0 
        elif isinstance(all_results, list):
            # Original processing for setup, teardown, manage
            total_success = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "success")
            total_failure = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "failed")
            total_skipped = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") in ["skipped", "cancelled", "completed_no_tasks", "completed_db_list"])
        else:
            logger.warning(f"Command function '{args.command}' did not return a list. Result: {all_results}")
            overall_status = "unknown"
            
        # Determine overall status based on counts
        if total_failure > 0: 
            overall_status = "completed_with_errors"
        elif total_success > 0 or (total_skipped > 0 and not all_results):
            overall_status = "completed"
        elif not all_results: 
            overall_status = "completed_no_tasks"
        # Handle cases where all results are skipped
        elif len(all_results) > 0 and (total_success + total_failure == 0):
             overall_status = "completed"
        else: # Fallback
            overall_status = "completed"

        logger.info(f"Command '{args.command}' result summary: Success={total_success}, Failed={total_failure}, Skipped={total_skipped}")

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