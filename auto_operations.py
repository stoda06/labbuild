import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

from db_utils import get_allocations_by_range
from operation_logger import OperationLogger
from commands import setup_environment, teardown_environment, manage_environment

logger = logging.getLogger('labbuild.auto_ops')

def run_auto_lookup_operation(args_dict: Dict[str, Any]):
    """
    Orchestrates an operation (setup, manage, teardown) by looking up
    target details from the database based on a vendor and pod range.
    """
    command = args_dict['command']
    vendor = args_dict['vendor']
    start_pod = args_dict['start_pod']
    end_pod = args_dict['end_pod']
    
    print(f"--- Auto-Lookup Mode Activated ---")
    print(f"Command: {command.capitalize()}")
    print(f"Vendor: {vendor.upper()}")
    print(f"Pod/Class Range: {start_pod}-{end_pod}")
    print("------------------------------------")
    
    # 1. Fetch matching allocations from the database
    jobs_to_run = get_allocations_by_range(vendor, start_pod, end_pod)

    if not jobs_to_run:
        print("No matching allocations found in the database for the specified vendor and range.")
        return

    # 2. Display the plan to the user for confirmation
    print("\nThe following operations will be performed:\n")
    for job in jobs_to_run:
        job_args = {**args_dict, **job} # Merge base args with looked-up args
        range_str = f"{job['start_pod']}" if job['start_pod'] == job['end_pod'] else f"{job['start_pod']}-{job['end_pod']}"
        desc = (
            f"{command.capitalize()} Pods {range_str} for course '{job['course_name']}' "
            f"on host '{job['host']}' (Tag: {job['tag']})"
        )
        if command == 'manage':
            desc += f" | Operation: {args_dict.get('operation')}"
        job['description'] = desc # Add description for later use
        print(f"  - {desc}")
        
    print(f"\nTotal operations to run: {len(jobs_to_run)}")
    # Check for the '-y' flag. If not present, ask for confirmation.
    if not args_dict.get('yes', False):
        if input("\nAre you sure you want to proceed? (yes/no): ").lower().strip() != 'yes':
            print("\nOperation cancelled by user.")
            return
    else:
        print("\nNon-interactive mode (-y) detected. Proceeding automatically.")

    # 3. Execute the jobs in parallel
    print(f"\nUser confirmed. Submitting {len(jobs_to_run)} operations now...")
    command_func_map = {
        'setup': setup_environment,
        'teardown': teardown_environment,
        'manage': manage_environment,
    }
    command_func = command_func_map.get(command)

    if not command_func:
        print(f"Error: No function defined for command '{command}'.", file=sys.stderr)
        return

    with ThreadPoolExecutor(max_workers=args_dict.get('thread', 4)) as executor:
        future_to_job = {}
        for job in jobs_to_run:
            # Each job gets its own args_dict and OperationLogger
            job_args = {**args_dict, **job}
            op_logger = OperationLogger(command, job_args)
            
            future = executor.submit(command_func, job_args, op_logger)
            future_to_job[future] = job['description']

        print("\n--- Waiting for all operations to complete ---")
        for future in as_completed(future_to_job):
            desc = future_to_job[future]
            try:
                results = future.result()  # Get the list of result dictionaries
                # Check if any part of the operation returned a "failed" status
                if isinstance(results, list) and any(r.get("status") == "failed" for r in results):
                    print(f"  [FAILURE] Operation completed with errors: {desc}.")
                else:
                    print(f"  [SUCCESS] Operation completed: {desc}")
            except Exception as exc:
                print(f"  [FAILURE] Operation failed: {desc}. Error: {exc}")

    print(f"\n--- Auto-Lookup Operation Finished for {command.capitalize()} ---")