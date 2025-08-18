import logging
import sys
import argparse
from typing import Optional, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor
from utils import wait_for_tasks
from tqdm import tqdm
from tabulate import tabulate
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# Import local utilities and helpers
from config_utils import fetch_and_prepare_course_config, extract_components, get_host_by_name
from vcenter_utils import get_vcenter_instance
# Pass args_dict down to orchestrator functions
from orchestrator import vendor_setup, vendor_teardown, update_monitor_and_database
from operation_logger import OperationLogger
# Import the new function from db_utils
from db_utils import update_database, delete_from_database, get_prtg_url, mongo_client, get_test_params_by_tag
from monitor.prtg import PRTGManager
from constants import DB_NAME, ALLOCATION_COLLECTION

import labs.setup.checkpoint as checkpoint 
from labs.test.test_utils import parse_exclude_string, get_test_jobs_by_vendor, display_test_jobs, get_test_jobs_for_range, execute_single_test_worker
import labs.manage.vm_operations as vm_operations
from labs.manage.vm_relocation import relocate_pod_vms
from labs.manage.vm_migration import migrate_pod

from managers.vm_manager import VmManager
from pyVmomi import vim

logger = logging.getLogger('labbuild.commands')

# --- Constants for return status ---
COMPONENT_LIST_STATUS = "component_list_displayed"
TEMP_COURSE_CONFIG_COLLECTION = "temp_courseconfig"

COMPONENT_LIST_STATUS = "component_list_displayed"
RED = '\033[91m'
ENDC = '\033[0m'

# --- NEW HELPER FUNCTION ---
def _format_pod_ranges(numbers: List[int]) -> str:
    """Converts a list of numbers into a compact range string. e.g., [1,2,3,5] -> '1-3, 5'"""
    if not numbers:
        return ""
    
    numbers = sorted(list(set(numbers))) # Sort and remove duplicates
    ranges = []
    start_range = numbers[0]
    
    for i in range(1, len(numbers)):
        if numbers[i] != numbers[i-1] + 1:
            end_range = numbers[i-1]
            if start_range == end_range:
                ranges.append(str(start_range))
            else:
                ranges.append(f"{start_range}-{end_range}")
            start_range = numbers[i]
    
    # Add the last range
    end_range = numbers[-1]
    if start_range == end_range:
        ranges.append(str(start_range))
    else:
        ranges.append(f"{start_range}-{end_range}")
        
    return ", ".join(ranges)

def test_environment(args_dict: Dict[str, Any], operation_logger: Optional[OperationLogger] = None) -> List[Dict[str, Any]]:
    """Runs a test suite for a lab, by tag, by vendor, or by manual parameters."""
    
    # --- Mode 0: Database listing mode (No changes here) ---
    if args_dict.get('db'):
        logger.info("Database listing mode invoked for 'test' command.")
        
        vendor_filter = args_dict.get('vendor')
        start_filter = args_dict.get('start_pod')
        end_filter = args_dict.get('end_pod')

        processed_data = []
        
        try:
            with mongo_client() as client:
                if not client:
                    err_msg = "Database connection failed."
                    logger.error(err_msg)
                    print(f"Error: {err_msg}", file=sys.stderr)
                    return [{"status": "failed", "error": err_msg}]

                db = client[DB_NAME]
                collection = db[ALLOCATION_COLLECTION]
                
                query = {}
                if vendor_filter:
                    query["courses.vendor"] = {"$regex": f"^{vendor_filter}$", "$options": "i"}
                
                logger.debug(f"Querying allocations with: {query}")
                allocations = collection.find(query)
                
                for alloc in allocations:
                    tag = alloc.get("tag", "N/A")
                    for course in alloc.get("courses", []):
                        vendor = course.get("vendor", "N/A")
                        course_name = course.get("course_name", "N/A")
                        
                        all_hosts = set()
                        all_item_numbers = [] 

                        is_f5 = vendor.lower() == 'f5'
                        item_key = "class_number" if is_f5 else "pod_number"
                        
                        for detail in course.get("pod_details", []):
                            host = detail.get("host")
                            item_num_raw = detail.get(item_key)
                            
                            if not host or item_num_raw is None:
                                continue
                            
                            try:
                                item_num = int(item_num_raw)
                            except (ValueError, TypeError):
                                logger.warning(f"Skipping non-integer {item_key} '{item_num_raw}' in course '{course_name}'.")
                                continue

                            if start_filter is not None and end_filter is not None:
                                if not (start_filter <= item_num <= end_filter):
                                    continue
                            
                            all_hosts.add(host)
                            all_item_numbers.append(item_num)
                        
                        if all_item_numbers:
                            range_str = _format_pod_ranges(all_item_numbers)
                            hosts_str = ", ".join(sorted(list(all_hosts)))
                            processed_data.append([tag, vendor.upper(), course_name, hosts_str, range_str])

        except Exception as e:
            err_msg = f"An error occurred while querying the database: {e}"
            logger.error(err_msg, exc_info=True)
            print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]

        if not processed_data:
            print("\nNo allocations found matching the specified criteria.")
        else:
            processed_data.sort(key=lambda x: (x[0], x[2]))
            headers = ["Tag", "Vendor", "Course Name", "Host(s)", "Pod/Class Range"]
            print("\n" + "="*80)
            print("                       DATABASE ALLOCATION SUMMARY")
            print("="*80)
            print(tabulate(processed_data, headers=headers, tablefmt="fancy_grid"))
            print("="*80)

        return [{"status": "completed_db_list"}]

    thread_count = args_dict.get('thread', 10)

    # --- Mode 1: Vendor-wide test mode (No changes here) ---
    is_vendor_mode = (args_dict.get('vendor') and not args_dict.get('tag') and
                      args_dict.get('start_pod') is None and args_dict.get('end_pod') is None and
                      args_dict.get('class_number') is None)
    if is_vendor_mode:
        vendor = args_dict['vendor']
        logger.info(f"Vendor-wide test mode for '{vendor}' with {thread_count} threads.")
        
        exclude_set = parse_exclude_string(args_dict.get('exclude'))
        jobs = get_test_jobs_by_vendor(vendor, exclude_set)
        
        if not jobs:
            print(f"\nNo testable allocations found for vendor '{vendor}' (after exclusions).")
            return [{"status": "completed_no_tasks"}]

        display_test_jobs(jobs, vendor)

        all_results = []
        all_failures = []
        print_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_job = {}
            for job in jobs:
                job_args_dict = job.copy()
                job_args_dict["group"] = job.get("course_name")
                job_args_dict["component"] = args_dict.get("component")
                future = executor.submit(execute_single_test_worker, job_args_dict, print_lock)
                future_to_job[future] = job
            
            for future in tqdm(as_completed(future_to_job), total=len(jobs), desc="Running Tests", unit="job"):
                try:
                    results = future.result()
                    job_info = future_to_job[future]
                    tag = job_info.get('tag')
                    if tag:
                        for res in results:
                            if isinstance(res, dict): res['tag'] = tag
                    all_results.extend(results)
                    for res in results:
                        status_upper = res.get('status', '').upper()
                        if status_upper not in ['UP', 'SUCCESS', 'OPEN', 'FILTERED'] and not status_upper.startswith('SKIPPED'):
                            all_failures.append(res)
                except Exception as e:
                    job_info = future_to_job[future]
                    logger.error(f"Job {job_info} generated an exception: {e}", exc_info=True)
                    failure_report = {'pod': job_info.get('start_pod', 'N/A'), 'component': 'Execution', 
                                      'status': 'EXCEPTION', 'error': str(e)}
                    all_failures.append(failure_report)
                    all_results.append(failure_report)

        if all_failures:
            print("\n" + "="*80)
            print("                       CONSOLIDATED ERROR REPORT")
            print("="*80)
            headers = ["Pod/Class", "Component", "IP Address", "Port", "Status/Error"]
            error_table_data = []
            for fail in sorted(all_failures, key=lambda x: (x.get('pod') or x.get('class_number', 0))):
                pod_id = fail.get('pod') or fail.get('class_number', 'N/A')
                status = f"{RED}{fail.get('status') or fail.get('error', 'Unknown')}{ENDC}"
                error_table_data.append([
                    pod_id, fail.get('component', 'N/A'), fail.get('ip', 'N/A'),
                    fail.get('port', 'N/A'), status
                ])
            print(tabulate(error_table_data, headers=headers, tablefmt="fancy_grid"))
            print("="*80)
        else:
            print("\n✅ All tests completed successfully with no reported failures.")

        return all_results

    # --- Mode 2: Component listing request (No changes here) ---
    if args_dict.get('component') == '?':
        course_name = args_dict.get('group')
        if not course_name:
            err_msg = "Cannot list components: --group (course name) is required with --component '?'"
            logger.error(err_msg); print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]
        
        logger.info(f"Listing components for course: {course_name}")
        try:
            with mongo_client() as client:
                if not client: raise ConnectionError("DB connection failed.")
                db = client[DB_NAME]
                collection = db[TEMP_COURSE_CONFIG_COLLECTION]
                doc = collection.find_one({"course_name": course_name}, {"components.component_name": 1})
                if not doc or 'components' not in doc:
                    print(f"No components found for course: {course_name}")
                    return [{"status": "success", "message": "No components found"}]
                
                component_names = sorted([c.get('component_name') for c in doc.get('components', []) if c.get('component_name')])
                print(f"\nAvailable components for course '{course_name}':\n  - " + "\n  - ".join(component_names))
                print("\nUse -c with one or more comma-separated names to test specific components.")
                return [{"status": "success", "message": "Component list displayed"}]
        except Exception as e:
            err_msg = f"Failed to fetch component list: {e}"; logger.error(err_msg, exc_info=True)
            print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]

    # --- Mode 3: Tag-based test mode (No changes here) ---
    # FILE: commands.py

    # --- Mode 3: Tag-based test mode ---
    if args_dict.get('tag'):
        tag = args_dict.get('tag')
        logger.info(f"Test command invoked with tag: '{tag}'. Fetching parameters from database.")
        
        if any(k in args_dict for k in ['vendor', 'start_pod', 'host', 'group'] if k != 'tag'):
            logger.warning(f"Conflicting arguments provided with --tag. Using parameters from tag '{tag}'.")
        
        try:
            jobs = get_test_params_by_tag(tag)
            if not jobs:
                err_msg = f"Could not retrieve any valid test jobs for tag '{tag}'. The tag might not exist or its courses may be invalid."
                logger.error(err_msg); print(f"Error: {err_msg}", file=sys.stderr)
                return [{"status": "failed", "error": err_msg}]

            logger.info(f"Successfully fetched {len(jobs)} test job(s) for tag '{tag}'.")
            
            # Since a tag can have multiple vendors, we can't assume one.
            # We'll use the vendor from the first job for display purposes, or a generic one.
            display_vendor = jobs[0].get('vendor', 'multi-vendor').upper()
            display_test_jobs(jobs, display_vendor)

            all_results = []
            all_failures = []
            print_lock = threading.Lock()
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                future_to_job = {}
                for job in jobs:
                    # The job dictionary from get_test_params_by_tag already has everything needed
                    job_args_dict = job.copy()
                    job_args_dict["component"] = args_dict.get("component")
                    future = executor.submit(execute_single_test_worker, job_args_dict, print_lock)
                    future_to_job[future] = job
                
                for future in tqdm(as_completed(future_to_job), total=len(jobs), desc="Running Tests", unit="job"):
                    try:
                        results = future.result()
                        # Tag is already in the job, so it will be in the results
                        all_results.extend(results)
                        for res in results:
                            status_upper = res.get('status', '').upper()
                            if status_upper not in ['UP', 'SUCCESS', 'OPEN', 'FILTERED'] and not status_upper.startswith('SKIPPED'):
                                all_failures.append(res)
                    except Exception as e:
                        job_info = future_to_job[future]
                        logger.error(f"Job {job_info} generated an exception: {e}", exc_info=True)
                        failure_report = {'pod': job_info.get('start_pod', 'N/A'), 'component': 'Execution', 
                                          'status': 'EXCEPTION', 'error': str(e)}
                        all_failures.append(failure_report)
                        all_results.append(failure_report)

            if all_failures:
                print("\n" + "="*80)
                print("                       CONSOLIDATED ERROR REPORT")
                print("="*80)
                headers = ["Pod/Class", "Component", "IP Address", "Port", "Status/Error"]
                error_table_data = []
                for fail in sorted(all_failures, key=lambda x: (x.get('pod') or x.get('class_number', 0))):
                    pod_id = fail.get('pod') or fail.get('class_number', 'N/A')
                    status = f"{RED}{fail.get('status') or fail.get('error', 'Unknown')}{ENDC}"
                    error_table_data.append([pod_id, fail.get('component', 'N/A'), fail.get('ip', 'N/A'), fail.get('port', 'N/A'), status])
                print(tabulate(error_table_data, headers=headers, tablefmt="fancy_grid"))
                print("="*80)
            else:
                print("\n✅ All tests completed successfully with no reported failures.")

            return all_results

        except Exception as e:
            err_msg = f"An unexpected error occurred while processing tag '{tag}': {e}"
            logger.error(err_msg, exc_info=True); print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]
        
    # --- Mode 4: Manual test mode with range ---
    # --- THIS ENTIRE SECTION IS REPLACED WITH THE NEW LOGIC ---
    is_manual_range_mode = args_dict.get('start_pod') is not None and args_dict.get('end_pod') is not None
    is_manual_class_mode = args_dict.get('vendor', '').lower() == 'f5' and args_dict.get('class_number') is not None

    if is_manual_range_mode or is_manual_class_mode:
        logger.info("Manual test mode invoked with pod/class parameters.")
        vendor = args_dict.get('vendor')
        if not vendor:
            err_msg = "Missing required argument for manual test: --vendor"
            logger.error(err_msg); print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]

        start_num = args_dict.get('start_pod')
        end_num = args_dict.get('end_pod')

        if is_manual_class_mode and start_num is None:
            start_num = 0
            end_num = 9999
        elif start_num is None or end_num is None:
            err_msg = "Manual test requires --start-pod and --end-pod for non-F5 vendors."
            logger.error(err_msg); print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]

        # --- STAGE 1: SEARCH AND ENRICH ---
        logger.info("Stage 1: Searching for matching allocations in the database...")
        jobs = get_test_jobs_for_range(
            vendor=vendor,
            start_num=start_num,
            end_num=end_num,
            class_filter=args_dict.get('class_number'),
            host_filter=args_dict.get('host'),
            group_filter=args_dict.get('group')
        )
        
        # --- STAGE 2: MANUAL OVERRIDE FALLBACK ---
        if not jobs:
            logger.warning("No matching allocations found in the database. Attempting manual override.")
            
            # Check for mandatory arguments for a manual run
            required_args = ['vendor', 'group', 'host', 'start_pod', 'end_pod']
            if is_manual_class_mode:
                required_args = ['vendor', 'group', 'host', 'class_number'] # F5 test needs class_number primarily

            missing_args = [f"--{arg.replace('_', '-')}" for arg in required_args if args_dict.get(arg) is None]

            if missing_args:
                err_msg = f"Manual test requires the following arguments: {', '.join(missing_args)}"
                logger.error(err_msg)
                print(f"\nError: {err_msg}", file=sys.stderr)
                return [{"status": "failed", "error": err_msg}]

            logger.info("All required manual arguments provided. Creating a synthetic test job.")
            # Create a single "job" from the provided arguments
            manual_job = {
                "vendor": args_dict['vendor'],
                "course_name": args_dict['group'],
                "host": args_dict['host'],
                "tag": "MANUAL-TEST", # Use a placeholder tag
                "start_pod": args_dict.get('start_pod'),
                "end_pod": args_dict.get('end_pod'),
                "class_number": args_dict.get('class_number')
            }
            jobs = [manual_job]

        # From here, the execution is the same whether jobs came from DB or manual override
        display_test_jobs(jobs, args_dict['vendor'])

        all_results = []
        all_failures = []
        print_lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_job = {}
            for job in jobs:
                job_args_dict = job.copy()
                job_args_dict["group"] = job.get("course_name")
                job_args_dict["component"] = args_dict.get("component")
                future = executor.submit(execute_single_test_worker, job_args_dict, print_lock)
                future_to_job[future] = job
            
            for future in tqdm(as_completed(future_to_job), total=len(jobs), desc="Running Tests", unit="job"):
                try:
                    results = future.result()
                    job_info = future_to_job[future]
                    tag = job_info.get('tag')
                    if tag:
                        for res in results:
                            if isinstance(res, dict): res['tag'] = tag
                    all_results.extend(results)
                    for res in results:
                        status_upper = res.get('status', '').upper()
                        if status_upper not in ['UP', 'SUCCESS', 'OPEN', 'FILTERED'] and not status_upper.startswith('SKIPPED'):
                            all_failures.append(res)
                except Exception as e:
                    job_info = future_to_job[future]
                    logger.error(f"Job {job_info} generated an exception: {e}", exc_info=True)
                    failure_report = {'pod': job_info.get('start_pod', 'N/A'), 'component': 'Execution', 
                                      'status': 'EXCEPTION', 'error': str(e)}
                    all_failures.append(failure_report)
                    all_results.append(failure_report)

        if all_failures:
            print("\n" + "="*80)
            print("                       CONSOLIDATED ERROR REPORT")
            print("="*80)
            headers = ["Pod/Class", "Component", "IP Address", "Port", "Status/Error"]
            error_table_data = []
            for fail in sorted(all_failures, key=lambda x: (x.get('pod') or x.get('class_number', 0))):
                pod_id = fail.get('pod') or fail.get('class_number', 'N/A')
                status = f"{RED}{fail.get('status') or fail.get('error', 'Unknown')}{ENDC}"
                error_table_data.append([pod_id, fail.get('component', 'N/A'), fail.get('ip', 'N/A'), fail.get('port', 'N/A'), status])
            print(tabulate(error_table_data, headers=headers, tablefmt="fancy_grid"))
            print("="*80)
        else:
            print("\n✅ All tests completed successfully with no reported failures.")

        return all_results
    
    # --- Fallback Error ---
    err_msg = "Invalid arguments for 'test' command. Please provide a --tag, or --vendor for a vendor-wide test, or a pod/class range with --start-pod and --end-pod, or use --db to list allocations."
    logger.error(err_msg); print(f"Error: {err_msg}", file=sys.stderr)
    return [{"status": "failed", "error": err_msg}] 
# --- Modified setup_environment ---
# Accepts args_dict instead of argparse.Namespace
def setup_environment(args_dict: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """Handles the 'setup' command logic, taking args as a dictionary."""
    # --- Extract arguments from dict, providing defaults and type conversion ---
    course_arg = args_dict.get('course')
    vendor_arg = args_dict.get('vendor')
    host_arg = args_dict.get('host')
    tag_arg = args_dict.get('tag') # Default already handled by argparse, but good practice here too
    thread_arg = int(args_dict.get('thread', 4)) # Use default from labbuild.py if not present
    # Handle potential None before int conversion for range/class/memory
    start_pod_arg_str = args_dict.get('start_pod')
    start_pod_arg = int(start_pod_arg_str) if start_pod_arg_str is not None else None
    end_pod_arg_str = args_dict.get('end_pod')
    end_pod_arg = int(end_pod_arg_str) if end_pod_arg_str is not None else None
    class_number_arg_str = args_dict.get('class_number')
    class_number_arg = int(class_number_arg_str) if class_number_arg_str is not None else None
    component_arg = args_dict.get('component')
    datastore_arg = args_dict.get('datastore', 'vms') # Default handled by argparse, good fallback
    db_only_arg = args_dict.get('db_only', False) # Checkbox values might be True/False or absent
    monitor_only_arg = args_dict.get('monitor_only', False)
    perm_arg = args_dict.get('perm', False)
    rebuild_arg = args_dict.get('re_build', False) # 're-build' becomes 're_build'
    full_arg = args_dict.get('full', False)
    memory_arg_str = args_dict.get('memory')
    memory_arg = int(memory_arg_str) if memory_arg_str is not None else None
    prtg_server_arg = args_dict.get('prtg_server')
    # verbose_arg is handled when setting up the logger in the calling function (labbuild.py)
    # --- End Argument Extraction ---
    # --- NEW: Extract optional metadata arguments ---
    start_date_arg = args_dict.get('start_date')
    end_date_arg = args_dict.get('end_date')
    trainer_name_arg = args_dict.get('trainer_name')
    username_arg = args_dict.get('username')
    password_arg = args_dict.get('password')
    # --- END NEW EXTRACTION ---

    all_results = []
    course_config = None

    # Check required args that argparse might have missed or aren't easily enforced
    # Course and Vendor are marked required in argparse now
    # if not course_arg or not vendor_arg:
    #      err_msg = "Missing required arguments: course and/or vendor."
    #      logger.critical(err_msg)
    #      operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
    #      return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]

    # Pod range/Class number validation (already checked in labbuild.py, but keep for robustness)
    is_f5_vendor = vendor_arg.lower() == 'f5'
    # Special modes don't need range/host/class validation here (checked in labbuild.py)
    needs_range_validation = not db_only_arg and not monitor_only_arg and not perm_arg and component_arg != "?"

    if needs_range_validation:
        if is_f5_vendor and class_number_arg is None:
            err_msg = "--class_number required for F5 setup."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
            return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        elif not is_f5_vendor and (start_pod_arg is None or end_pod_arg is None):
             err_msg = "--start-pod / --end-pod required for non-F5 setup."
             logger.critical(err_msg)
             operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
             return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        if start_pod_arg is not None and end_pod_arg is not None and start_pod_arg > end_pod_arg:
             err_msg = "Error: start-pod cannot be greater than end-pod."
             logger.critical(err_msg)
             operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="invalid_pod_range", error=err_msg)
             return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        if not host_arg: # Host is generally needed unless db/monitor/perm only or listing components
            err_msg = "--host is required for this operation."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
            return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]

    # Fetch course config early (needed for component list and most paths)
    # Skip fetch if only listing courses (course_arg == '?') - this case is handled in labbuild.py now
    if course_arg != "?":
        try:
            course_config = fetch_and_prepare_course_config(course_arg)
        except ValueError as e: # Specific error for "not found"
            logger.critical(f"Configuration Error: {e}")
            operation_logger.log_pod_status(pod_id="config_fetch", status="failed", step="fetch_initial_config", error=str(e))
            return [{"identifier": "config_fetch", "status": "failed", "error_message": str(e)}]
        except Exception as e: # Catch other potential errors (DB connection, JSON parsing)
            logger.critical(f"Failed to fetch or prepare course config '{course_arg}': {e}", exc_info=True)
            operation_logger.log_pod_status(pod_id="config_fetch", status="failed", step="fetch_initial_config", error=str(e))
            return [{"identifier": "config_fetch", "status": "failed", "error_message": str(e)}]

    # --- Handle Course Listing "?" ---
    # This should be handled by labbuild.py based on args.course == "?"
    # If somehow this function is called with course_arg == "?", handle gracefully.
    if course_arg == "?":
        logger.warning("setup_environment called with course='?'. Listing should be handled by caller.")
        # list_vendor_courses(vendor_arg) # list_vendor_courses exits, don't call here.
        # Return an empty success or a specific status if needed, but labbuild.py handles this now.
        return [{"identifier": "course_list_request", "status": "skipped", "message": "Course listing handled by entry point."}]

    # --- Handle Component Listing "?" ---
    if component_arg == "?":
        if not course_config: # Should have been loaded above unless course_arg was "?"
            logger.error("Cannot list components: Config not loaded.")
            operation_logger.log_pod_status(pod_id="component_list", status="failed", step="config_not_loaded", error="Config not loaded")
            return [{"identifier": "component_list", "status": "failed", "error_message": "Config not loaded"}]

        try:
            comps = extract_components(course_config)
            # Log the components instead of printing directly, let caller handle display
            component_list_str = "\nComponents for '{}':\n  - {}".format(course_arg, "\n  - ".join(comps))
            logger.info(component_list_str) # Log the list
            operation_logger.log_pod_status(pod_id="component_list", status="success", step="list_components")
            # Return a special status that the calling function (labbuild.py) can recognize
            return [{"identifier": "component_list", "status": COMPONENT_LIST_STATUS, "components": comps}]
        except Exception as e_extract:
            logger.error(f"Error extracting components for '{course_arg}': {e_extract}", exc_info=True)
            operation_logger.log_pod_status(pod_id="component_list", status="failed", step="extract_components_error", error=str(e_extract))
            return [{"identifier": "component_list", "status": "failed", "error_message": str(e_extract)}]
    # --- End Component Listing ---

    # --- Component Selection Validation ---
    selected_components = None
    if component_arg:
        if not course_config: # Should be loaded unless error occurred
             logger.error("Cannot validate components: Config not loaded.")
             return [{"identifier": "component_validation", "status": "failed", "error_message": "Config not loaded"}]
        selected_components = [c.strip() for c in component_arg.split(",")]
        available_components = extract_components(course_config)
        invalid_components = [c for c in selected_components if c not in available_components]
        if invalid_components:
            err_msg = f"Invalid components specified: {', '.join(invalid_components)}. Available: {', '.join(available_components)}"
            logger.error(err_msg)
            operation_logger.log_pod_status(pod_id="component_validation", status="failed", step="invalid_component", error=err_msg)
            return [{"identifier": "component_validation", "status": "failed", "error_message": err_msg}]

    # --- DB Only Mode ---
    if db_only_arg:
        logger.info("DB-only mode activated: Updating database entries.")
        if start_pod_arg is None or end_pod_arg is None:
             err_msg="DB-only mode requires --start-pod and --end-pod."
             logger.error(err_msg)
             operation_logger.log_pod_status(pod_id="db_only", status="failed", step="missing_pod_range", error=err_msg)
             return [{"identifier": "db_only", "status": "failed", "error_message": err_msg}]
        if not host_arg:
             err_msg="DB-only mode requires --host."
             logger.error(err_msg); operation_logger.log_pod_status(pod_id="db_only", status="failed", error=err_msg); return [{"identifier": "db_only", "status": "failed", "error_message": err_msg}]

        # --- THIS IS THE CORRECTED LOGIC ---
        # Construct the data dictionary with all optional metadata.
        data_for_db = {
            "tag": tag_arg,
            "course_name": course_arg,
            "vendor": vendor_arg,
            "pod_details": [],
            "start_date": start_date_arg,
            "end_date": end_date_arg,
            "trainer_name": trainer_name_arg,
            "apm_username": username_arg,
            "apm_password": password_arg,
        }

        for pod_num in range(start_pod_arg, end_pod_arg + 1):
            # Create a MINIMAL pod entry. Do NOT include prtg_url or poweron.
            # This prevents the update operation from overwriting existing values with defaults.
            pod_db_entry = {"pod_number": pod_num, "host": host_arg}
            data_for_db["pod_details"].append(pod_db_entry)
        
        try:
            update_database(data_for_db)
            logger.info("DB-only setup complete: Database updated.")
            operation_logger.log_pod_status(pod_id="db_only", status="skipped", step="db_only_mode")
            results = [{"identifier": str(pd["pod_number"]), "status": "skipped", "message": "DB Only"} for pd in data_for_db["pod_details"]]
            return results
        except Exception as db_err:
            logger.error(f"DB-only mode failed during database update: {db_err}", exc_info=True)
            operation_logger.log_pod_status(pod_id="db_only", status="failed", step="db_update_error", error=str(db_err))
            return [{"identifier": "db_only", "status": "failed", "error_message": str(db_err)}]

    # --- Prepare vCenter Connection (needed for perm, monitor, and normal setup) ---
    host_details = None
    service_instance = None
    needs_vcenter = perm_arg or monitor_only_arg or (not db_only_arg)

    if needs_vcenter:
        if not host_arg: # Should have been caught by validation already, but double-check
            err_msg = "--host is required for this operation."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="vcenter_prep", status="failed", error=err_msg); return [{"identifier": "vcenter_prep", "status": "failed", "error_message": err_msg}]
        host_details = get_host_by_name(host_arg)
        if not host_details:
            err_msg = f"Host details not found in configuration for '{host_arg}'."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="host_fetch", status="failed", error=err_msg)
            return [{"identifier": "host_fetch", "status": "failed", "error_message": err_msg}]
        service_instance = get_vcenter_instance(host_details)
        if not service_instance:
            err_msg = f"vCenter connection failed for host '{host_arg}' ({host_details.get('vcenter')})."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="vcenter_connect", status="failed", error=err_msg)
            return [{"identifier": "vcenter_connect", "status": "failed", "error_message": err_msg}]

    # --- Perm Only Mode (CheckPoint specific) ---
    if perm_arg:
        if vendor_arg.lower() == 'cp':
            logger.info("Perm-only mode: Running CheckPoint permissions setup.")
            # Check required range
            if start_pod_arg is None or end_pod_arg is None:
                err_msg="Perm-only mode requires --start-pod and --end-pod."
                logger.error(err_msg); operation_logger.log_pod_status(pod_id="perm_only", status="failed", error=err_msg); return [{"identifier": "perm_only", "status": "failed", "error_message": err_msg}]

            perm_success_count, perm_fail_count = 0, 0
            for pod in range(start_pod_arg, end_pod_arg + 1):
                pod_id_str = f"perm-pod-{pod}"
                perm_status = "success"; perm_step, perm_error = None, None
                try:
                    # Fetch config per pod for permissions
                    p_config = fetch_and_prepare_course_config(course_arg, pod=pod)
                    p_config.update({"host_fqdn": host_details["fqdn"], "pod_number": pod}) # Add necessary context
                    logger.info(f"Applying permissions for pod {pod}.")
                    # Ensure the perm_only_cp_pod function exists and works
                    checkpoint.perm_only_cp_pod(service_instance, p_config)
                    perm_success_count += 1
                except Exception as e:
                    perm_status, perm_step, perm_error = "failed", "perm_only_exception", str(e)
                    perm_fail_count += 1
                    logger.error(f"Error during perm-only mode for pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=str(pod), status=perm_status, step=perm_step, error=perm_error)
                all_results.append({"identifier": pod_id_str, "status": perm_status, "error_message": perm_error})
            logger.info(f"Perm-only mode completed. Success: {perm_success_count}, Failed: {perm_fail_count}")
        else:
            err_msg = "Permission-only mode (--perm) is currently only supported for CheckPoint (cp)."
            logger.error(err_msg)
            operation_logger.log_pod_status(pod_id="perm_only", status="failed", error=err_msg)
            all_results.append({"identifier": "perm_only", "status": "failed", "error_message": err_msg})
        return all_results # Return results from perm-only mode

    # --- Monitor Only Mode ---
    if monitor_only_arg:
        logger.info("Monitor-only mode: Creating/updating monitoring entries.")
        data_acc = {"tag": tag_arg, "course_name": course_arg, "vendor": vendor_arg, "pod_details": []}
        mon_success_count, mon_fail_count = 0, 0
        # Course config should be loaded already, get vendor shortcode
        vs_code = course_config.get("vendor_shortcode")
        if not vs_code:
            err_msg = "Vendor shortcode missing in course config, cannot proceed with monitor-only."
            logger.error(err_msg); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error=err_msg); return [{"identifier": "monitor_only", "status": "failed", "error_message": err_msg}]


        if vs_code == "f5":
            # F5 Monitor-Only: Class Number is mandatory
            if class_number_arg is None:
                err_msg="F5 monitor-only mode requires --class_number."
                logger.error(err_msg); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error=err_msg); return [{"identifier": "monitor_only", "status": "failed", "error_message": err_msg}]

            # Handle F5 class monitor entry (always created/updated)
            class_id_str = f"class-{class_number_arg}"
            status, step, error = "success", None, None
            try:
                # Fetch config specific to the class
                cfg = fetch_and_prepare_course_config(course_arg, f5_class=class_number_arg)
                cfg.update({
                    "host_fqdn": host_details["fqdn"],
                    "class_number": class_number_arg,
                    "vendor_shortcode": vs_code,
                    "prtg_server": prtg_server_arg # Pass optional server preference
                    # Other necessary fields for add_monitor?
                })
                # Pass args_dict to update_monitor_and_database via orchestrator
                update_monitor_and_database(cfg, args_dict, data_acc, {"class_number": class_number_arg})
                mon_success_count += 1
            except Exception as e:
                status, step, error = "failed", "monitor_only_class_exception", str(e)
                mon_fail_count += 1
                logger.error(f"Error updating monitor/DB for F5 class {class_number_arg}: {e}", exc_info=True)
            operation_logger.log_pod_status(pod_id=class_id_str, status=status, step=step, error=error, class_id=class_number_arg)
            all_results.append({"identifier": class_id_str, "status": status, "error_message": error})

            # Handle F5 pod monitors *if* range is provided
            if start_pod_arg is not None and end_pod_arg is not None:
                for pod in range(start_pod_arg, end_pod_arg + 1):
                    pod_id_str = str(pod)
                    status, step, error = "success", None, None
                    try:
                        # Fetch config specific to the pod within the class
                        cfg = fetch_and_prepare_course_config(course_arg, pod=pod, f5_class=class_number_arg)
                        cfg.update({
                            "host_fqdn": host_details["fqdn"],
                            "class_number": class_number_arg,
                            "pod_number": pod,
                            "vendor_shortcode": vs_code,
                            "prtg_server": prtg_server_arg # Pass server preference
                        })
                        # Pass args_dict
                        update_monitor_and_database(cfg, args_dict, data_acc, {"class_number": class_number_arg})
                        mon_success_count += 1
                    except Exception as e:
                        status, step, error = "failed", "monitor_only_pod_exception", str(e)
                        mon_fail_count += 1
                        logger.error(f"Error updating monitor/DB for F5 pod {pod} (Class {class_number_arg}): {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error, class_id=class_number_arg)
                    all_results.append({"identifier": pod_id_str, "status": status, "error_message": error})
            else:
                 logger.info("No pod range provided for F5 monitor-only mode, only class monitor processed.")

        else: # Non-F5 Monitor-Only: Range is mandatory
            if start_pod_arg is None or end_pod_arg is None:
                 err_msg="Monitor-only mode requires --start-pod and --end-pod for non-F5 vendors."
                 logger.error(err_msg); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error=err_msg); return [{"identifier": "monitor_only", "status": "failed", "error_message": err_msg}]

            for pod in range(start_pod_arg, end_pod_arg + 1):
                pod_id_str = str(pod)
                status, step, error = "success", None, None
                try:
                    # Fetch config specific to the pod
                    cfg = fetch_and_prepare_course_config(course_arg, pod=pod)
                    cfg.update({
                        "host_fqdn": host_details["fqdn"],
                        "pod_number": pod,
                        "vendor_shortcode": vs_code,
                         "prtg_server": prtg_server_arg # Pass server preference
                    })
                    # Pass args_dict
                    update_monitor_and_database(cfg, args_dict, data_acc)
                    mon_success_count += 1
                except Exception as e:
                    status, step, error = "failed", "monitor_only_exception", str(e)
                    mon_fail_count += 1
                    logger.error(f"Error updating monitor/DB for pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error)
                all_results.append({"identifier": pod_id_str, "status": status, "error_message": error})

        logger.info(f"Monitor-only mode completed. Success: {mon_success_count}, Failed: {mon_fail_count}")
        return all_results # Return results from monitor-only mode

    # --- Normal Build Process ---
    # This section is reached only if not db_only, perm_only, or monitor_only
    log_target = f"pods {start_pod_arg}-{end_pod_arg}" if not is_f5_vendor else f"class {class_number_arg}" + (f" pods {start_pod_arg}-{end_pod_arg}" if start_pod_arg is not None else "")
    logger.info(f"Starting normal setup for course '{course_arg}' on host '{host_arg}' targeting {log_target}")

    # Pass the args_dict down to the vendor-specific setup orchestrator
    # The orchestrator will handle F5 vs non-F5 logic internally
    all_results = vendor_setup(
        service_instance,
        host_details,
        args_dict, # Pass the dictionary
        course_config,
        selected_components,
        operation_logger
    )
    logger.info(f"Setup process finished for course '{course_arg}'.")
    return all_results

# --- Modified teardown_environment ---
# Accepts args_dict instead of argparse.Namespace
def teardown_environment(args_dict: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """Handles the 'teardown' command logic using args_dict."""
    # --- Extract arguments ---
    course_arg = args_dict.get('course')
    vendor_arg = args_dict.get('vendor')
    host_arg = args_dict.get('host')
    tag_arg = args_dict.get('tag')
    start_pod_arg_str = args_dict.get('start_pod')
    start_pod_arg = int(start_pod_arg_str) if start_pod_arg_str is not None else None
    end_pod_arg_str = args_dict.get('end_pod')
    end_pod_arg = int(end_pod_arg_str) if end_pod_arg_str is not None else None
    class_number_arg_str = args_dict.get('class_number')
    class_number_arg = int(class_number_arg_str) if class_number_arg_str is not None else None
    db_only_arg = args_dict.get('db_only', False)
    monitor_only_arg = args_dict.get('monitor_only', False)
    # --- End Argument Extraction ---

    all_results = []
    course_config = None

    # Basic validation (Course/Vendor required by argparse)

    # Range/Class validation similar to setup, adapted for teardown
    is_f5_vendor = vendor_arg.lower() == 'f5'
    needs_range_validation = not db_only_arg and not monitor_only_arg

    if needs_range_validation:
        if is_f5_vendor and class_number_arg is None:
            err_msg = "--class_number required for F5 teardown."
            logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg)
            return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        # Non-F5 teardown needs pod range for current implementation
        elif not is_f5_vendor and (start_pod_arg is None or end_pod_arg is None):
             err_msg = "--start-pod / --end-pod required for non-F5 teardown."
             logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg)
             return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        # Pod range sanity check
        if start_pod_arg is not None and end_pod_arg is not None and start_pod_arg > end_pod_arg:
             err_msg = "Error: start-pod cannot be greater than end-pod."
             logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg)
             return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        # Host required for non-db/monitor only teardown
        if not host_arg:
             err_msg = "--host is required for teardown."
             logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg)
             return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]


    # Fetch course config (needed for vendor shortcode, course name consistency)
    # Skip fetch if course_arg == "?" (should not happen for teardown)
    if course_arg == "?":
         logger.error("Cannot teardown: Course '?' is not a valid target.")
         return [{"identifier": "arg_validation", "status": "failed", "error_message": "Course '?' invalid for teardown"}]

    try:
        course_config = fetch_and_prepare_course_config(course_arg)
    except ValueError as e:
        logger.critical(f"Configuration Error: {e}")
        operation_logger.log_pod_status(pod_id="config_fetch", status="failed", step="fetch_initial_config", error=str(e))
        return [{"identifier": "config_fetch", "status": "failed", "error_message": str(e)}]
    except Exception as e:
        logger.critical(f"Failed to fetch course config for teardown '{course_arg}': {e}", exc_info=True)
        operation_logger.log_pod_status(pod_id="config_fetch", status="failed", error=str(e))
        return [{"identifier": "config_fetch", "status": "failed", "error_message": str(e)}]

    vendor_shortcode = course_config.get("vendor_shortcode")
    course_name_from_config = course_config.get("course_name", course_arg) # Use config name for consistency

    # --- DB Only Teardown ---
    if db_only_arg:
        logger.info("DB-only teardown mode activated: Deleting database entries.")
        deleted_items = []
        delete_success = True
        try:
            if vendor_shortcode == "f5":
                if class_number_arg is None:
                    err_msg = "F5 DB-only teardown requires --class_number."; logger.error(err_msg); operation_logger.log_pod_status(pod_id="db_only", status="failed", error=err_msg); return [{"identifier": "db_only", "status": "failed", "error_message": err_msg}]
                # Delete the whole class entry
                delete_from_database(tag_arg, course_name=course_name_from_config, class_number=class_number_arg)
                deleted_items.append({"identifier": f"class-{class_number_arg}", "status": "skipped"})
                logger.info(f"DB entry deleted for F5 class {class_number_arg}, tag '{tag_arg}', course '{course_name_from_config}'.")
            else: # Non-F5
                if start_pod_arg is None or end_pod_arg is None:
                     err_msg = "DB-only teardown requires --start-pod and --end-pod for non-F5."; logger.error(err_msg); operation_logger.log_pod_status(pod_id="db_only", status="failed", error=err_msg); return [{"identifier": "db_only", "status": "failed", "error_message": err_msg}]
                for pod in range(start_pod_arg, end_pod_arg + 1):
                    delete_from_database(tag_arg, course_name=course_name_from_config, pod_number=pod)
                    deleted_items.append({"identifier": str(pod), "status": "skipped"})
                    logger.info(f"DB entry deleted for pod {pod}, tag '{tag_arg}', course '{course_name_from_config}'.")
        except Exception as db_err:
             logger.error(f"DB-only teardown failed during database deletion: {db_err}", exc_info=True)
             operation_logger.log_pod_status(pod_id="db_only", status="failed", step="db_delete_error", error=str(db_err))
             delete_success = False
             # Return failure status for all targeted items
             deleted_items = [{"identifier": f"class-{class_number_arg}" if is_f5_vendor else str(p), "status": "failed", "error_message": str(db_err)} for p in range(start_pod_arg or class_number_arg or 0, (end_pod_arg or class_number_arg or 0) + 1)]

        if delete_success:
             logger.info("DB-only teardown complete.")
             operation_logger.log_pod_status(pod_id="db_only", status="skipped", step="db_only_mode")
        return deleted_items

    # --- Monitor Only Teardown ---
    if monitor_only_arg:
        logger.info("Monitor-only teardown mode activated: Deleting monitors and DB entries.")
        # This requires fetching PRTG URLs from DB, then deleting monitor, then deleting DB entry.
        mon_deleted_items = []
        mon_delete_success = True
        try:
             with mongo_client() as client: # Need client for PRTGManager.delete_monitor
                if not client:
                    raise ConnectionError("Monitor-only teardown: DB connection failed.")

                if vendor_shortcode == "f5":
                    if class_number_arg is None: raise ValueError("F5 monitor-only teardown requires --class_number.")
                    # Delete class monitor
                    prtg_url_class = get_prtg_url(tag_arg, course_name_from_config, class_number=class_number_arg)
                    if prtg_url_class:
                         if PRTGManager.delete_monitor(prtg_url_class, client): logger.info(f"Deleted monitor for F5 class {class_number_arg}.")
                         else: logger.warning(f"Failed to delete monitor for F5 class {class_number_arg} (URL: {prtg_url_class}).")
                    # Delete F5 pod monitors if range provided
                    if start_pod_arg is not None and end_pod_arg is not None:
                        for pod in range(start_pod_arg, end_pod_arg + 1):
                             prtg_url_pod = get_prtg_url(tag_arg, course_name_from_config, pod_number=pod, class_number=class_number_arg)
                             if prtg_url_pod:
                                 if PRTGManager.delete_monitor(prtg_url_pod, client): logger.info(f"Deleted monitor for F5 pod {pod}.")
                                 else: logger.warning(f"Failed to delete monitor for F5 pod {pod} (URL: {prtg_url_pod}).")
                    # Finally, delete the whole class DB entry
                    delete_from_database(tag_arg, course_name=course_name_from_config, class_number=class_number_arg)
                    mon_deleted_items.append({"identifier": f"class-{class_number_arg}", "status": "skipped"})

                else: # Non-F5
                    if start_pod_arg is None or end_pod_arg is None: raise ValueError("Monitor-only teardown requires --start-pod/--end-pod for non-F5.")
                    for pod in range(start_pod_arg, end_pod_arg + 1):
                         prtg_url_pod = get_prtg_url(tag_arg, course_name_from_config, pod_number=pod)
                         if prtg_url_pod:
                             if PRTGManager.delete_monitor(prtg_url_pod, client): logger.info(f"Deleted monitor for pod {pod}.")
                             else: logger.warning(f"Failed to delete monitor for pod {pod} (URL: {prtg_url_pod}).")
                         # Delete DB entry for the pod
                         delete_from_database(tag_arg, course_name=course_name_from_config, pod_number=pod)
                         mon_deleted_items.append({"identifier": str(pod), "status": "skipped"})

        except Exception as mon_err:
             logger.error(f"Monitor-only teardown failed: {mon_err}", exc_info=True)
             operation_logger.log_pod_status(pod_id="monitor_only", status="failed", step="monitor_delete_error", error=str(mon_err))
             mon_delete_success = False
             mon_deleted_items = [{"identifier": f"class-{class_number_arg}" if is_f5_vendor else str(p), "status": "failed", "error_message": str(mon_err)} for p in range(start_pod_arg or class_number_arg or 0, (end_pod_arg or class_number_arg or 0) + 1)]

        if mon_delete_success:
            logger.info("Monitor-only teardown complete.")
            operation_logger.log_pod_status(pod_id="monitor_only", status="skipped", step="monitor_only_mode")
        return mon_deleted_items


    # --- Normal Teardown (Requires vCenter) ---
    host_details = get_host_by_name(host_arg)
    if not host_details:
        err_msg = f"Host details not found for '{host_arg}'."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="host_fetch", status="failed", error=err_msg); return [{"identifier": "host_fetch", "status": "failed", "error_message": err_msg}]
    service_instance = get_vcenter_instance(host_details)
    if not service_instance:
        err_msg = f"vCenter connection failed for host '{host_arg}' ({host_details.get('vcenter')})."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="vcenter_connect", status="failed", error=err_msg); return [{"identifier": "vcenter_connect", "status": "failed", "error_message": err_msg}]

    log_target = f"pods {start_pod_arg}-{end_pod_arg}" if not is_f5_vendor else f"class {class_number_arg}"
    logger.info(f"Starting normal teardown for course '{course_arg}' on host '{host_arg}' targeting {log_target}")

    # Pass the args_dict down to the vendor-specific teardown orchestrator
    all_results = vendor_teardown(
        service_instance,
        host_details,
        args_dict, # Pass the dictionary
        course_config,
        operation_logger
    )
    logger.info(f"Teardown process finished for course '{course_arg}'.")
    return all_results


# --- Modified manage_environment ---
# Accepts args_dict instead of argparse.Namespace
def manage_environment(args_dict: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """Handles the 'manage' command logic using args_dict."""
    # --- Extract arguments ---
    course_arg = args_dict.get('course')
    vendor_arg = args_dict.get('vendor')
    host_arg = args_dict.get('host')
    start_pod_arg_str = args_dict.get('start_pod')
    start_pod_arg = int(start_pod_arg_str) if start_pod_arg_str is not None else None
    end_pod_arg_str = args_dict.get('end_pod')
    end_pod_arg = int(end_pod_arg_str) if end_pod_arg_str is not None else None
    class_number_arg_str = args_dict.get('class_number')
    class_number_arg = int(class_number_arg_str) if class_number_arg_str is not None else None
    component_arg = args_dict.get('component')
    operation_arg = args_dict.get('operation') # Required arg from argparse
    thread_arg = int(args_dict.get('thread', 4)) # Use default from labbuild.py
    # --- End Argument Extraction ---

    all_results = []
    host_details, service_instance, course_config = None, None, None

    # Basic validation (Most required args checked by argparse)
    if operation_arg not in ['start', 'stop', 'reboot']:
         # This check is technically redundant if argparse choices are set correctly, but safe.
         err_msg = f"Invalid operation specified: {operation_arg}. Must be 'start', 'stop', or 'reboot'."
         logger.critical(err_msg)
         operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg)
         return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]

    # Range/Class validation
    is_f5_vendor = vendor_arg.lower() == 'f5'
    is_listing_components = component_arg == "?"

    if not is_listing_components: # Don't validate range/host if just listing components
        if is_f5_vendor and class_number_arg is None:
            err_msg = "--class_number required for F5 manage operations."
            logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg); return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        elif not is_f5_vendor and (start_pod_arg is None or end_pod_arg is None):
            err_msg = "--start-pod / --end-pod required for non-F5 manage operations."
            logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg); return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        if start_pod_arg is not None and end_pod_arg is not None and start_pod_arg > end_pod_arg:
            err_msg = "Error: start-pod cannot be greater than end-pod."
            logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg); return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        if not host_arg:
            err_msg = "--host is required for manage operations."
            logger.critical(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg); return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]


    # --- Prerequisites: Connect to vCenter, Fetch Config ---
    try:
         host_details = get_host_by_name(host_arg)
         if not host_details: raise ValueError(f"Host details not found for '{host_arg}'.")
         service_instance = get_vcenter_instance(host_details)
         if not service_instance: raise ConnectionError(f"vCenter connection failed for host '{host_arg}'.")
         # Fetch config only if not listing components or course is not '?'
         if component_arg != "?" and course_arg != "?":
             course_config = fetch_and_prepare_course_config(course_arg)
         elif course_arg == "?":
             logger.warning("Manage command called with course='?'. Component listing expected.")
             # Return an empty success, as the component listing logic below will handle it
             return [{"identifier": "course_list_request", "status": "skipped", "message": "Course '?' invalid for manage"}]

    except ValueError as e: # Handle config not found or host not found
         err_msg = f"Prerequisite Error for manage: {e}"; logger.critical(err_msg)
         operation_logger.log_pod_status(pod_id="prereq_check", status="failed", error=err_msg)
         return [{"identifier": "prereq_check", "status": "failed", "error_message": err_msg}]
    except ConnectionError as e: # Handle vCenter connection failure
         err_msg = f"Prerequisite Error for manage: {e}"; logger.critical(err_msg)
         operation_logger.log_pod_status(pod_id="prereq_check", status="failed", error=err_msg)
         return [{"identifier": "prereq_check", "status": "failed", "error_message": err_msg}]
    except Exception as e: # Catch-all for other prereq errors
        err_msg = f"Unexpected prerequisite error for manage: {e}"; logger.critical(err_msg, exc_info=True)
        operation_logger.log_pod_status(pod_id="prereq_check", status="failed", error=err_msg)
        return [{"identifier": "prereq_check", "status": "failed", "error_message": err_msg}]


    # --- Handle Component Listing "?" ---
    if component_arg == "?":
        if not course_config: # Should have been loaded unless course was '?'
            logger.error("Cannot list components: Config not loaded."); operation_logger.log_pod_status(pod_id="component_list", status="failed", error="Config not loaded"); return [{"identifier": "component_list", "status": "failed", "error_message": "Config not loaded"}]
        try:
            comps = extract_components(course_config)
            component_list_str = "\nComponents for '{}':\n  - {}".format(course_arg, "\n  - ".join(comps))
            logger.info(component_list_str) # Log
            operation_logger.log_pod_status(pod_id="component_list", status="success", step="list_components")
            # Return special status for labbuild.py to handle
            return [{"identifier": "component_list", "status": COMPONENT_LIST_STATUS, "components": comps}]
        except Exception as e_extract:
             err_msg = f"Error extracting components for '{course_arg}': {e_extract}"; logger.error(err_msg, exc_info=True)
             operation_logger.log_pod_status(pod_id="component_list", status="failed", error=err_msg); return [{"identifier": "component_list", "status": "failed", "error_message": err_msg}]

    # --- Component Selection Validation ---
    selected_components = None
    if component_arg:
        if not course_config: return [{"identifier": "component_validation", "status": "failed", "error_message": "Config not loaded"}] # Should be loaded
        selected_components = [c.strip() for c in component_arg.split(",")]
        available_components = extract_components(course_config)
        invalid_components = [c for c in selected_components if c not in available_components]
        if invalid_components:
            err_msg = f"Invalid components: {', '.join(invalid_components)}. Available: {', '.join(available_components)}"; logger.error(err_msg)
            operation_logger.log_pod_status(pod_id="component_validation", status="failed", error=err_msg)
            return [{"identifier": "component_validation", "status": "failed", "error_message": err_msg}]

    # --- Execute Manage Operation ---
    futures = []
    logger.info(f"Dispatching VM operation '{operation_arg}' for target range. RunID: {operation_logger.run_id}")

    # Determine iteration range based on vendor
    iteration_target = []
    if is_f5_vendor:
        # F5 manage operations usually target pods within a class resource pool.
        # Class-level manage isn't typically done directly (manage individual VMs).
        # Require pod range for F5 manage as well for clarity.
        if start_pod_arg is None or end_pod_arg is None:
             err_msg = "--start-pod / --end-pod required for F5 manage operations (targets pods within class)."
             logger.error(err_msg); operation_logger.log_pod_status(pod_id="arg_validation", status="failed", error=err_msg); return [{"identifier": "arg_validation", "status": "failed", "error_message": err_msg}]
        iteration_target = range(start_pod_arg, end_pod_arg + 1)
    else: # Non-F5
        # Range already validated as required
        iteration_target = range(start_pod_arg, end_pod_arg + 1)


    with ThreadPoolExecutor(max_workers=thread_arg) as executor:
        for pod_num in iteration_target: # Iterate over determined pod range
            pod_id_str = str(pod_num)
            class_num_for_log = class_number_arg if is_f5_vendor else None
            try:
                # Fetch config per pod - needed to resolve component names to VM names
                pod_config_manage = fetch_and_prepare_course_config(course_arg, pod=pod_num, f5_class=class_num_for_log)
                # Add necessary context for vm_operations (like vendor code)
                pod_config_manage.update({
                    "host_fqdn": host_details["fqdn"], # Potentially useful context
                    "pod_number": pod_num,
                    "vendor_shortcode": vendor_arg # Crucial for determining RP name etc.
                })
                if class_num_for_log is not None:
                    pod_config_manage["class_number"] = class_num_for_log

                # Submit task to vm_operations
                # vm_operations.perform_vm_operations needs adaptation for args_dict
                # Assume it's adapted to take pod_config dictionary now
                future = executor.submit(vm_operations.perform_vm_operations,
                                         service_instance,
                                         pod_config_manage, # Pass the config dictionary
                                         operation_arg,
                                         selected_components)
                # Attach identifiers for result processing
                future.pod_number = pod_num
                future.class_number = class_num_for_log
                futures.append(future)

            except ValueError as e: # Handle config fetch/prepare errors per pod
                logger.error(f"Skipping manage task for pod {pod_num}: Config error: {e}")
                operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e), class_id=class_num_for_log)
                all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
            except Exception as e: # Handle unexpected errors during task submission
                logger.error(f"Error submitting manage task for pod {pod_num}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e), class_id=class_num_for_log)
                all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})

    # Wait for results and process (wait_for_tasks should be fine)
    task_results = wait_for_tasks(futures, description=f"VM management ({operation_arg})")

    # Log results using OperationLogger
    for res_data in task_results:
        operation_logger.log_pod_status(
            pod_id=res_data["identifier"],
            status=res_data["status"],
            step=res_data["failed_step"],
            error=res_data["error_message"],
            class_id=res_data["class_identifier"]
        )

    all_results.extend(task_results) # Combine submission errors with task results
    logger.info(f"VM management ({operation_arg}) tasks submitted/completed.")
    return all_results

def move_environment(args_dict: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """
    Handles the 'move' command logic to relocate VMs to their correct containers.
    """
    # --- Extract Arguments ---
    vendor_arg = args_dict.get('vendor')
    course_arg = args_dict.get('course')
    host_arg = args_dict.get('host')
    start_pod_arg = args_dict.get('start_pod')
    end_pod_arg = args_dict.get('end_pod')
    thread_arg = args_dict.get('thread', 4)

    logger.info(f"Dispatching VM 'move' operation for pods {start_pod_arg}-{end_pod_arg}. RunID: {operation_logger.run_id}")

    # --- Prerequisites: Connect to vCenter ---
    try:
        host_details = get_host_by_name(host_arg)
        if not host_details: raise ValueError(f"Host details not found for '{host_arg}'.")
        service_instance = get_vcenter_instance(host_details)
        if not service_instance: raise ConnectionError(f"vCenter connection failed for host '{host_arg}'.")
    except (ValueError, ConnectionError) as e:
        err_msg = f"Prerequisite Error for move: {e}"
        logger.critical(err_msg)
        operation_logger.log_pod_status(pod_id="prereq_check", status="failed", error=err_msg)
        return [{"identifier": "prereq_check", "status": "failed", "error_message": err_msg}]

    # --- Execute Move Operation in Parallel ---
    futures = []
    all_results = []
    with ThreadPoolExecutor(max_workers=thread_arg) as executor:
        for pod_num in range(start_pod_arg, end_pod_arg + 1):
            pod_id_str = str(pod_num)
            try:
                # Fetch config for each pod to know its components and naming conventions
                pod_config = fetch_and_prepare_course_config(course_arg, pod=pod_num)
                pod_config.update({
                    "host_fqdn": host_details["fqdn"],
                    "pod_number": pod_num,
                    "vendor_shortcode": vendor_arg
                })

                future = executor.submit(relocate_pod_vms, service_instance, pod_config)
                future.pod_number = pod_num
                future.class_number = None # 'move' is pod-based for now
                futures.append(future)

            except Exception as e:
                error_msg = f"Error submitting move task for pod {pod_num}: {e}"
                logger.error(error_msg, exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e))
                all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})

    # --- Wait for and Process Results ---
    task_results = wait_for_tasks(futures, description=f"VM relocation for pods {start_pod_arg}-{end_pod_arg}")
    for res_data in task_results:
        operation_logger.log_pod_status(
            pod_id=res_data["identifier"],
            status=res_data["status"],
            step=res_data["failed_step"],
            error=res_data["error_message"],
        )
    all_results.extend(task_results)
    logger.info(f"VM relocation tasks completed for pods {start_pod_arg}-{end_pod_arg}.")
    return all_results


def migrate_environment(args_dict: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """
    Handles the 'migrate' command logic to move pod VMs from a source to a destination host,
    supporting both intra- and inter-vCenter migrations.
    """
    # --- 1. Extract and Validate Arguments ---
    vendor_arg = args_dict.get('vendor')
    course_arg = args_dict.get('course')
    source_host_arg = args_dict.get('host')
    dest_host_arg = args_dict.get('destination_host')
    start_pod_arg = args_dict.get('start_pod')
    end_pod_arg = args_dict.get('end_pod')
    # Use a lower default thread count for intensive migration tasks
    thread_arg = args_dict.get('thread', 2)

    logger.info(
        f"Dispatching VM 'migrate' from '{source_host_arg}' to '{dest_host_arg}' "
        f"for pods {start_pod_arg}-{end_pod_arg}. RunID: {operation_logger.run_id}"
    )

    # --- 2. Prerequisites: Connect to vCenters and get host details ---
    try:
        # Get details for the source host from the database
        source_host_details = get_host_by_name(source_host_arg)
        if not source_host_details:
            raise ValueError(f"Source host details not found for '{source_host_arg}'.")
        
        # Get details for the destination host from the database
        dest_host_details = get_host_by_name(dest_host_arg)
        if not dest_host_details:
            raise ValueError(f"Destination host details not found for '{dest_host_arg}'.")

        # Connect to the source vCenter
        logger.info(f"Connecting to source vCenter for host '{source_host_arg}'...")
        source_service_instance = get_vcenter_instance(source_host_details)
        if not source_service_instance:
            raise ConnectionError(f"vCenter connection failed for source host '{source_host_arg}'.")

        # Connect to the destination vCenter
        logger.info(f"Connecting to destination vCenter for host '{dest_host_arg}'...")
        dest_service_instance = get_vcenter_instance(dest_host_details)
        if not dest_service_instance:
            raise ConnectionError(f"vCenter connection failed for destination host '{dest_host_arg}'.")

        # Get the internal pyvmomi object for the destination host from its vCenter connection
        # This is required by the migration task
        dest_vmm = VmManager(dest_service_instance)
        dest_host_obj = dest_vmm.get_obj([vim.HostSystem], dest_host_details['fqdn'])
        if not dest_host_obj:
            raise ValueError(f"Could not find destination host '{dest_host_details['fqdn']}' in its vCenter.")
        
        # Add the pyvmomi object to the details dict to pass to the worker threads
        dest_host_details['pyvmomi_obj'] = dest_host_obj

    except (ValueError, ConnectionError) as e:
        err_msg = f"Prerequisite Error for migrate: {e}"
        logger.critical(err_msg)
        operation_logger.log_pod_status(pod_id="prereq_check", status="failed", error=err_msg)
        return [{"identifier": "prereq_check", "status": "failed", "error_message": err_msg}]

    # --- 3. Execute Migrate Operation in Parallel ---
    futures = []
    all_results = []
    with ThreadPoolExecutor(max_workers=thread_arg) as executor:
        for pod_num in range(start_pod_arg, end_pod_arg + 1):
            pod_id_str = str(pod_num)
            try:
                # Fetch the specific configuration for the pod being migrated
                source_pod_config = fetch_and_prepare_course_config(course_arg, pod=pod_num)
                source_pod_config.update({
                    "host_fqdn": source_host_details["fqdn"],
                    "pod_number": pod_num,
                    "vendor_shortcode": vendor_arg
                })
                
                # Submit the migration task to the thread pool
                # The `migrate_pod` dispatcher will handle both inter- and intra-vCenter logic
                future = executor.submit(
                    migrate_pod,
                    source_service_instance,
                    dest_service_instance,
                    source_pod_config,
                    dest_host_details
                )
                future.pod_number = pod_num
                futures.append(future)
                
            except Exception as e:
                # Handle errors that occur before the task is even submitted
                error_msg = f"Error submitting migrate task for pod {pod_num}: {e}"
                logger.error(error_msg, exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e))
                all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})

    # --- 4. Wait for and Process Results ---
    task_results = wait_for_tasks(futures, description=f"VM migration for pods {start_pod_arg}-{end_pod_arg}")
    all_results.extend(task_results)
    
    # Log the final status of each pod's migration to the database
    for res_data in task_results:
        operation_logger.log_pod_status(
            pod_id=res_data["identifier"],
            status=res_data["status"],
            step=res_data["failed_step"],
            error=res_data["error_message"],
        )
        
    logger.info(f"VM migration tasks for pods {start_pod_arg}-{end_pod_arg} have been processed.")
    return all_results