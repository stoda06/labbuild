import re
import sys
import logging
from collections import defaultdict
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo.errors import PyMongoError
from constants import DB_NAME, ALLOCATION_COLLECTION
from db_utils import mongo_client
from operation_logger import OperationLogger
from commands import setup_environment, teardown_environment, manage_environment

logger = logging.getLogger('labbuild.batch')

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


def perform_vendor_level_operation(
    vendor: str, 
    operation: str, 
    verbose: bool, 
    start_pod_filter: Optional[int] = None, 
    end_pod_filter: Optional[int] = None
):
    """
    Finds all valid allocations for a vendor, asks for confirmation,
    and then performs a batch operation, optionally filtered by a pod/class range.
    """
    print(f"--- Starting Vendor-Level Operation ---")
    print(f"Vendor: {vendor.upper()}")
    print(f"Operation: {operation.capitalize()}")
    if start_pod_filter is not None and end_pod_filter is not None:
        print(f"Filtering for Pod/Class Range: {start_pod_filter}-{end_pod_filter}")
    print("---------------------------------------")
    
    try:
        with mongo_client() as client:
            if not client:
                print("Error: Could not connect to the database.", file=sys.stderr)
                return

            alloc_collection = client[DB_NAME][ALLOCATION_COLLECTION]
            query = {
                "courses.vendor": {"$regex": f"^{re.escape(vendor)}$", "$options": "i"},
                "tag": {"$ne": "untagged"}
            }
            
            allocations_cursor = alloc_collection.find(query)
            tasks_to_process = defaultdict(lambda: {"pods": set(), "f5_classes": set()})
            
            for doc in allocations_cursor:
                tag = doc.get("tag")
                if not tag: continue

                for course in doc.get("courses", []):
                    c_vendor, c_name = course.get("vendor"), course.get("course_name")
                    if not c_vendor or not c_name or c_vendor.lower() != vendor.lower():
                        continue

                    for pd in course.get("pod_details", []):
                        if not (host := pd.get("host")): continue
                        
                        item_num, is_f5 = None, c_vendor.lower() == 'f5'
                        try:
                            if is_f5 and pd.get("class_number") is not None:
                                item_num = int(pd.get("class_number"))
                            elif not is_f5 and pd.get("pod_number") is not None:
                                item_num = int(pd.get("pod_number"))
                        except (ValueError, TypeError): item_num = None

                        if start_pod_filter is not None and end_pod_filter is not None:
                            if item_num is None or not (start_pod_filter <= item_num <= end_pod_filter):
                                continue
                        
                        group_key = (tag, c_name, c_vendor, host)
                        if is_f5 and pd.get("class_number") is not None:
                            tasks_to_process[group_key]['f5_classes'].add(pd["class_number"])
                        elif pd.get("pod_number") is not None:
                            tasks_to_process[group_key]['pods'].add(pd["pod_number"])

            if not tasks_to_process:
                filter_msg = f" within the range {start_pod_filter}-{end_pod_filter}" if start_pod_filter is not None else ""
                print(f"No valid, tagged allocations found for this vendor{filter_msg}.")
                return

            final_jobs = []
            for group_key, items in tasks_to_process.items():
                tag, course, c_vendor, host = group_key
                for class_num in sorted(list(items['f5_classes'])):
                    base_args = {'vendor': c_vendor, 'course': course, 'host': host, 'tag': tag, 'verbose': verbose, 'class_number': class_num}
                    if operation == 'rebuild':
                        final_jobs.append({'args': {**base_args, 'command': 'setup', 're_build': True}, 'desc': f"Rebuild Class {class_num} for '{course}' on '{host}' (Tag: {tag})"})
                    elif operation == 'teardown':
                        final_jobs.append({'args': {**base_args, 'command': 'teardown'}, 'desc': f"Teardown Class {class_num} for '{course}' on '{host}' (Tag: {tag})"})
                    elif operation in ['start', 'stop']:
                         final_jobs.append({'args': {**base_args, 'command': 'manage', 'operation': operation}, 'desc': f"{operation.capitalize()} Class {class_num} for '{course}' on '{host}' (Tag: {tag})"})

                pod_ranges = _create_contiguous_ranges(items['pods'])
                for start_pod, end_pod in pod_ranges:
                    range_str = f"{start_pod}" if start_pod == end_pod else f"{start_pod}-{end_pod}"
                    base_args = {'vendor': c_vendor, 'course': course, 'host': host, 'tag': tag, 'verbose': verbose, 'start_pod': start_pod, 'end_pod': end_pod}
                    if c_vendor.lower() == 'f5':
                        base_args['class_number'] = next(iter(items['f5_classes']), None)
                    if operation == 'rebuild':
                        final_jobs.append({'args': {**base_args, 'command': 'setup', 're_build': True}, 'desc': f"Rebuild Pods {range_str} for '{course}' on '{host}' (Tag: {tag})"})
                    elif operation == 'teardown':
                        final_jobs.append({'args': {**base_args, 'command': 'teardown'}, 'desc': f"Teardown Pods {range_str} for '{course}' on '{host}' (Tag: {tag})"})
                    elif operation in ['start', 'stop']:
                        final_jobs.append({'args': {**base_args, 'command': 'manage', 'operation': operation}, 'desc': f"{operation.capitalize()} Pods {range_str} for '{course}' on '{host}' (Tag: {tag})"})
            
            if not final_jobs:
                print("No specific operations could be generated from the found allocations.")
                return

            print("\nThe following operations will be performed:\n")
            for job in sorted(final_jobs, key=lambda x: x['desc']): print(f"  - {job['desc']}")
            print(f"\nTotal operations to run: {len(final_jobs)}")
            if input("\nAre you sure you want to proceed? (yes/no): ").lower().strip() != 'yes':
                print("\nOperation cancelled by user."); return

            print(f"\nUser confirmed. Submitting {len(final_jobs)} operations now...")
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_job = {
                    executor.submit(
                        (globals()[f"{job['args']['command']}_environment"]), job['args'], OperationLogger(job['args']['command'], job['args'])
                    ): job['desc']
                    for job in final_jobs
                }
                print("\n--- Waiting for all operations to complete ---")
                for future in as_completed(future_to_job):
                    desc = future_to_job[future]
                    try:
                        future.result()
                        print(f"  [SUCCESS] Operation completed: {desc}")
                    except Exception as exc:
                        print(f"  [FAILURE] Operation failed: {desc}. Error: {exc}")
            print("\n--- Vendor-Level Operation Finished ---")
    except PyMongoError as e: print(f"Database Error: {e}", file=sys.stderr)
    except Exception as e: print(f"An unexpected error occurred: {e}", file=sys.stderr)