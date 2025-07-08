import logging
import re
import threading
from typing import Set, List, Dict, Any, Optional
from collections import defaultdict
from tabulate import tabulate
from db_utils import mongo_client, DB_NAME, ALLOCATION_COLLECTION

logger = logging.getLogger('labbuild.test.utils')

# Color constants
CYAN = '\033[96m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
ENDC = '\033[0m'

def execute_single_test_worker(args_dict: Dict[str, Any], print_lock: threading.Lock) -> List[Dict[str, Any]]:
    """
    Worker function for the thread pool. It executes a single test run and returns structured results.
    """
    vendor = args_dict.get("vendor", "").lower()
    start = args_dict.get("start_pod")
    end = args_dict.get("end_pod")
    host = args_dict.get("host")
    group = args_dict.get("group")
    component_arg = args_dict.get("component")

    # Final safeguard validation
    required = ['vendor', 'start_pod', 'end_pod', 'host', 'group']
    if any(args_dict.get(arg) is None for arg in required):
        return [{'status': 'failed', 'error': f"Internal Error: Missing args for worker: {args_dict}"}]

    # Dispatch to vendor-specific test script, passing the lock
    try:
        if vendor == "cp":
            from labs.test import checkpoint
            argv = ["-s", str(start), "-e", str(end), "-H", host, "-g", group]
            if component_arg: argv.extend(["-c", component_arg])
            return checkpoint.main(argv, print_lock=print_lock)
        elif vendor == "pa":
            from labs.test import palo
            argv = ["-s", str(start), "-e", str(end), "--host", host, "-g", group]
            if component_arg: argv.extend(["-c", component_arg])
            return palo.main(argv, print_lock=print_lock)
        elif vendor == "nu":
            from labs.test import nu
            argv = ["-s", str(start), "-e", str(end), "--host", host, "-g", group]
            if component_arg: argv.extend(["-c", component_arg])
            return nu.main(argv, print_lock=print_lock)
        elif vendor == "av":
            from labs.test import avaya
            argv = ["-s", str(start), "-e", str(end), "--host", host, "-g", group]
            if component_arg: argv.extend(["-c", component_arg])
            return avaya.main(argv, print_lock=print_lock)
        elif vendor == "f5":
            classnum = args_dict.get("class_number")
            if classnum is None: return [{'status': 'failed', 'error': "Internal Error: F5 class_number missing."}]
            from labs.test import f5
            argv = ["-s", str(start), "-e", str(end), "--host", host, "-g", group, "--classnum", str(classnum)]
            if component_arg: argv.extend(["-c", component_arg])
            return f5.main(argv, print_lock=print_lock)
        else:
            with print_lock:
                print(f"Vendor '{vendor}' is not yet supported for testing.")
            return [{'status': 'failed', 'error': f"Unsupported vendor: {vendor}"}]
    except Exception as e:
        with print_lock:
            logger.error(f"Test worker for {vendor} pod {start}-{end} failed: {e}", exc_info=True)
        return [{
            'pod': start,
            'class_number': args_dict.get('class_number'),
            'component': 'Module Execution',
            'ip': host,
            'port': 'N/A',
            'status': 'CRASHED',
            'error': str(e)
        }]

def parse_exclude_string(exclude_str: Optional[str]) -> Set[int]:
    """Parses a string like '1-5,8,10-12' into a set of integers."""
    if not exclude_str:
        return set()

    excluded_numbers = set()
    # Remove brackets if present, e.g., from "[1-5, 10]"
    exclude_str = exclude_str.strip().strip('[]')
    parts = exclude_str.replace(" ", "").split(',')
    
    for part in parts:
        if not part:
            continue
        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                excluded_numbers.update(range(start, end + 1))
            except ValueError:
                logger.warning(f"Invalid range in exclude string: '{part}'")
        else:
            try:
                excluded_numbers.add(int(part))
            except ValueError:
                logger.warning(f"Invalid number in exclude string: '{part}'")
    return excluded_numbers

def group_non_f5_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Groups individual pod jobs into contiguous ranges for efficient testing."""
    if not jobs:
        return []

    # Group by a unique key: tag, course_name, host
    grouped = defaultdict(list)
    for job in jobs:
        key = (job['tag'], job['course_name'], job['host'])
        grouped[key].append(job['pod_number'])
    
    final_jobs = []
    vendor = jobs[0]['vendor'] if jobs else '' # Vendor is the same for all

    for (tag, course_name, host), pod_numbers in grouped.items():
        if not pod_numbers:
            continue
        
        pod_numbers.sort()
        
        # Find and create jobs for contiguous ranges
        start_pod = pod_numbers[0]
        for i in range(len(pod_numbers)):
            # Check if it's the last pod or if the next one breaks the sequence
            if i == len(pod_numbers) - 1 or pod_numbers[i+1] != pod_numbers[i] + 1:
                end_pod = pod_numbers[i]
                final_jobs.append({
                    "vendor": vendor,
                    "tag": tag,
                    "course_name": course_name,
                    "host": host,
                    "start_pod": start_pod,
                    "end_pod": end_pod
                })
                # If not the last pod, start a new range with the next one
                if i < len(pod_numbers) - 1:
                    start_pod = pod_numbers[i+1]
                    
    return final_jobs

def get_test_jobs_for_range(vendor: str, start_num: int, end_num: int, host_filter: Optional[str] = None, group_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Queries allocations for a specific vendor and number range (pods or classes).
    Generates a list of test jobs, automatically determining course and host.
    User-provided host and group are used as filters.
    """
    raw_jobs = []
    f5_job_collector = {} # Key: (vendor, course, host, class_num), Value: {base_info, set_of_pods}

    logger.info(f"Searching for jobs for {vendor} in range {start_num}-{end_num} (Host: {host_filter or 'any'}, Group: {group_filter or 'any'})")
    with mongo_client() as client:
        if not client:
            logger.error("Cannot get test jobs: DB connection failed.")
            return []

        db = client[DB_NAME]
        collection = db[ALLOCATION_COLLECTION]
        
        allocations = collection.find({"courses.vendor": vendor.lower()})

        for alloc in allocations:
            tag = alloc.get("tag")
            for course in alloc.get("courses", []):
                if course.get("vendor", "").lower() != vendor.lower():
                    continue
                
                course_name = course.get("course_name")
                if group_filter and course_name != group_filter:
                    continue
                
                if vendor.lower() == 'f5':
                    for pod_detail in course.get("pod_details", []):
                        class_num = pod_detail.get("class_number")
                        host = pod_detail.get("host")
                        if class_num is None or host is None: continue
                        if host_filter and host.lower() != host_filter.lower(): continue

                        # --- MODIFICATION START ---
                        matching_pods = []
                        for p in pod_detail.get('pods', []):
                            f5_pod_num_raw = p.get('pod_number')
                            if f5_pod_num_raw is None:
                                continue
                            try:
                                f5_pod_num = int(f5_pod_num_raw)
                                if start_num <= f5_pod_num <= end_num:
                                    matching_pods.append(f5_pod_num)
                            except (ValueError, TypeError):
                                logger.warning(f"Skipping non-integer F5 pod number '{f5_pod_num_raw}' in course '{course_name}'.")
                                continue
                        # --- MODIFICATION END ---
                        
                        if matching_pods:
                            job_key = (vendor.lower(), course_name, host, class_num)
                            if job_key not in f5_job_collector:
                                f5_job_collector[job_key] = {
                                    "base": { "vendor": vendor.lower(), "tag": tag, "course_name": course_name, "host": host, "class_number": class_num },
                                    "pods": set()
                                }
                            f5_job_collector[job_key]["pods"].update(matching_pods)
                else:
                    for pod_detail in course.get("pod_details", []):
                        # --- MODIFICATION START ---
                        pod_num_raw = pod_detail.get("pod_number")
                        host = pod_detail.get("host")
                        if pod_num_raw is None or host is None: continue

                        try:
                            pod_num = int(pod_num_raw)
                        except (ValueError, TypeError):
                            logger.warning(f"Skipping non-integer pod number '{pod_num_raw}' in course '{course_name}'.")
                            continue
                        
                        if not (start_num <= pod_num <= end_num): continue
                        # --- MODIFICATION END ---
                        if host_filter and host.lower() != host_filter.lower(): continue
                        
                        raw_jobs.append({ "vendor": vendor.lower(), "tag": tag, "course_name": course_name, "host": host, "pod_number": pod_num })

    # Finalize jobs from collectors
    final_jobs = []
    for key, collected_job in f5_job_collector.items():
        if collected_job["pods"]:
            job = collected_job["base"]
            job["start_pod"] = min(collected_job["pods"])
            job["end_pod"] = max(collected_job["pods"])
            final_jobs.append(job)

    if vendor.lower() != 'f5' and raw_jobs:
        logger.debug(f"Grouping {len(raw_jobs)} non-F5 jobs into ranges.")
        final_jobs.extend(group_non_f5_jobs(raw_jobs))

    return final_jobs

def get_test_jobs_by_vendor(vendor: str, exclude_set: Set[int]) -> List[Dict[str, Any]]:
    """
    Queries allocations by vendor, creates a list of test jobs, and groups them for efficiency.
    """
    jobs = []
    with mongo_client() as client:
        if not client:
            logger.error("Cannot get test jobs: DB connection failed.")
            return []

        db = client[DB_NAME]
        collection = db[ALLOCATION_COLLECTION]
        
        # Query for all documents where the vendor appears in any of the courses
        allocations = collection.find({"courses.vendor": vendor.lower()})

        for alloc in allocations:
            tag = alloc.get("tag")
            for course in alloc.get("courses", []):
                # Ensure we only process courses for the requested vendor
                if course.get("vendor", "").lower() != vendor.lower():
                    continue

                course_name = course.get("course_name")
                
                # F5 labs are structured by class
                if vendor.lower() == 'f5':
                    for pod_detail in course.get("pod_details", []):
                        class_num = pod_detail.get("class_number")
                        host = pod_detail.get("host")
                        if class_num is None or host is None: continue
                        
                        if class_num in exclude_set:
                            logger.info(f"Excluding F5 class {class_num} for tag '{tag}' based on --exclude list.")
                            continue

                        pod_numbers = [p.get('pod_number') for p in pod_detail.get('pods', []) if p.get('pod_number') is not None]
                        if not pod_numbers: continue

                        job = {
                            "vendor": vendor.lower(), "tag": tag, "course_name": course_name,
                            "host": host, "class_number": class_num,
                            "start_pod": min(pod_numbers), "end_pod": max(pod_numbers)
                        }
                        jobs.append(job)
                else: # Non-F5 labs are structured by individual pods
                    for pod_detail in course.get("pod_details", []):
                        pod_num = pod_detail.get("pod_number")
                        host = pod_detail.get("host")
                        if pod_num is None or host is None: continue
                        
                        if pod_num in exclude_set:
                            logger.info(f"Excluding pod {pod_num} for tag '{tag}' based on --exclude list.")
                            continue
                        
                        job = {
                            "vendor": vendor.lower(), "tag": tag, "course_name": course_name,
                            "host": host, "pod_number": pod_num
                        }
                        jobs.append(job)

    # For non-F5 vendors, group individual pod jobs into ranges
    if vendor.lower() != 'f5':
        return group_non_f5_jobs(jobs)

    return jobs

def display_test_jobs(jobs: List[Dict[str, Any]], vendor: str):
    """Displays a colorful table of jobs to be tested without confirmation."""
    if not jobs:
        return

    is_f5 = vendor.lower() == 'f5'
    
    # Colorize headers
    if is_f5:
        headers = [f"{CYAN}Tag{ENDC}", f"{CYAN}Course Name{ENDC}", f"{CYAN}Host{ENDC}", f"{CYAN}Class Number{ENDC}", f"{CYAN}Pod Range{ENDC}"]
    else:
        headers = [f"{CYAN}Tag{ENDC}", f"{CYAN}Course Name{ENDC}", f"{CYAN}Host{ENDC}", f"{CYAN}Pod Range{ENDC}"]
    
    table_data = []

    # Sort for predictable output
    sort_key = lambda x: (x.get('tag', ''), x.get('class_number', 0), x.get('start_pod', 0))
    for job in sorted(jobs, key=sort_key):
        pod_range = f"{GREEN}{job['start_pod']}-{job['end_pod']}{ENDC}"
        tag = f"{YELLOW}{job['tag']}{ENDC}"
        
        if is_f5:
            table_data.append([tag, job['course_name'], job['host'], job['class_number'], pod_range])
        else:
            table_data.append([tag, job['course_name'], job['host'], pod_range])

    print("\n" + "="*80)
    print(f"The following allocations for vendor '{vendor.upper()}' will be tested:")
    print("="*80)
    print(tabulate(table_data, headers=headers, tablefmt="fancy_grid", disable_numparse=True))
    print("="*80)