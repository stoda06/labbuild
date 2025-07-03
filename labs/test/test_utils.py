import logging
import re
from typing import Set, List, Dict, Any, Optional
from collections import defaultdict
from tabulate import tabulate
from db_utils import mongo_client, DB_NAME, ALLOCATION_COLLECTION

logger = logging.getLogger('labbuild.test.utils')

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
    jobs = []
    raw_jobs = [] # For non-F5 grouping
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
                        
                        if not (start_num <= class_num <= end_num): continue
                        if host_filter and host.lower() != host_filter.lower(): continue

                        pod_numbers = [p.get('pod_number') for p in pod_detail.get('pods', []) if p.get('pod_number') is not None]
                        if not pod_numbers: continue

                        job = {
                            "vendor": vendor.lower(), "tag": tag, "course_name": course_name,
                            "host": host, "class_number": class_num,
                            "start_pod": min(pod_numbers), "end_pod": max(pod_numbers)
                        }
                        jobs.append(job)
                else: # Non-F5
                    for pod_detail in course.get("pod_details", []):
                        pod_num = pod_detail.get("pod_number")
                        host = pod_detail.get("host")
                        if pod_num is None or host is None: continue
                        
                        if not (start_num <= pod_num <= end_num): continue
                        if host_filter and host.lower() != host_filter.lower(): continue
                        
                        job = {
                            "vendor": vendor.lower(), "tag": tag, "course_name": course_name,
                            "host": host, "pod_number": pod_num
                        }
                        raw_jobs.append(job)

    if vendor.lower() != 'f5' and raw_jobs:
        logger.debug(f"Grouping {len(raw_jobs)} non-F5 jobs into ranges.")
        return group_non_f5_jobs(raw_jobs)

    return jobs

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

def display_test_jobs_and_confirm(jobs: List[Dict[str, Any]], vendor: str) -> bool:
    """Displays a table of jobs and asks for user confirmation to proceed."""
    if not jobs:
        print(f"No testable allocations found for vendor '{vendor}' (after exclusions).")
        return False

    is_f5 = vendor.lower() == 'f5'
    headers = ["Tag", "Course Name", "Host", "Class Number", "Pod Range"] if is_f5 else ["Tag", "Course Name", "Host", "Pod Range"]
    table_data = []

    # Sort for predictable output
    sort_key = lambda x: (x.get('tag', ''), x.get('class_number', 0), x.get('start_pod', 0))
    for job in sorted(jobs, key=sort_key):
        pod_range = f"{job['start_pod']}-{job['end_pod']}"
        if is_f5:
            table_data.append([job['tag'], job['course_name'], job['host'], job['class_number'], pod_range])
        else:
            table_data.append([job['tag'], job['course_name'], job['host'], pod_range])

    print("\n" + "="*80)
    print(f"The following allocations for vendor '{vendor.upper()}' will be tested:")
    print("="*80)
    print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"))
    print("="*80)

    try:
        confirm = input("Proceed with these tests? (y/N): ").lower().strip()
        if confirm == 'y':
            return True
        else:
            print("Operation cancelled by user.")
            return False
    except (KeyboardInterrupt, EOFError):
        print("\nOperation cancelled by user.")
        return False