# listing.py
"""Functions for listing courses and allocations."""

import logging
import sys
from collections import defaultdict
from typing import Optional, Dict, List, Any, Generator
from concurrent.futures import ThreadPoolExecutor, wait
import threading

import pymongo
from pymongo.errors import PyMongoError
from pyVmomi import vim

# Local Imports
from db_utils import mongo_client, delete_from_database # Need DB utils
from constants import DB_NAME, ALLOCATION_COLLECTION, COURSE_CONFIG_COLLECTION # Need constants
from config_utils import get_host_by_name # Need config utils
from vcenter_utils import get_vcenter_instance # Need vCenter utils
from managers.vm_manager import VmManager # Need VmManager for testing

logger = logging.getLogger('labbuild.listing')

# --- Globals for thread-safe vCenter connection caching in test mode ---
vcenter_connections_cache: Dict[str, Optional[VmManager]] = {}
vcenter_connection_lock = threading.Lock()
# ---

def list_vendor_courses(vendor: str):
    """List available courses for the given vendor from MongoDB."""
    print(f"Looking for courses for vendor '{vendor}'...")
    try:
        with mongo_client() as client:
            if not client:
                 print("\nError: Could not connect to MongoDB to list courses.")
                 sys.exit(1)

            db = client[DB_NAME]
            collection = db[COURSE_CONFIG_COLLECTION]
            # Case-insensitive query using regex
            courses_cursor = collection.find(
                {"vendor_shortcode": {"$regex": f"^{vendor}$", "$options": "i"}},
                {"course_name": 1, "_id": 0} # Projection
            )
            # Use list comprehension and check for key existence robustly
            courses = sorted([doc["course_name"] for doc in courses_cursor if doc and "course_name" in doc])

            if courses:
                print(f"\nAvailable courses for vendor '{vendor}':")
                for course in courses:
                    print(f"  - {course}")
            else:
                print(f"\nNo courses found for vendor '{vendor}'.")
    except PyMongoError as e:
         print(f"\nError accessing course configurations (PyMongoError): {e}")
    except Exception as e:
        print(f"\nError accessing course configurations: {e}")
    # Exit after listing, regardless of success/failure after attempting
    sys.exit(0)

def check_vr_vm_existence(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Worker function to check if a specific VR VM exists on the correct vCenter.
    Handles connection caching in a thread-safe manner.
    Args: task_data: Dictionary containing 'vr_vm_name', 'host_details', 'vr_check_id'.
    Returns: Dictionary containing results including 'exists', 'error', and original data.
    """
    vr_vm_name = task_data.get("vr_vm_name"); host_details = task_data.get("host_details"); vr_check_id = task_data.get("vr_check_id"); vmm = None
    result = {"vr_vm_name": vr_vm_name, "vr_check_id": vr_check_id, "exists": False, "error": None, "original_task_data": task_data }
    if not vr_vm_name: result["error"] = "VR VM name not determined."; logger.warning(f"Skip check {vr_check_id}: VR name missing."); return result
    if not host_details or not host_details.get("vcenter"): result["error"] = f"Missing host/vCenter for '{vr_vm_name}'."; logger.warning(f"Skip check {vr_check_id}: Host/vCenter missing."); return result
    vcenter_host = host_details["vcenter"]

    with vcenter_connection_lock: # Thread-safe cache access
        if vcenter_host in vcenter_connections_cache:
            vmm = vcenter_connections_cache[vcenter_host];
            if vmm: logger.debug(f"[{vr_check_id}] Use cached vCenter conn: {vcenter_host}")
            else: logger.warning(f"[{vr_check_id}] Use cached FAILED conn state: {vcenter_host}.")
        else:
            logger.info(f"[{vr_check_id}] Attempt new conn to vCenter '{vcenter_host}'...")
            service_instance = get_vcenter_instance(host_details)
            if service_instance:
                vmm = VmManager(service_instance); vcenter_connections_cache[vcenter_host] = vmm; logger.info(f"[{vr_check_id}] Connected successfully: '{vcenter_host}'.")
            else: logger.error(f"[{vr_check_id}] Failed connect to vCenter '{vcenter_host}'."); vcenter_connections_cache[vcenter_host] = None; result["error"] = f"Failed connect to vCenter '{vcenter_host}'."

    if vmm: # Perform check if connection available
        try:
            logger.debug(f"[{vr_check_id}] Checking VM '{vr_vm_name}' on {vcenter_host}...")
            vm_obj = vmm.get_obj([vim.VirtualMachine], vr_vm_name)
            result["exists"] = vm_obj is not None
            if result["exists"]: logger.info(f"[{vr_check_id}] VR VM '{vr_vm_name}' CONFIRMED.")
            else: logger.warning(f"[{vr_check_id}] VR VM '{vr_vm_name}' NOT FOUND.")
        except Exception as e: logger.error(f"[{vr_check_id}] Error check VM '{vr_vm_name}': {e}", exc_info=True); result["error"] = f"API error: {e}"; result["exists"] = False
    else: result["error"] = f"vCenter conn unavailable for {vcenter_host}."; result["exists"] = False
    return result

def _get_min_id(items: List[Dict]) -> float:
    """Helper to find the minimum pod or class number in a list of allocation items."""
    min_id = float('inf')
    for item in items:
        item_id = item.get("class_num") if item.get("is_f5_class") else item.get("pod_num")
        if item_id is not None and item_id < min_id:
            min_id = item_id
    return min_id

def list_allocations(vendor_shortcode: str, start_filter: Optional[int] = None, end_filter: Optional[int] = None, test_mode: bool = False):
    """
    Queries MongoDB for allocations, prints summary. Optionally filters by range.
    If test_mode, concurrently checks VR VM existence via vCenter and removes invalid DB entries.
    MODIFIED: Now sorts and prints the output as a flat, single-line list.
    """
    filter_active = start_filter is not None and end_filter is not None
    if filter_active and start_filter > end_filter:
        print(f"Error: Start filter ({start_filter}) > end filter ({end_filter})."); logger.error(f"Invalid list filter range"); return
    filter_range = set(range(start_filter, end_filter + 1)) if filter_active else set()

    mode_msg = " [TEST MODE - Checking VR VM Existence]" if test_mode else ""
    filter_msg = f" from {start_filter} to {end_filter}" if filter_active else ""
    logger.info(f"Listing allocations for vendor: {vendor_shortcode}{filter_msg}{mode_msg}")
    print(f"\n--- Current Allocations for Vendor: {vendor_shortcode.upper()}{filter_msg}{mode_msg} ---")

    output_found = False
    global vcenter_connections_cache; vcenter_connections_cache = {} # Reset connection cache

    items_to_potentially_delete = []
    all_processed_items = []

    try:
        # --- Phase 1: Gather potential items and info needed for testing ---
        with mongo_client() as client:
            if not client: logger.error("DB connection failed."); print("Error: DB Connection failed."); return

            db = client[DB_NAME]; alloc_collection = db[ALLOCATION_COLLECTION]; course_collection = db[COURSE_CONFIG_COLLECTION]
            query = {"courses.vendor": {"$regex": f"^{vendor_shortcode}$", "$options": "i"}}
            alloc_cursor = alloc_collection.find(query); course_config_cache = {}

            for tag_doc in alloc_cursor:
                tag = tag_doc.get("tag", "Unknown Tag")
                courses_in_tag = tag_doc.get("courses", [])
                if not isinstance(courses_in_tag, list): logger.warning(f"Tag '{tag}' malformed courses. Skip."); continue

                for course in courses_in_tag:
                    if not isinstance(course, dict): logger.warning(f"Skip malformed course tag '{tag}'."); continue
                    course_vendor = course.get("vendor")
                    if course_vendor is not None and isinstance(course_vendor, str) and course_vendor.lower() == vendor_shortcode.lower():
                        course_name = course.get("course_name", "Unknown Course")
                        course_config_data = course_config_cache.get(course_name)
                        if not course_config_data:
                            try:
                                config_doc = course_collection.find_one({"course_name": course_name})
                                if config_doc: course_config_data = config_doc; course_config_cache[course_name] = config_doc
                                else: logger.warning(f"Could not find course config for '{course_name}'.")
                            except PyMongoError as e: logger.error(f"Failed fetch course config '{course_name}': {e}")

                        pod_details_list = course.get("pod_details", [])
                        if not isinstance(pod_details_list, list): logger.warning(f"Course '{course_name}' tag '{tag}' malformed pod_details. Skip."); continue

                        for pod_detail in pod_details_list:
                            if not isinstance(pod_detail, dict): logger.warning(f"Skip malformed pod_detail course '{course_name}' tag '{tag}'."); continue
                            host = pod_detail.get("host", pod_detail.get("pod_host", "Unknown Host"))
                            pod_num = pod_detail.get("pod_number"); class_num = pod_detail.get("class_number")
                            item_id = None; vr_check_id = None; is_f5_class_summary = False
                            if vendor_shortcode.lower() == 'f5' and class_num is not None: item_id = class_num; is_f5_class_summary = True; vr_check_id = f"Class {class_num}"
                            elif pod_num is not None: item_id = pod_num; vr_check_id = f"Pod {pod_num}"
                            if filter_active and (item_id is None or item_id not in filter_range): continue
                            
                            item_data = {
                                "tag": tag, "course_name": course_name, "host": host, 
                                "pod_num": pod_num, "class_num": class_num, "item_id": item_id, 
                                "vr_check_id": vr_check_id, "is_f5_class": is_f5_class_summary, 
                                "nested_pods": pod_detail.get("pods", []) if is_f5_class_summary else [], 
                                "course_config_data": course_config_data, 
                                "host_details": get_host_by_name(host) if test_mode and host != "Unknown Host" else None,
                                "type": "f5_class" if is_f5_class_summary else "pod"
                            }
                            all_processed_items.append(item_data)

        # --- Phase 2: Run VM checks concurrently ---
        valid_items_after_test = []
        if test_mode:
            # (Test mode logic remains unchanged)
            tasks_to_run = []
            for item in all_processed_items:
                 vr_vm_name = None; course_config_data = item["course_config_data"]; is_f5_class = item["is_f5_class"]; class_num = item["class_num"]; pod_num = item["pod_num"]
                 if course_config_data:
                     vr_comp = next((c for c in course_config_data.get("components", []) if isinstance(c,dict) and ("vr" in c.get("component_name", "").lower() or "vr" in c.get("clone_name","").lower())), None)
                     if not vr_comp: vr_comp = next((c for g in course_config_data.get("groups",[]) if isinstance(g.get("component"),list) for c in g["component"] if isinstance(c,dict) and ("vr" in c.get("component_name", "").lower() or "vr" in c.get("clone_vm","").lower())), None)
                     vr_patt = None
                     if vr_comp: vr_patt = vr_comp.get("clone_vm") or vr_comp.get("clone_name");
                     if vr_patt:
                         if is_f5_class and class_num is not None: vr_vm_name = vr_patt.replace("{Y}", str(class_num))
                         elif pod_num is not None: vr_vm_name = vr_patt.replace("{X}", str(pod_num))
                 if vr_vm_name and item["host_details"]: tasks_to_run.append({"vr_vm_name": vr_vm_name, "host_details": item["host_details"], "vr_check_id": item["vr_check_id"], "original_item_data": item})
                 elif item["host_details"]: logger.warning(f"VR name unknown {item['vr_check_id']}. Skip VM check."); valid_items_after_test.append(item)
                 else: logger.warning(f"Host unknown {item['vr_check_id']}. Mark delete."); items_to_potentially_delete.append((item["tag"], item["course_name"], item["pod_num"], item["class_num"]))

            if not tasks_to_run and not items_to_potentially_delete: print("\n--- Test Mode: No VR VMs to check based on config/filters. ---"); valid_items_after_test = all_processed_items
            elif tasks_to_run:
                print(f"\n--- Test Mode: Checking {len(tasks_to_run)} VR VMs concurrently... ---"); test_results = []
                max_workers = min(len(tasks_to_run), 10)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_task = {executor.submit(check_vr_vm_existence, task): task for task in tasks_to_run}; done_futures, _ = wait(future_to_task)
                    for future in done_futures:
                         try: test_results.append(future.result())
                         except Exception as exc: task_info = future_to_task[future]; logger.error(f"Thread task {task_info.get('vr_check_id')} exception: {exc}", exc_info=True); test_results.append({"vr_vm_name": task_info.get("vr_vm_name"), "vr_check_id": task_info.get("vr_check_id"), "exists": False, "error": f"Thread exception: {exc}", "original_item_data": task_info.get("original_item_data") })
                print("--- Test Mode: VM Checks Complete ---"); tested_items_map = {res["original_item_data"]["item_id"]: res for res in test_results}

                items_found_during_test = set()
                for res in test_results:
                    item = res["original_item_data"]
                    if res["exists"]:
                        valid_items_after_test.append(item)
                        if item.get('item_id') is not None: items_found_during_test.add(item['item_id'])
                    else:
                        items_to_potentially_delete.append((item["tag"], item["course_name"], item["pod_num"], item["class_num"]))

                for item in all_processed_items:
                    if item.get("item_id") not in tested_items_map and item["host"] != "Unknown Host" and item["host_details"] is not None:
                        valid_items_after_test.append(item)

            else: print("\n--- Test Mode: Only items with unknown hosts marked for deletion. ---"); valid_items_after_test = []
        else: valid_items_after_test = all_processed_items

        # --- Phase 3: Perform Deletions ---
        if test_mode and items_to_potentially_delete:
            # (Deletion logic remains unchanged)
            print("\n--- Deleting DB Entries for Missing or Untestable VR VMs ---"); deleted_count = 0
            unique_deletions = set(items_to_potentially_delete)
            for del_tag, del_course, del_pod, del_class in unique_deletions:
                log_id_del = f"Class {del_class}" if del_pod is None else f"Pod {del_pod}";
                if del_class is not None and del_pod is not None: log_id_del += f" in Class {del_class}"
                print(f"  Deleting: Tag='{del_tag}', Course='{del_course}', Item='{log_id_del}'"); logger.info(f"Deleting DB entry (test mode): Tag='{del_tag}', Course='{del_course}', Pod='{del_pod}', Class='{del_class}'")
                delete_from_database(del_tag, del_course, del_pod, del_class); deleted_count += 1
            print(f"--- Deletion Phase Complete ({deleted_count} items removed) ---")
        elif test_mode: print("\n--- Test Mode: No invalid entries found to delete. ---")

        # --- Phase 4: Group, Sort, and Print Output (REVISED LOGIC) ---
        if valid_items_after_test:
            output_found = True
            
            # Group items by a unique key of (tag, course, host)
            grouped_for_display = defaultdict(list)
            for item in valid_items_after_test:
                key = (item['tag'], item['course_name'], item['host'])
                grouped_for_display[key].append(item)

            display_lines = []
            for (tag, course_name, host), details in grouped_for_display.items():
                pod_numbers = sorted([d["pod_num"] for d in details if d["type"] == "pod" and d.get("pod_num") is not None])
                f5_details_display = sorted([d for d in details if d["type"] == "f5_class"], key=lambda x: x["class_num"])
                
                display_items_str = []
                item_prefix = ""
                
                # Format pod ranges
                if pod_numbers:
                    item_prefix = "Pods"
                    ranges = []; start = end = None
                    if len(pod_numbers) > 0: start = pod_numbers[0]; end = start
                    if start is not None:
                        for i in range(1, len(pod_numbers)):
                            if pod_numbers[i] == end + 1: end = pod_numbers[i]
                            else: ranges.append((start, end)); start = end = pod_numbers[i]
                        ranges.append((start, end))
                    for start, end in ranges: display_items_str.append(str(start) if start == end else f"{start}-{end}")
                
                # Format F5 classes
                if f5_details_display:
                    item_prefix = "Classes" if not pod_numbers else "Pods/Classes"
                    for f5 in f5_details_display:
                        f5_nested_pods = {p['pod_number'] for p in f5['nested_pods'] if p.get('pod_number') is not None}
                        pod_list_str = f" (Pods: {', '.join(map(str, sorted(list(f5_nested_pods))))})" if f5_nested_pods else ""
                        display_items_str.append(f"Class {f5['class_num']}{pod_list_str}")
                
                if display_items_str:
                    display_lines.append({
                        "prefix": item_prefix,
                        "items_str": ", ".join(display_items_str),
                        "course": course_name,
                        "host": host,
                        "tag": tag,
                        "sort_id": _get_min_id(details)
                    })

            # Sort the final flat list by the minimum pod/class number
            display_lines.sort(key=lambda x: x['sort_id'])
            
            # Print the sorted flat list
            for line in display_lines:
                print(f"{line['prefix']} {line['items_str']} for course '{line['course']}' on host '{line['host']}' (Tag: {line['tag']})")


        if not output_found:
            filter_msg = f" matching range {start_filter}-{end_filter}" if filter_active else ""
            print(f"  No active (and valid, if tested) allocations found for vendor '{vendor_shortcode}'{filter_msg}.")

    except PyMongoError as e: logger.error(f"Error listing allocations: {e}", exc_info=True); print(f"Error: DB query failed: {e}")
    except Exception as e: logger.error(f"Unexpected error listing allocations: {e}", exc_info=True); print(f"Error: {e}")

    print("\n-------------------------------------------------")