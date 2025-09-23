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

# --- NEW Imports for host-level operations ---
from vcenter_utils import get_vcenter_instance
from config_utils import get_host_by_name
from managers.vm_manager import VmManager
from pyVmomi import vim
# --- END NEW Imports ---


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


# --- NEW FUNCTION FOR HOST-LEVEL OPERATIONS ---
def perform_host_level_operation(vendor: str, host: str, operation: str, yes: bool, thread_count: int, verbose: bool):
    """
    Finds all resource pools for a vendor on a specific host and performs a power operation.
    """
    print(f"--- Starting Host-Level Operation ---")
    print(f"Vendor: {vendor.upper()}")
    print(f"Host: {host}")
    print(f"Operation: {operation.capitalize()}")
    print("---------------------------------------")

    try:
        host_details = get_host_by_name(host)
        if not host_details:
            print(f"Error: Host '{host}' not found in the database.", file=sys.stderr)
            return

        service_instance = get_vcenter_instance(host_details)
        if not service_instance:
            print(f"Error: Could not connect to vCenter for host '{host}'.", file=sys.stderr)
            return

        vmm = VmManager(service_instance)

        host_obj = vmm.get_obj([vim.HostSystem], host_details['fqdn'])
        if not host_obj:
            print(f"Error: Host object '{host_details['fqdn']}' not found in vCenter.", file=sys.stderr)
            return

        root_rp_for_host = host_obj.parent.resourcePool

        host_short_2char_from_fqdn = host_details['fqdn'][0:2]
        patterns = {
            'cp': re.compile(r'^cp-(maestro-)?pod\d+$'),
            'pa': re.compile(r'^pa-pod\d+$'),
            'f5': re.compile(r'^f5-class\d+$'),
            'av': re.compile(r'^av-(ipo-)?pod\d+$'),
            'nu': re.compile(rf'^nu-pod\d+-{host_short_2char_from_fqdn}$'),
            'pr': re.compile(r'^(pr-pod\d+|.*-pod\d+)$') # Generalize PR slightly
        }
        vendor_pattern = patterns.get(vendor.lower())
        if not vendor_pattern:
            print(f"Error: No resource pool naming convention defined for vendor '{vendor}'.", file=sys.stderr)
            return

        def collect_rps_recursive(rp_root, collected_list):
            for child_rp in rp_root.resourcePool:
                if vendor_pattern.match(child_rp.name):
                    collected_list.append(child_rp)
                # Recurse into all children, as a matching RP might be nested
                collect_rps_recursive(child_rp, collected_list)

        target_rps = []
        collect_rps_recursive(root_rp_for_host, target_rps)

        if not target_rps:
            print(f"No resource pools matching the '{vendor.upper()}' naming convention found on host '{host}'.")
            return

        print("\nThe following resource pools will be affected:\n")
        for rp in sorted(target_rps, key=lambda x: x.name):
            print(f"  - {rp.name}")
        
        print(f"\nTotal resource pools to {operation}: {len(target_rps)}")
        
        if not yes:
            if input("\nAre you sure you want to proceed? (yes/no): ").lower().strip() != 'yes':
                print("\nOperation cancelled by user.")
                return
        else:
            print("\nNon-interactive mode (-y) detected. Proceeding automatically.")

        print(f"\nUser confirmed. Submitting {len(target_rps)} operations now...")

        def power_op_worker(vm_obj):
            """Performs a power operation on a single VM object."""
            if operation == 'start':
                vmm.poweron_vm(vm_obj.name)
            elif operation == 'stop':
                vmm.poweroff_vm(vm_obj.name)
            elif operation == 'reboot':
                # Implement reboot as a safe power-off then power-on
                vmm.poweroff_vm(vm_obj.name)
                vmm.poweron_vm(vm_obj.name)

        def rp_worker(rp_obj):
            """Worker to manage all VMs within a resource pool."""
            all_vms_in_rp = []
            def collect_vms_recursive(current_rp):
                all_vms_in_rp.extend(current_rp.vm)
                for child_rp in current_rp.resourcePool:
                    collect_vms_recursive(child_rp)
            collect_vms_recursive(rp_obj)

            if not all_vms_in_rp:
                return (rp_obj.name, "success", "No VMs found in RP.")
            
            errors = []
            with ThreadPoolExecutor(max_workers=10) as vm_executor:
                future_to_vm = {vm_executor.submit(power_op_worker, vm): vm.name for vm in all_vms_in_rp}
                for future in as_completed(future_to_vm):
                    vm_name = future_to_vm[future]
                    try:
                        future.result()
                    except Exception as e:
                        errors.append(f"VM '{vm_name}': {e}")
            
            if errors:
                return (rp_obj.name, "failure", "; ".join(errors))
            return (rp_obj.name, "success", f"Operation '{operation}' completed for {len(all_vms_in_rp)} VM(s).")

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_rp = {executor.submit(rp_worker, rp): rp.name for rp in target_rps}
            
            print("\n--- Waiting for all operations to complete ---")
            for future in as_completed(future_to_rp):
                name = future_to_rp[future]
                try:
                    rp_name, status, message = future.result()
                    if status == 'success':
                        print(f"  [SUCCESS] Operation completed for RP: {rp_name}")
                    else:
                        print(f"  [FAILURE] Operation for RP {rp_name} completed with errors: {message}")
                except Exception as exc:
                    print(f"  [FAILURE] Operation failed catastrophically for RP: {name}. Error: {exc}")
                    if verbose:
                        import traceback
                        traceback.print_exc()

        print("\n--- Host-Level Operation Finished ---")
    except Exception as e:
        print(f"An unexpected error occurred during host-level operation: {e}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc()