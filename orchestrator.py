# --- START OF FILE orchestrator.py ---

# orchestrator.py
"""Handles vendor-specific dispatch and common build/teardown orchestration steps."""

import logging
from typing import Optional, Dict, List, Any, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor
# argparse no longer needed directly here
# import argparse
import time

# Import vendor modules
import labs.setup.avaya as avaya
import labs.setup.checkpoint as checkpoint
import labs.setup.f5 as f5
import labs.setup.palo as palo
import labs.setup.pr as pr
import labs.setup.nu as nu

# Import local utils and helpers
from managers.vcenter import VCenter # Assuming VCenter is the type hint for service_instance
from monitor.prtg import PRTGManager
from db_utils import update_database, mongo_client, get_prtg_url, delete_from_database # Added delete_from_database
from config_utils import fetch_and_prepare_course_config
from operation_logger import OperationLogger
from utils import wait_for_tasks

logger = logging.getLogger('labbuild.orchestrator')

# --- Vendor Maps ---
VENDOR_SETUP_MAP: Dict[str, Callable] = {
    "cp": checkpoint.build_cp_pod, "pa": palo.build_1110_pod, "f5": f5.build_pod,
    "av": avaya.build_aura_pod, "pr": pr.build_pr_pod, "nu": nu.build_nu_pod,
    # Add other vendor setup functions here
}
VENDOR_TEARDOWN_MAP: Dict[str, Callable] = {
    "cp": checkpoint.teardown_pod, "pa": palo.teardown_1110, "f5": f5.teardown_class,
    "av": avaya.teardown_aura, "pr": pr.teardown_pr_pod, "nu": nu.teardown_nu_pod,
    # Add other vendor teardown functions here
}
VENDOR_MONITOR_MAP: Dict[str, Callable] = {
    "cp": checkpoint.add_monitor, "pa": palo.add_monitor, "f5": f5.add_monitor,
    "av": PRTGManager.add_monitor, "pr": PRTGManager.add_monitor,
    "nu": PRTGManager.add_monitor, "ot": PRTGManager.add_monitor, # Default/Other
    # Add other vendor monitor functions here if they differ from default
}

# --- Monitor and DB Update Helper ---
# Updated signature to accept args_dict
def update_monitor_and_database(
    config: Dict[str, Any],
    args_dict: Dict[str, Any],
    data: Dict[str, Any],
    extra_details: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
    retry_delay_seconds: int = 10
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Add PRTG monitor and update database allocation."""
    vendor_shortcode = config.get("vendor_shortcode", "ot")
    # Use the VENDOR_MONITOR_MAP to get the correct add_monitor function
    add_monitor_func = VENDOR_MONITOR_MAP.get(vendor_shortcode, PRTGManager.add_monitor)
    
    prtg_url = None
    prtg_server_preference = args_dict.get('prtg_server') # From CLI args

    # Determine identifier for logging
    if config.get('pod_number') is not None: # Pod-specific context
        monitor_identifier = f"Pod {config.get('pod_number')}"
        if config.get('class_number') is not None: # F5 Pod within a class
            monitor_identifier += f" in Class {config.get('class_number')}"
    elif config.get('class_number') is not None: # Class-level context (e.g., F5 class itself)
        monitor_identifier = f"Class {config.get('class_number')}"
    else: # Fallback if no pod or class number (should ideally not happen for monitored items)
        monitor_identifier = "Unknown Entity"
    monitor_identifier += f" (Course: {config.get('course_name', 'N/A')})"

    logger.info(f"Attempting to add/update PRTG monitor for {monitor_identifier} using function '{add_monitor_func.__name__}'...")

    for attempt in range(1, max_retries + 1):
        prtg_url = None 
        monitor_success = False

        try:
            with mongo_client() as client:
                if not client:
                    logger.error(f"[Attempt {attempt}/{max_retries}] Cannot add monitor for {monitor_identifier}: DB connection failed.")
                else:
                    # --- Call the appropriate add_monitor function ---
                    # Check if the function expects prtg_server_preference
                    # Based on previous f5.add_monitor, it accepts it.
                    # Checkpoint and Palo also accept it. Others might use default.
                    if vendor_shortcode in ["cp", "pa", "f5"]:
                         prtg_url = add_monitor_func(config, client, prtg_server_preference)
                    else: # For av, pr, nu, ot (using PRTGManager.add_monitor by default)
                         # PRTGManager.add_monitor in monitor/prtg.py also needs adaptation if it should take preference
                         # For now, assuming the generic one doesn't.
                         prtg_url = add_monitor_func(config, client) 

            if prtg_url:
                logger.info(f"[Attempt {attempt}/{max_retries}] PRTG monitor added/updated for {monitor_identifier}. URL: {prtg_url}")
                monitor_success = True
                break 
            else:
                logger.warning(f"[Attempt {attempt}/{max_retries}] Failed to add/update PRTG monitor for {monitor_identifier} (check specific add_monitor logs).")

        except Exception as e:
            logger.error(f"[Attempt {attempt}/{max_retries}] Error during PRTG monitor action for {monitor_identifier}: {e}", exc_info=True)
        
        if not monitor_success and attempt < max_retries:
            logger.info(f"Waiting {retry_delay_seconds}s before retrying PRTG monitor action...")
            time.sleep(retry_delay_seconds)
    
    if not monitor_success:
        err_msg = f"Failed to add/update PRTG monitor for {monitor_identifier} after {max_retries} attempts."
        logger.error(err_msg)
        return False, None, err_msg

    # --- Update Data Accumulator (Database structure) ---
    host_value = args_dict.get('host', 'unknown_host') # From CLI args
    record = {"host": host_value, "poweron": "True", "prtg_url": prtg_url}
    if extra_details:
        record.update(extra_details)

    # --- F5 Specific DB Update Logic ---
    if vendor_shortcode == "f5":
        class_number = config.get("class_number")
        if class_number is None:
            logger.error("F5 configuration missing class_number during DB update. Cannot proceed.")
            return 

        # Check if this 'config' is for the class itself or for a pod within the class
        is_class_level_config = "pod_number" not in config 

        # Find or create the entry for this class number in the data accumulator
        class_entry = next((e for e in data["pod_details"] if isinstance(e, dict) and e.get("class_number") == class_number), None)
        if not class_entry:
            # Initialize class entry with common fields if it's new
            class_entry = {"class_number": class_number, "host": host_value, "vendor": vendor_shortcode, "pods": []}
            data["pod_details"].append(class_entry)

        if is_class_level_config:
            # This 'config' is for the F5 class itself. Update class-level details.
            class_entry.update(record) # Adds host, poweron (for class), prtg_url (for class VR)
            class_entry.setdefault("pods", []) # Ensure 'pods' list exists
            logger.debug(f"Updated/Created DB entry for F5 Class {class_number} with PRTG URL: {prtg_url}")
        else:
            # This 'config' is for a specific pod within the F5 class.
            pod_number = config.get("pod_number")
            if pod_number is None:
                 logger.error("F5 pod configuration missing pod_number during DB update. Cannot proceed.")
                 return

            record["pod_number"] = pod_number # Add pod number to the record for this pod
            
            # Find or create the entry for this specific pod within the class's 'pods' list
            pod_list_in_class_entry = class_entry.setdefault("pods", [])
            pod_entry_in_list = next((p for p in pod_list_in_class_entry if isinstance(p, dict) and p.get("pod_number") == pod_number), None)
            
            if pod_entry_in_list:
                pod_entry_in_list.update(record) # Update existing pod entry
            else:
                pod_list_in_class_entry.append(record) # Add new pod entry
            
            # Ensure the main class entry has a host if it wasn't set during a class-level build
            if "host" not in class_entry or not class_entry["host"]:
                 class_entry["host"] = host_value
            logger.debug(f"Updated/Created DB entry for F5 Pod {pod_number} in Class {class_number} with PRTG URL: {prtg_url}")
    else: # --- Non-F5 DB Update Logic ---
        pod_number = config.get("pod_number")
        if pod_number is None:
            logger.error(f"{vendor_shortcode.upper()} config missing pod_number during DB update. Cannot proceed.")
            return 
        record["pod_number"] = pod_number
        # Avoid duplicates for non-F5 - check if pod already exists in data["pod_details"]
        existing_pod_entry = next((p for p in data["pod_details"] if isinstance(p,dict) and p.get("pod_number") == pod_number), None)
        if existing_pod_entry:
            existing_pod_entry.update(record) # Update if exists
        else:
            data["pod_details"].append(record) # Add if new
        logger.debug(f"Updated/Created DB entry for {vendor_shortcode.upper()} Pod {pod_number} with PRTG URL: {prtg_url}")


    # --- Update Database (common for all) ---
    try:
        logger.info(f"Updating database allocation record for tag '{data.get('tag')}'...")
        update_database(data)
        logger.info(f"Database successfully updated for tag '{data.get('tag')}', course '{data.get('course_name')}'.")
        return True, prtg_url, None
    except Exception as e:
        db_err_msg = f"Failed during final database update: {e}"
        logger.error(db_err_msg, exc_info=True)
        return False, prtg_url, db_err_msg # Return failure on DB error


# --- Vendor Setup Orchestration ---
# Updated signature to accept args_dict
def vendor_setup(
    service_instance: VCenter,
    host_details: Dict[str, Any],
    args_dict: Dict[str, Any], # Changed from args: argparse.Namespace
    course_config: Dict[str, Any],
    selected_components: Optional[List[str]],
    operation_logger: OperationLogger
) -> List[Dict[str, Any]]:
    """Unified vendor setup routine."""
    vendor_shortcode = course_config.get("vendor_shortcode")
    if not vendor_shortcode:
        logger.error("Vendor shortcode missing in course config. Cannot proceed.")
        return [{"identifier": "config_error", "status": "failed", "error_message": "Missing vendor_shortcode"}]

    # Extract common args from dict
    tag = args_dict.get('tag', 'untagged')
    course_name = args_dict.get('course')
    rebuild = args_dict.get('re_build', False)
    full_clone = args_dict.get('full', False)
    thread_count = args_dict.get('thread', 4)
    start_pod = args_dict.get('start_pod') # Already int or None from commands.py
    end_pod = args_dict.get('end_pod')     # Already int or None
    class_number = args_dict.get('class_number') # Already int or None
    memory = args_dict.get('memory') # Already int or None
    clonefrom_pod_number = args_dict.get('clonefrom') 

    logger.info(f"Dispatching setup for vendor '{vendor_shortcode}'. RunID: {operation_logger.run_id}")
    # --- MODIFICATION: Add new optional fields to the data accumulator ---
    data_accumulator = {
        "tag": tag,
        "course_name": course_name,
        "vendor": vendor_shortcode,
        "pod_details": [],
        # New optional fields from args_dict
        "start_date": args_dict.get("start_date"),
        "end_date": args_dict.get("end_date"),
        "trainer_name": args_dict.get("trainer_name"),
        "apm_username": args_dict.get("username"), # Using consistent key names
        "apm_password": args_dict.get("password")
    }
    # --- END MODIFICATION ---
    all_results = []

    if vendor_shortcode == "f5": # F5 Synchronous Build
        if class_number is None:
            err_msg = "F5 setup requires --class_number."
            logger.error(err_msg)
            operation_logger.log_pod_status(pod_id=f"f5-validation", status="failed", step="missing_class_number", error=err_msg)
            return [{"identifier": f"f5-validation", "status": "failed", "error_message": err_msg}]

        class_id_str = f"class-{class_number}"
        class_success = False # Assume failure initially
        try:
            logger.info(f"Preparing F5 class {class_number} configuration...")
            class_config = fetch_and_prepare_course_config(course_name, f5_class=class_number)
            class_config.update({
                "host_fqdn": host_details["fqdn"], "class_number": class_number,
                "class_name": f"f5-class{class_number}", "vendor_shortcode": vendor_shortcode
            })

            logger.info(f"Building F5 class {class_number} infrastructure...")
            build_result = f5.build_class(
                service_instance, class_config, 
                rebuild=rebuild, full=full_clone, 
                selected_components=selected_components
            )
            class_success, class_step, class_error = build_result
            status_msg = "success" if class_success else "failed"
            operation_logger.log_pod_status(pod_id=class_id_str, status=status_msg, step=class_step, error=class_error, class_id=class_number)
            all_results.append({"identifier": class_id_str, "class_identifier": class_number, "status": status_msg, "failed_step": class_step, "error_message": class_error})

            if class_success:
                logger.info(f"F5 class {class_number} built successfully. Updating monitor/DB.")
                try:
                    # Pass args_dict here
                    monitor_ok, _, monitor_err = update_monitor_and_database(class_config, args_dict, data_accumulator, {"class_number": class_number})
                    if not monitor_ok:
                        logger.error(f"Monitor/DB update failed for Class {class_number}: {monitor_err}")
                        operation_logger.log_pod_status(pod_id=class_id_str, status="failed", step="update_monitor_db", error=monitor_err, class_id=class_number)
                        class_result_entry.update({"status": "failed", "failed_step": "update_monitor_db", "error_message": monitor_err})
                        # Set class_success to False to prevent pod builds if class monitor fails
                        class_success = False
                except Exception as e_upd:
                    logger.error(f"Error updating monitor/DB for Class {class_number}: {e_upd}", exc_info=True)
                    # Log failure for the class update step
                    operation_logger.log_pod_status(pod_id=class_id_str, status="failed", step="update_monitor_db", error=str(e_upd), class_id=class_number)
                    # Update the overall class result to indicate failure at this step
                    class_result_entry = next((r for r in all_results if r["identifier"] == class_id_str), None)
                    if class_result_entry:
                        class_result_entry["status"] = "failed"; class_result_entry["failed_step"] = "update_monitor_db"; class_result_entry["error_message"] = str(e_upd)
            else:
                logger.warning(f"F5 class {class_number} build failed at step '{class_step}'. Error: {class_error}")

        except Exception as e_class:
             logger.error(f"Critical error during F5 class {class_number} build: {e_class}", exc_info=True)
             status, step, error = "failed", "build_class_exception", str(e_class)
             operation_logger.log_pod_status(pod_id=class_id_str, status=status, step=step, error=error, class_id=class_number)
             all_results.append({"identifier": class_id_str, "class_identifier": class_number, "status": status, "failed_step": step, "error_message": error})
             class_success = False # Ensure build is marked as failed

        # Proceed with Pod builds only if Class build succeeded and Pod range is provided
        if class_success and start_pod is not None and end_pod is not None:
            logger.info(f"Proceeding with F5 pod builds for range {start_pod}-{end_pod} in class {class_number}...")
            for pod in range(start_pod, end_pod + 1):
                pod_id_str = str(pod)
                try:
                    logger.info(f"Building F5 pod {pod} in class {class_number}...")
                    pod_config_f5 = fetch_and_prepare_course_config(course_name, pod=pod, f5_class=class_number)
                    pod_config_f5.update({
                        "host_fqdn": host_details["fqdn"], "class_number": class_number,
                        "pod_number": pod, "vendor_shortcode": vendor_shortcode
                    })

                    # Call F5 pod build function
                    pod_result = f5.build_pod(service_instance, pod_config_f5, mem=memory, rebuild=rebuild, full=full_clone, selected_components=selected_components)
                    pod_success, pod_step, pod_error = pod_result
                    pod_status_msg = "success" if pod_success else "failed"
                    operation_logger.log_pod_status(pod_id=pod_id_str, status=pod_status_msg, step=pod_step, error=pod_error, class_id=class_number)
                    all_results.append({"identifier": pod_id_str, "class_identifier": class_number, "status": pod_status_msg, "failed_step": pod_step, "error_message": pod_error})

                    if pod_success:
                        logger.info(f"F5 pod {pod} built successfully. Updating monitor/DB.")
                        try:
                            # Pass args_dict here
                            monitor_ok, _, monitor_err = update_monitor_and_database(pod_config_f5, args_dict, data_accumulator, {"class_number": class_number})
                            if not monitor_ok:
                                logger.error(f"Monitor/DB update failed for F5 pod {pod}: {monitor_err}")
                                operation_logger.log_pod_status(pod_id=str(pod), status="failed", step="update_monitor_db", error=monitor_err, class_id=class_number)
                                pod_result_entry.update({"status": "failed", "failed_step": "update_monitor_db", "error_message": monitor_err})
                        except Exception as e_upd_pod:
                            logger.error(f"Error updating monitor/DB for F5 pod {pod}: {e_upd_pod}", exc_info=True)
                            operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="update_monitor_db", error=str(e_upd_pod), class_id=class_number)
                            # Update pod result entry
                            pod_result_entry = next((r for r in all_results if r["identifier"] == pod_id_str), None)
                            if pod_result_entry:
                                pod_result_entry["status"] = "failed"; pod_result_entry["failed_step"] = "update_monitor_db"; pod_result_entry["error_message"] = str(e_upd_pod)
                    else:
                        logger.warning(f"F5 pod {pod} build failed at step '{pod_step}'. Error: {pod_error}")

                except Exception as e_pod:
                    logger.error(f"Critical error during F5 pod {pod} build: {e_pod}", exc_info=True)
                    status, step, error = "failed", "build_pod_exception", str(e_pod)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error, class_id=class_number)
                    all_results.append({"identifier": pod_id_str, "class_identifier": class_number, "status": status, "failed_step": step, "error_message": error})
        elif class_success:
             logger.info(f"F5 class {class_number} built, but no pod range provided. Skipping pod builds.")
        else:
             logger.error(f"F5 class {class_number} build failed. Skipping pod builds.")

    else: # Other Vendors Async Build
        # Validate range for non-F5 vendors
        if start_pod is None or end_pod is None:
            err_msg = f"{vendor_shortcode.upper()} setup requires --start-pod and --end-pod."
            logger.error(err_msg); operation_logger.log_pod_status(pod_id=f"{vendor_shortcode}-validation", status="failed", error=err_msg); return [{"identifier": f"{vendor_shortcode}-validation", "status": "failed", "error_message": err_msg}]

        build_func = VENDOR_SETUP_MAP.get(vendor_shortcode)
        if not build_func:
            err_msg = f"Unsupported setup vendor code: '{vendor_shortcode}'"; logger.error(err_msg)
            operation_logger.log_pod_status(pod_id=f"vendor-{vendor_shortcode}", status="failed", step="unsupported_vendor", error=err_msg)
            return [{"identifier": f"vendor-{vendor_shortcode}", "status": "failed", "error_message": err_msg}]

        futures = []
        pod_configs_map = {} # Store prepared config per pod for later DB update

        source_pod_vms_map = None
        if vendor_shortcode == "cp" and clonefrom_pod_number is not None:
            logger.info(f"CloneFrom active for CP: Source Pod {clonefrom_pod_number}. Preparing source VM map...")
            try:
                # Fetch the config for the *source* pod to determine its VM names
                # Assumes the course_name is the same for the source and target.
                source_pod_config_raw = fetch_and_prepare_course_config(course_name, pod=clonefrom_pod_number)
                source_pod_vms_map = {}
                # This logic needs to correctly derive VM names based on how 'build_cp_pod' names them
                # For CP, it's typically component['clone_name'] with {X} replaced by pod_number
                for comp_template in source_pod_config_raw.get("components", []):
                    original_comp_name = comp_template.get("component_name")
                    # Use the 'clone_name' from the config as the pattern for the source VM.
                    clone_name_pattern = comp_template.get("clone_name")
                    if original_comp_name and clone_name_pattern:
                        source_vm_name = clone_name_pattern.replace("{X}", str(clonefrom_pod_number))
                        source_pod_vms_map[original_comp_name] = source_vm_name
                if not source_pod_vms_map:
                    logger.error(f"Could not determine source VM names for pod {clonefrom_pod_number} from its config. Aborting CloneFrom.")
                    # You might want to return an error result here
                    return [{"identifier": f"clonefrom_prep_failed_pod_{clonefrom_pod_number}", "status": "failed", "error_message": f"Could not map source VMs for pod {clonefrom_pod_number}"}]
                else:
                    logger.info(f"Source VM map for pod {clonefrom_pod_number}: {source_pod_vms_map}")
            except Exception as e_src_cfg:
                logger.error(f"Failed to prepare source pod VM map for pod {clonefrom_pod_number}: {e_src_cfg}. CloneFrom will likely fail.")
                source_pod_vms_map = None # Ensure it's None on error
                # Potentially return error here as well.

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for pod in range(start_pod, end_pod + 1):
                pod_id_str = str(pod)
                logger.debug(f"Preparing setup task for pod {pod}...")
                try:
                    pod_config_vendor = fetch_and_prepare_course_config(course_name, pod=pod)
                    pod_config_vendor.update({
                        "host_fqdn": host_details["fqdn"], "pod_number": pod,
                        "vendor_shortcode": vendor_shortcode,
                        "start_pod": start_pod,
                        "end_pod": end_pod
                    })
                    course_name_lower = pod_config_vendor.get("course_name", "").lower()
                    pod_configs_map[pod_id_str] = pod_config_vendor # Store config
                    current_build_func = build_func # Start with default build func

                    # --- Vendor Specific Build Function Selection ---
                    if vendor_shortcode == "pa": # Palo specific builds
                        if "cortex" in course_name_lower: current_build_func = palo.build_cortex_pod
                        elif "1100-210" in course_name_lower: current_build_func = palo.build_1100_210_pod
                        elif "1110" in course_name_lower: current_build_func = palo.build_1110_pod
                        elif "1100-220" in course_name_lower: current_build_func = palo.build_1100_220_pod
                        else: raise ValueError(f"Unsupported Palo Alto course name pattern: {course_name_lower}")
                    elif vendor_shortcode == "av": # Avaya specific builds
                        if "aura" in course_name_lower: current_build_func = avaya.build_aura_pod
                        elif "ipo" in course_name_lower: current_build_func = avaya.build_ipo_pod
                        elif "aep" in course_name_lower: current_build_func = avaya.build_aep_pod
                        else: raise ValueError(f"Unsupported Avaya course name pattern: {course_name_lower}")
                    # Add other vendor specific selections here

                    # --- Prepare Build Arguments ---
                    # Default args for most vendors
                    build_args = {
                        "service_instance": service_instance,
                        "pod_config": pod_config_vendor,
                        "rebuild": rebuild,
                        "full": full_clone,
                        "selected_components": selected_components
                    }
                    # Adjust args for specific vendors/courses if needed
                    if vendor_shortcode == "cp":
                        build_args["thread"] = thread_count # CP function takes thread count
                        build_args["clonefrom"] = clonefrom_pod_number
                        build_args["source_pod_vms"] = source_pod_vms_map
                    if vendor_shortcode == "pa" and ("1100" in course_name_lower or "cortex" in course_name_lower):
                        # 1100/Cortex build functions might need host_details directly
                        build_args["host_details"] = host_details
                    if vendor_shortcode == "av":
                         build_args.pop("full", None) # Avaya functions don't use 'full'
                         if "aep" in course_name_lower:
                              # Special handling for AEP Common vs Student
                              safe_selected = selected_components if isinstance(selected_components, list) else []
                              if "Student" not in safe_selected:
                                  # Build common components first (synchronously for dependency)
                                  common_config = pod_config_vendor.copy(); common_config["type"] = "common"
                                  logger.info(f"Building AEP common components for pod {pod}...")
                                  aep_res = avaya.build_aep_pod(service_instance, common_config, selected_components=selected_components)
                                  aep_succ, aep_step, aep_err = aep_res
                                  aep_id = f"{pod_id_str}-common"
                                  aep_status = "success" if aep_succ else "failed"
                                  operation_logger.log_pod_status(pod_id=aep_id, status=aep_status, step=aep_step, error=aep_err)
                                  all_results.append({"identifier": aep_id, "status": aep_status, "failed_step": aep_step, "error_message": aep_err})
                                  if not aep_succ:
                                      logger.error(f"AEP common build failed for pod {pod}, skipping student build.")
                                      continue # Skip student build if common failed
                              # Prepare args for student build (will be submitted async)
                              pod_config_vendor["type"] = "student"; build_args["pod_config"] = pod_config_vendor
                         # Ensure aura/ipo functions get the right args
                         elif "aura" in course_name_lower:
                              build_args = {"service_instance": service_instance, "pod_config": pod_config_vendor}
                              # Remove keys not needed by build_aura_pod
                              build_args.pop("rebuild", None); build_args.pop("selected_components", None)
                         elif "ipo" in course_name_lower:
                              # build_ipo_pod takes rebuild and selected_components
                              build_args = {"service_instance": service_instance, "pod_config": pod_config_vendor, "rebuild": rebuild, "selected_components": selected_components}

                    # --- Submit Task ---
                    logger.info(f"Submitting build task for pod {pod} using function {current_build_func.__name__}...")
                    future = executor.submit(current_build_func, **build_args)
                    future.pod_number = pod
                    future.class_number = None # Non-F5 doesn't use class number here
                    futures.append(future)

                except ValueError as e: # Catch config errors (e.g., unsupported course)
                    logger.error(f"Skipping pod {pod}: Configuration error: {e}")
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e))
                    all_results.append({"identifier": pod_id_str, "status": "failed", "failed_step": "config_error", "error_message": str(e)})
                except Exception as e: # Catch other errors during task prep/submission
                    logger.error(f"Error preparing/submitting setup task for pod {pod}: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e))
                    all_results.append({"identifier": pod_id_str, "status": "failed", "failed_step": "submit_task_error", "error_message": str(e)})

        # --- Wait for and Process Async Results ---
        task_results = wait_for_tasks(futures, description=f"{vendor_shortcode} pod builds")
        for res_data in task_results:
            pod_id = res_data["identifier"]
            class_id = res_data["class_identifier"] # Will be None for non-F5
            status = res_data["status"]
            step = res_data["failed_step"]
            error = res_data["error_message"]

            operation_logger.log_pod_status(pod_id=pod_id, status=status, step=step, error=error, class_id=class_id)

            if status == "success":
                pod_conf_for_update = pod_configs_map.get(pod_id)
                if pod_conf_for_update:
                    try:
                        logger.info(f"Pod {pod_id} build successful. Updating monitor/DB.")
                        # Capture the new return values from the helper
                        monitor_ok, _, monitor_err = update_monitor_and_database(pod_conf_for_update, args_dict, data_accumulator)
                        
                        # If the monitor/db update failed, override the pod's status
                        if not monitor_ok:
                            logger.error(f"Monitor/DB update failed for pod {pod_id}: {monitor_err}")
                            operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db", error=monitor_err)
                            # Update the result dictionary for this pod to reflect the failure
                            res_data["status"] = "failed"
                            res_data["failed_step"] = "update_monitor_db"
                            res_data["error_message"] = monitor_err
                        else:
                             # If successful, log the initial success status for the build step.
                             # The OperationLogger gets updated implicitly here by the loop's end.
                             operation_logger.log_pod_status(pod_id=res_data["identifier"], status=res_data["status"], step=res_data["failed_step"], error=res_data["error_message"])
                             
                    except Exception as upd_err:
                        # This block handles exceptions *during* the call, which is also important.
                        logger.error(f"Exception during monitor/DB update for pod {pod_id}: {upd_err}", exc_info=True)
                        operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db_exception", error=str(upd_err))
                        res_data["status"] = "failed"
                        res_data["failed_step"] = "update_monitor_db_exception"
                        res_data["error_message"] = str(upd_err)
                else:
                    # Config not found, this is an internal error
                    err_msg = "Internal: Config not found for update"
                    logger.warning(f"Configuration for successfully built pod {pod_id} not found. Cannot update monitor/DB.")
                    operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db", error=err_msg)
                    res_data["status"] = "failed"
                    res_data["failed_step"] = "update_monitor_db"
                    res_data["error_message"] = err_msg
            else:
                 # If build failed, just log that status from wait_for_tasks
                 operation_logger.log_pod_status(pod_id=res_data["identifier"], status=res_data["status"], step=res_data["failed_step"], error=res_data["error_message"])

        all_results.extend(task_results)
        logger.info(f"Finished asynchronous setup for pods {start_pod}-{end_pod}.")

    return all_results


# --- Vendor Teardown Orchestration ---
# Updated signature to accept args_dict
def vendor_teardown(
    service_instance: VCenter,
    host_details: Dict[str, Any],
    args_dict: Dict[str, Any], # Changed from args: argparse.Namespace
    course_config: Dict[str, Any],
    operation_logger: OperationLogger
) -> List[Dict[str, Any]]:
    """Unified vendor teardown routine."""
    vendor_shortcode = course_config.get("vendor_shortcode")
    if not vendor_shortcode:
        logger.error("Vendor shortcode missing in course config. Cannot proceed with teardown.")
        return [{"identifier": "config_error", "status": "failed", "error_message": "Missing vendor_shortcode"}]

    # Extract common args from dict
    tag = args_dict.get('tag', 'untagged')
    course_name_arg = args_dict.get('course')
    course_name = course_config.get("course_name", course_name_arg) # Use config name if available
    thread_count = args_dict.get('thread', 4)
    start_pod = args_dict.get('start_pod')
    end_pod = args_dict.get('end_pod')
    class_number = args_dict.get('class_number')

    logger.info(f"Dispatching teardown for vendor '{vendor_shortcode}'. RunID: {operation_logger.run_id}")
    all_results = []

    if vendor_shortcode == "f5":
        if class_number is None:
            err_msg = "F5 teardown requires --class_number."; logger.error(err_msg)
            operation_logger.log_pod_status(pod_id=f"f5-validation", status="failed", error=err_msg)
            return [{"identifier": f"f5-validation", "status": "failed", "error_message": err_msg}]

        # --- MODIFICATION START ---
        # Determine teardown mode based on whether a pod range is provided.
        if start_pod is not None and end_pod is not None:
            log_target = f"pods {start_pod}-{end_pod} in class {class_number}"
            # This will trigger the selective pod deletion logic in teardown_class
        else:
            log_target = f"entire class {class_number}"
        # --- MODIFICATION END ---
        
        class_id_str = f"class-{class_number}"
        logger.info(f"Starting teardown for F5 {log_target}...")
        td_success = True; td_error = None; td_step = None
        try:
            class_config = fetch_and_prepare_course_config(course_name_arg, f5_class=class_number)
            class_config.update({
                "host_fqdn": host_details["fqdn"],
                "class_number": class_number,
                "class_name": f"f5-class{class_number}"
            })

            # --- MODIFICATION START ---
            # Pass start_pod and end_pod to the teardown function.
            # They will be None if a full class teardown is intended.
            f5.teardown_class(service_instance, class_config, start_pod=start_pod, end_pod=end_pod)
            # --- MODIFICATION END ---

            # Monitor/DB cleanup only happens for full class teardown
            if start_pod is None and end_pod is None:
                try:
                    with mongo_client() as client:
                        if not client: logger.warning(f"Cannot delete PRTG monitor for F5 class {class_number}: DB connection failed.")
                        else:
                            prtg_url = get_prtg_url(tag, course_name, pod_number=None, class_number=class_number)
                            if prtg_url:
                                if PRTGManager.delete_monitor(prtg_url, client): logger.info(f"Deleted PRTG monitor for F5 class {class_number}.")
                                else: logger.warning(f"Failed to delete PRTG monitor for F5 class {class_number} (URL: {prtg_url}).")
                            else: logger.debug(f"No PRTG URL found for F5 class {class_number}, skipping monitor deletion.")
                    delete_from_database(tag, course_name=course_name, class_number=class_number)
                    logger.info(f"Database entry deleted for F5 class {class_number}.")
                except Exception as db_mon_err:
                     logger.error(f"Error deleting monitor/DB entry for F5 class {class_number}: {db_mon_err}", exc_info=True)
                     td_success = False; td_step = "delete_monitor_db"; td_error = str(db_mon_err)

        except Exception as e:
            td_success = False; td_error = str(e); td_step = "teardown_class_exception"
            logger.error(f"Error during F5 teardown for {log_target}: {e}", exc_info=True)

        final_status = "success" if td_success else "failed"
        operation_logger.log_pod_status(pod_id=class_id_str, status=final_status, step=td_step, error=td_error, class_id=class_number)
        all_results.append({"identifier": class_id_str, "class_identifier": class_number, "status": final_status, "failed_step": td_step, "error_message": td_error})

    else: # Other Vendors Async Teardown
        # Validate range for non-F5
        if start_pod is None or end_pod is None:
            err_msg = f"{vendor_shortcode.upper()} teardown requires --start-pod and --end-pod."
            logger.error(err_msg); operation_logger.log_pod_status(pod_id=f"{vendor_shortcode}-validation", status="failed", error=err_msg); return [{"identifier": f"{vendor_shortcode}-validation", "status": "failed", "error_message": err_msg}]

        teardown_func = VENDOR_TEARDOWN_MAP.get(vendor_shortcode)
        if not teardown_func:
            err_msg = f"Unsupported teardown vendor code: '{vendor_shortcode}'"; logger.error(err_msg)
            operation_logger.log_pod_status(pod_id=f"vendor-{vendor_shortcode}", status="failed", error=err_msg)
            return [{"identifier": f"vendor-{vendor_shortcode}", "status": "failed", "error_message": err_msg}]

        futures = []
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for pod in range(start_pod, end_pod + 1):
                pod_id_str = str(pod)
                try:
                    pod_config_vendor = fetch_and_prepare_course_config(course_name_arg, pod=pod)
                    pod_config_vendor.update({
                        "host_fqdn": host_details["fqdn"], # Pass context if needed by teardown func
                        "pod_number": pod
                    })
                    course_name_lower = pod_config_vendor.get("course_name", "").lower()
                    current_teardown_func = teardown_func # Default

                    # --- Vendor Specific Teardown Function Selection ---
                    if vendor_shortcode == "pa":
                        if "1110" in course_name_lower: current_teardown_func = palo.teardown_1110
                        elif "1100" in course_name_lower: current_teardown_func = palo.teardown_1100
                        elif "cortex" in course_name_lower: current_teardown_func = palo.teardown_cortex
                        else: raise ValueError(f"Unsupported Palo Alto course for teardown: {course_name_lower}")
                    elif vendor_shortcode == "av":
                         if "ipo" in course_name_lower: current_teardown_func = avaya.teardown_ipo
                         elif "aura" in course_name_lower: current_teardown_func = avaya.teardown_aura
                         # Add AEP teardown if needed
                         else: raise ValueError(f"Unsupported Avaya course for teardown: {course_name_lower}")
                    # Add other vendor specific selections here

                    # --- Delete Monitor/DB Entry (Before submitting async teardown) ---
                    try:
                        with mongo_client() as client:
                             if not client: logger.warning(f"Cannot delete PRTG monitor for pod {pod}: DB connection failed.")
                             else:
                                 prtg_url = get_prtg_url(tag, course_name, pod_number=pod)
                                 if prtg_url:
                                     if PRTGManager.delete_monitor(prtg_url, client): logger.info(f"Deleted PRTG monitor for pod {pod}.")
                                     else: logger.warning(f"Failed to delete PRTG monitor for pod {pod} (URL: {prtg_url}).")
                                 else: logger.debug(f"No PRTG URL found for pod {pod}, skipping monitor deletion.")
                        # Delete DB entry
                        delete_from_database(tag, course_name=course_name, pod_number=pod)
                        logger.info(f"Database entry deleted for pod {pod}.")
                    except Exception as db_mon_err:
                        # Log the error but proceed with submitting the VM teardown task
                        logger.error(f"Error deleting monitor/DB entry for pod {pod}: {db_mon_err}", exc_info=True)
                        # Log this failure specifically
                        operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="delete_monitor_db", error=str(db_mon_err))
                        # Add a failure result immediately for this step
                        all_results.append({"identifier": pod_id_str, "status": "failed", "failed_step": "delete_monitor_db", "error_message": str(db_mon_err)})

                    # --- Submit Async Teardown Task ---
                    logger.info(f"Submitting teardown task for pod {pod} using function {current_teardown_func.__name__}...")
                    future = executor.submit(current_teardown_func, service_instance, pod_config_vendor)
                    future.pod_number = pod
                    future.class_number = None # Non-F5
                    futures.append(future)

                except ValueError as e: # Catch config errors
                    logger.error(f"Skipping pod {pod} teardown: Configuration error: {e}")
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e))
                    all_results.append({"identifier": pod_id_str, "status": "failed", "failed_step": "config_error", "error_message": str(e)})
                except Exception as e: # Catch other errors during task prep/submission
                    logger.error(f"Error preparing/submitting teardown task for pod {pod}: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e))
                    all_results.append({"identifier": pod_id_str, "status": "failed", "failed_step": "submit_task_error", "error_message": str(e)})

        # --- Wait for and Process Async Results ---
        task_results = wait_for_tasks(futures, description=f"{vendor_shortcode} pod teardowns")
        for res_data in task_results:
            # Log the VM teardown result specifically
            operation_logger.log_pod_status(
                pod_id=res_data["identifier"],
                status=res_data["status"],
                step=res_data["failed_step"] or "vm_teardown", # Assign step if missing
                error=res_data["error_message"],
                class_id=res_data["class_identifier"]
            )
        all_results.extend(task_results) # Combine submission errors and task results
        logger.info(f"Finished asynchronous teardown for pods {start_pod}-{end_pod}.")

    return all_results

# --- END OF FILE orchestrator.py ---