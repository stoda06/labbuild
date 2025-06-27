# --- START OF FILE commands.py ---

# commands.py
"""Functions that handle the main logic for setup, manage, and teardown commands."""

import logging
import sys
# argparse is no longer directly needed here, keep for type hints if desired
import argparse
import argparse
from typing import Optional, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor
from utils import wait_for_tasks
from labs.test import checkpoint as test_checkpoint 
from labs.test import checkpoint as test_checkpoint 

# Import local utilities and helpers
from config_utils import fetch_and_prepare_course_config, extract_components, get_host_by_name
from vcenter_utils import get_vcenter_instance
# Pass args_dict down to orchestrator functions
from orchestrator import vendor_setup, vendor_teardown, update_monitor_and_database
from operation_logger import OperationLogger
# Import the new function from db_utils
from db_utils import update_database, delete_from_database, get_prtg_url, mongo_client, get_test_params_by_tag
from monitor.prtg import PRTGManager

import labs.setup.checkpoint as checkpoint 
from labs.test import checkpoint as test_checkpoint 
import labs.manage.vm_operations as vm_operations

logger = logging.getLogger('labbuild.commands')

# --- Constants for return status ---
COMPONENT_LIST_STATUS = "component_list_displayed"

def test_environment(args_dict, operation_logger=None):
    """Runs a test suite for a lab, either by tag or by manual parameters."""
    # --- Check for tag and fetch parameters ---
    if args_dict.get('tag'):
        tag = args_dict.get('tag')
        logger.info(f"Test command invoked with tag: '{tag}'. Fetching parameters from database.")
        try:
            # Warn if conflicting arguments are provided, then ignore them.
            conflicting_args = ['vendor', 'start_pod', 'end_pod', 'host', 'group']
            if any(arg in args_dict for arg in conflicting_args if arg != 'tag'):
                logger.warning(f"Conflicting arguments provided with --tag. Using parameters from tag '{tag}'.")
                # Clean them up to be safe
                for arg in conflicting_args:
                    args_dict.pop(arg, None)
            
            test_params = get_test_params_by_tag(tag)
            args_dict.update(test_params)
            logger.info(f"Successfully fetched parameters for tag '{tag}': {test_params}")

        except (ValueError, ConnectionError) as e:
            err_msg = f"Failed to get test parameters for tag '{tag}': {e}"
            logger.error(err_msg)
            print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]
        except Exception as e:
            err_msg = f"An unexpected error occurred while fetching parameters for tag '{tag}': {e}"
            logger.error(err_msg, exc_info=True)
            print(f"Error: {err_msg}", file=sys.stderr)
            return [{"status": "failed", "error": err_msg}]

    # --- Validate that all necessary arguments are now present ---
    required_args = ['vendor', 'start_pod', 'end_pod', 'host', 'group']
    missing_args = [arg for arg in required_args if args_dict.get(arg) is None]
    
    # Special validation for F5
    if args_dict.get('vendor', '').lower() == 'f5':
        if args_dict.get('class_number') is None:
            missing_args.append('class_number')

    if missing_args:
        err_msg = f"Missing required arguments for test command: {', '.join(missing_args)}. Provide them manually or use a valid --tag."
        logger.error(err_msg)
        print(f"Error: {err_msg}", file=sys.stderr)
        return [{"status": "failed", "error": err_msg}]

    # --- Extract final arguments ---
    vendor = args_dict["vendor"]
    start = args_dict["start_pod"]
    end = args_dict["end_pod"]
    host = args_dict["host"]
    group = args_dict["group"]

    # --- Execute vendor-specific test script ---
    logger.info(f"Executing test for vendor '{vendor}' on host '{host}' for group '{group}' (Pods/Class: {start}-{end})")

    if vendor.lower() == "cp":
        from labs.test import checkpoint
        checkpoint_args = ["-s", str(start), "-e", str(end), "-H", host, "-g", group]
        checkpoint.main(checkpoint_args)
        return [{"status": "success", "pod": start}]

    elif vendor.lower() == "pa":
        from labs.test import palo
        palo_args = ["-s", str(start), "-e", str(end), "--host", host, "-g", group]
        palo.main(palo_args)
        return [{"status": "success", "pod": start}]

    elif vendor.lower() == "nu":
        from labs.test import nu
        nu_args = ["-s", str(start), "-e", str(end), "--host", host, "-g", group]
        nu.main(nu_args)
        return [{"status": "success", "pod": start}]

    elif vendor.lower() == "f5":
        classnum = args_dict.get("class_number")
        # Validation already happened, but this is a final safeguard.
        if classnum is None:
            raise ValueError("Internal Error: class_number is missing for F5 test.")
        from labs.test import f5
        f5_args = [
            "-s", str(start),
            "-e", str(end),
            "--host", host,
            "-g", group,
            "--classnum", str(classnum)
        ]
        f5.main(f5_args)
        return [{"status": "success", "pod": start}]

    else:
        err_msg = f"Vendor '{vendor}' is not yet supported for testing."
        print(err_msg)
        return [{"status": "failed", "error": err_msg}]


# --- Modified setup_environment ---
# (The rest of the file remains unchanged)
# ...
# --- Modified setup_environment ---
# Accepts args_dict instead of argparse.Namespace
def setup_environment(args_dict: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """Handles the 'setup' command logic, taking args as a dictionary."""
    # --- Extract arguments from dict, providing defaults and type conversion ---
    course_arg = args_dict.get('course')
    vendor_arg = args_dict.get('vendor')
    host_arg = args_dict.get('host')
    tag_arg = args_dict.get('tag', 'untagged') # Default already handled by argparse, but good practice here too
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
    tag_arg = args_dict.get('tag', 'untagged')
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

# --- END OF FILE commands.py ---