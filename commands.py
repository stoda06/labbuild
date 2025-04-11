# commands.py
"""Functions that handle the main logic for setup, manage, and teardown commands."""

import logging
import sys
import argparse
from typing import Optional, Dict, List, Any
# --- ADD IMPORTS ---
from concurrent.futures import ThreadPoolExecutor
from utils import wait_for_tasks # Assuming wait_for_tasks is in utils.py
# --- END IMPORTS ---


# Import local utilities and helpers
from config_utils import fetch_and_prepare_course_config, extract_components, get_host_by_name
from vcenter_utils import get_vcenter_instance
from orchestrator import vendor_setup, vendor_teardown, update_monitor_and_database
from operation_logger import OperationLogger
from db_utils import update_database, delete_from_database, get_prtg_url, mongo_client # Need DB utils
from monitor.prtg import PRTGManager # Need PRTGManager for monitor_only teardown

# Import specific vendor modules only if needed (e.g., for perm_only)
import labs.setup.checkpoint as checkpoint
import labs.manage.vm_operations as vm_operations

logger = logging.getLogger('labbuild.commands')

def setup_environment(args: argparse.Namespace, operation_logger: OperationLogger):
    """Handles the 'setup' command logic."""
    all_results = []
    course_config = None
    try:
        course_config = fetch_and_prepare_course_config(args.course)
    except Exception as e:
         logger.critical(f"Failed fetch course config '{args.course}': {e}")
         operation_logger.log_pod_status(pod_id="config_fetch", status="failed", step="fetch_initial_config", error=str(e))
         return [{"identifier": "config_fetch", "status": "failed", "error_message": str(e)}]

    if getattr(args, 'component', None) == "?":
        if not course_config: logger.error("Cannot list components: Config not loaded."); return [{"identifier": "component_list", "status": "failed", "error_message": "Config not loaded"}]
        comps = extract_components(course_config); print(f"\nComponents for '{args.course}':"); [print(f"  - {c}") for c in comps]; sys.exit(0)

    selected_components = None
    if args.component:
        selected_components = [c.strip() for c in args.component.split(",")]; available = extract_components(course_config); invalid = [c for c in selected_components if c not in available]
        if invalid: err_msg = f"Invalid components: {', '.join(invalid)}. Avail: {', '.join(available)}"; logger.error(err_msg); operation_logger.log_pod_status(pod_id="component_validation", status="failed", step="invalid_component", error=err_msg); return [{"identifier": "component_validation", "status": "failed", "error_message": err_msg}]

    if args.db_only:
        logger.info("DB-only mode: Updating database."); data = {"tag": args.tag, "course_name": args.course, "vendor": args.vendor, "pod_details": []}
        for pod_num in range(int(args.start_pod), int(args.end_pod) + 1): pd = {"pod_number": pod_num, "host": args.host, "poweron": "False", "prtg_url": None}; data["pod_details"].append(pd)
        update_database(data); logger.info("DB-only setup complete.")
        operation_logger.log_pod_status(pod_id="db_only", status="skipped", step="db_only_mode"); return [{"identifier": "db_only", "status": "skipped"}]

    host_details = None; service_instance = None; needs_vcenter = not args.db_only
    if needs_vcenter:
        host_details = get_host_by_name(args.host)
        if not host_details: err_msg = f"Host details missing '{args.host}'."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="host_fetch", status="failed", error=err_msg); return [{"identifier": "host_fetch", "status": "failed", "error_message": err_msg}]
        service_instance = get_vcenter_instance(host_details)
        if not service_instance: err_msg = f"vCenter connection failed '{args.host}'."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="vcenter_connect", status="failed", error=err_msg); return [{"identifier": "vcenter_connect", "status": "failed", "error_message": err_msg}]

    if args.perm:
        if args.vendor.lower() == 'cp':
            logger.info("Perm-only mode: Running CP permissions."); success_count, fail_count = 0, 0
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod); status = "success"; step, error = None, None
                try: p_config = fetch_and_prepare_course_config(args.course, pod=pod); p_config.update({"host_fqdn": host_details["fqdn"], "pod_number": pod}); logger.info(f"Running permissions pod {pod}."); checkpoint.perm_only_cp_pod(service_instance, p_config); success_count +=1
                except Exception as e: status, step, error = "failed", "perm_only_exception", str(e); fail_count += 1; logger.error(f"Error perm-only pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error); all_results.append({"identifier": pod_id_str, "status": status, "error_message": error})
            logger.info("Perm-only complete.")
        else: logger.error("Perm-only only for CheckPoint (cp)."); operation_logger.log_pod_status(pod_id="perm_only", status="failed", error="--perm only for cp"); all_results.append({"identifier": "perm_only", "status": "failed", "error_message": "--perm only for cp"})
        return all_results

    if args.monitor_only:
        logger.info("Monitor-only mode: Creating monitors."); data_acc = {"tag": args.tag, "course_name": args.course, "vendor": args.vendor, "pod_details": []}; success_count, fail_count = 0, 0
        vs_code = course_config.get("vendor_shortcode")
        if vs_code == "f5":
            if not args.class_number: logger.error("F5 monitor-only needs --class_number."); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error="--class_number required"); return [{"identifier": "monitor_only", "status": "failed", "error_message": "--class_number required"}]
            class_id_str = f"class-{args.class_number}"; status, step, error = "success", None, None
            try: cfg = fetch_and_prepare_course_config(args.course, f5_class=args.class_number); cfg.update({"host_fqdn": host_details["fqdn"], "class_number": args.class_number, "vendor_shortcode": vs_code}); update_monitor_and_database(cfg, args, data_acc, {"class_number": args.class_number}); success_count +=1
            except Exception as e: status, step, error = "failed", "monitor_only_class_exception", str(e); fail_count += 1; logger.error(f"Error monitor F5 class {args.class_number}: {e}", exc_info=True)
            operation_logger.log_pod_status(pod_id=class_id_str, status=status, step=step, error=error, class_id=args.class_number); all_results.append({"identifier": class_id_str, "status": status, "error_message": error})
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod); status, step, error = "success", None, None
                try: cfg = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number); cfg.update({"host_fqdn": host_details["fqdn"], "class_number": args.class_number, "pod_number": pod, "vendor_shortcode": vs_code}); update_monitor_and_database(cfg, args, data_acc, {"class_number": args.class_number}); success_count += 1
                except Exception as e: status, step, error = "failed", "monitor_only_pod_exception", str(e); fail_count += 1; logger.error(f"Error monitor F5 pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error, class_id=args.class_number); all_results.append({"identifier": pod_id_str, "status": status, "error_message": error})
        else: # Non-F5
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod); status, step, error = "success", None, None
                try: cfg = fetch_and_prepare_course_config(args.course, pod=pod); cfg.update({"host_fqdn": host_details["fqdn"], "pod_number": pod, "vendor_shortcode": vs_code}); update_monitor_and_database(cfg, args, data_acc); success_count += 1
                except Exception as e: status, step, error = "failed", "monitor_only_exception", str(e); fail_count += 1; logger.error(f"Error monitor pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error); all_results.append({"identifier": pod_id_str, "status": status, "error_message": error})
        logger.info("Monitor-only setup complete."); return all_results

    # --- Normal Build Process ---
    logger.info(f"Starting normal setup course '{args.course}' host '{args.host}' pods {args.start_pod}-{args.end_pod}")
    all_results = vendor_setup(service_instance, host_details, args, course_config, selected_components, operation_logger)
    logger.info(f"Setup process finished for course '{args.course}'.")
    return all_results

def teardown_environment(args: argparse.Namespace, operation_logger: OperationLogger):
    """Handles the 'teardown' command logic."""
    all_results = []; course_config = None
    try: course_config = fetch_and_prepare_course_config(args.course)
    except Exception as e: logger.critical(f"Failed fetch config teardown '{args.course}': {e}"); operation_logger.log_pod_status(pod_id="config_fetch", status="failed", error=str(e)); return [{"identifier": "config_fetch", "status": "failed", "error_message": str(e)}]
    vendor_shortcode = course_config.get("vendor_shortcode"); course_name = course_config.get("course_name", args.course)

    if args.db_only:
        logger.info("DB-only teardown: Deleting DB entries."); deleted_items = []
        if vendor_shortcode == "f5":
            if not args.class_number: err_msg = "F5 DB-only needs --class_number."; logger.error(err_msg); operation_logger.log_pod_status(pod_id="db_only", status="failed", error=err_msg); return [{"identifier": "db_only", "status": "failed", "error_message": err_msg}]
            delete_from_database(args.tag, course_name=course_name, class_number=args.class_number); deleted_items.append({"identifier": f"class-{args.class_number}", "status": "skipped"})
        else:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1): delete_from_database(args.tag, course_name=course_name, pod_number=pod); deleted_items.append({"identifier": str(pod), "status": "skipped"})
        logger.info("DB-only teardown complete."); operation_logger.log_pod_status(pod_id="db_only", status="skipped", step="db_only_mode"); return deleted_items

    if args.monitor_only:
        logger.info("Monitor-only teardown: Deleting monitors/DB."); success_count, fail_count = 0, 0
        try:
            with mongo_client() as client:
                if not client: err_msg = "DB connection failed mon-only teardown."; logger.error(err_msg); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error=err_msg); return [{"identifier": "monitor_only", "status": "failed", "error_message": err_msg}]
                if vendor_shortcode == "f5":
                    if not args.class_number: err_msg = "F5 mon-only teardown needs --class_number."; logger.error(err_msg); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error=err_msg); return [{"identifier": "monitor_only", "status": "failed", "error_message": err_msg}]
                    class_id_str = f"class-{args.class_number}"; status, step, error = "success", None, None
                    try:
                        prtg_url = get_prtg_url(args.tag, course_name, pod_number=None, class_number=args.class_number)
                        if prtg_url:
                            if not PRTGManager.delete_monitor(prtg_url, client): logger.warning(f"Failed delete PRTG F5 class {args.class_number}."); status="failed"; step="prtg_delete"; error="PRTG delete failed"
                        else: logger.warning(f"No PRTG URL F5 class {args.class_number}.")
                        delete_from_database(args.tag, course_name=course_name, class_number=args.class_number)
                        if status == "success": success_count += 1; 
                        else: fail_count += 1
                    except Exception as e: status, step, error = "failed", "mon_only_class_exception", str(e); fail_count += 1; logger.error(f"Error mon-only teardown class {args.class_number}: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=class_id_str, status=status, step=step, error=error, class_id=args.class_number); all_results.append({"identifier": class_id_str, "status": status, "error_message": error})
                else: # Non-F5
                    for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                        pod_id_str = str(pod); status, step, error = "success", None, None
                        try:
                            prtg_url = get_prtg_url(args.tag, course_name, pod_number=pod)
                            if prtg_url:
                                if not PRTGManager.delete_monitor(prtg_url, client): logger.warning(f"Failed delete PRTG pod {pod}."); status="failed"; step="prtg_delete"; error="PRTG delete failed"
                            else: logger.warning(f"No PRTG URL pod {pod}.")
                            delete_from_database(args.tag, course_name=course_name, pod_number=pod)
                            if status == "success": success_count += 1; 
                            else: fail_count += 1
                        except Exception as e: status, step, error = "failed", "mon_only_pod_exception", str(e); fail_count += 1; logger.error(f"Error mon-only teardown pod {pod}: {e}", exc_info=True)
                        operation_logger.log_pod_status(pod_id=pod_id_str, status=status, step=step, error=error); all_results.append({"identifier": pod_id_str, "status": status, "error_message": error})
            logger.info("Monitor-only teardown complete.")
        except Exception as e: logger.error(f"Error mon-only teardown: {e}", exc_info=True); operation_logger.log_pod_status(pod_id="monitor_only", status="failed", error=str(e)); all_results.append({"identifier": "monitor_only", "status": "failed", "error_message": str(e)})
        return all_results

    # --- Normal Teardown ---
    host_details = get_host_by_name(args.host)
    if not host_details: err_msg = f"Host details missing '{args.host}'."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="host_fetch", status="failed", error=err_msg); return [{"identifier": "host_fetch", "status": "failed", "error_message": err_msg}]
    service_instance = get_vcenter_instance(host_details)
    if not service_instance: err_msg = f"vCenter connection failed via '{args.host}'."; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="vcenter_connect", status="failed", error=err_msg); return [{"identifier": "vcenter_connect", "status": "failed", "error_message": err_msg}]
    logger.info(f"Starting normal teardown course '{args.course}' host '{args.host}' pods {args.start_pod}-{args.end_pod}")
    all_results = vendor_teardown(service_instance, host_details, args, course_config, operation_logger)
    logger.info(f"Teardown process finished for course '{args.course}'.")
    return all_results

def manage_environment(args: argparse.Namespace, operation_logger: OperationLogger):
    """Handles the 'manage' command logic."""
    all_results = []; host_details, service_instance, course_config = None, None, None
    try:
         host_details = get_host_by_name(args.host);
         if not host_details: raise ValueError(f"Host invalid '{args.host}'.")
         service_instance = get_vcenter_instance(host_details);
         if not service_instance: raise ConnectionError(f"vCenter connect failed '{args.host}'.")
         course_config = fetch_and_prepare_course_config(args.course)
    except Exception as e: err_msg = f"Prereq fail manage: {e}"; logger.critical(err_msg); operation_logger.log_pod_status(pod_id="prereq_check", status="failed", error=err_msg); return [{"identifier": "prereq_check", "status": "failed", "error_message": err_msg}]
    selected_components = None
    if args.component:
        if args.component == "?": comps = extract_components(course_config); print(f"\nComponents for '{args.course}':"); [print(f"  - {c}") for c in comps]; sys.exit(0)
        selected_components = [c.strip() for c in args.component.split(",")]; available = extract_components(course_config); invalid = [c for c in selected_components if c not in available]
        if invalid: err_msg = f"Invalid components: {', '.join(invalid)}. Avail: {', '.join(available)}"; logger.error(err_msg); operation_logger.log_pod_status(pod_id="component_validation", status="failed", error=err_msg); return [{"identifier": "component_validation", "status": "failed", "error_message": err_msg}]
    futures = []; logger.info(f"Dispatch VM op '{args.operation}' pods {args.start_pod}-{args.end_pod}. RunID: {operation_logger.run_id}")
    with ThreadPoolExecutor(max_workers=args.thread) as executor:
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            pod_id_str = str(pod); class_num_for_log = args.class_number if args.vendor.lower() == 'f5' else None
            try:
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=class_num_for_log); pod_config.update({"host_fqdn": host_details["fqdn"], "pod_number": pod})
                if class_num_for_log is not None: pod_config["class_number"] = class_num_for_log
                future = executor.submit(vm_operations.perform_vm_operations, service_instance, pod_config, args.operation, selected_components); future.pod_number = pod; future.class_number = class_num_for_log; futures.append(future)
            except ValueError as e: logger.error(f"Skip pod {pod} manage config error: {e}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e), class_id=class_num_for_log); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
            except Exception as e: logger.error(f"Error submit manage task pod {pod}: {e}", exc_info=True); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e), class_id=class_num_for_log); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
    task_results = wait_for_tasks(futures, description=f"VM management ({args.operation})")
    for res_data in task_results: operation_logger.log_pod_status(pod_id=res_data["identifier"], status=res_data["status"], step=res_data["failed_step"], error=res_data["error_message"], class_id=res_data["class_identifier"])
    all_results.extend(task_results); logger.info("VM management submitted."); return all_results