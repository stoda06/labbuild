# orchestrator.py
"""Handles vendor-specific dispatch and common build/teardown orchestration steps."""

import logging
from typing import Optional, Dict, List, Any, Callable
from concurrent.futures import ThreadPoolExecutor
import argparse

# Import vendor modules
import labs.setup.avaya as avaya
import labs.setup.checkpoint as checkpoint
import labs.setup.f5 as f5
import labs.setup.palo as palo
import labs.setup.pr as pr
import labs.setup.nu as nu

# Import local utils and helpers
from managers.vcenter import VCenter
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
}
VENDOR_TEARDOWN_MAP: Dict[str, Callable] = {
    "cp": checkpoint.teardown_pod, "pa": palo.teardown_1110, "f5": f5.teardown_class,
    "av": avaya.teardown_aura, "pr": pr.teardown_pr_pod, "nu": nu.teardown_nu_pod,
}
VENDOR_MONITOR_MAP: Dict[str, Callable] = {
    "cp": checkpoint.add_monitor, "pa": palo.add_monitor, "f5": f5.add_monitor,
    "av": PRTGManager.add_monitor, "pr": PRTGManager.add_monitor,
    "nu": PRTGManager.add_monitor, "ot": PRTGManager.add_monitor,
}

# --- Monitor and DB Update Helper ---
def update_monitor_and_database(config: Dict[str, Any], args: argparse.Namespace, data: Dict[str, Any], extra_details: Optional[Dict[str, Any]] = None):
    """Add PRTG monitor and update database allocation."""
    vendor_shortcode = config.get("vendor_shortcode", "ot"); add_monitor_func = VENDOR_MONITOR_MAP.get(vendor_shortcode, PRTGManager.add_monitor); prtg_url = None
    try:
        with mongo_client() as client:
            if not client: logger.error("Cannot add monitor: DB connection failed.")
            else:
                if vendor_shortcode == "cp": prtg_url = add_monitor_func(config, client, getattr(args, 'prtg_server', None))
                else: prtg_url = add_monitor_func(config, client)
        if prtg_url: logger.debug(f"PRTG monitor added/updated, URL: {prtg_url}")
        else: logger.warning("Failed/skipped PRTG monitor add/update.")
    except Exception as e: logger.error(f"Error adding PRTG monitor: {e}", exc_info=True)
    record = {"host": args.host, "poweron": "True", "prtg_url": prtg_url};
    if extra_details: record.update(extra_details)
    if vendor_shortcode == "f5":
        class_number = config.get("class_number", "unknown"); is_class_build = "pod_number" not in config
        class_entry = next((e for e in data["pod_details"] if e.get("class_number") == class_number), None)
        if not class_entry: class_entry = {"class_number": class_number, "pods": []}; data["pod_details"].append(class_entry)
        if is_class_build: class_entry.update(record); class_entry.setdefault("pods", [])
        else:
            pod_number = config.get("pod_number", "unknown"); record["pod_number"] = pod_number
            pod_entry = next((p for p in class_entry.get("pods", []) if p.get("pod_number") == pod_number), None)
            if pod_entry: pod_entry.update(record)
            else: class_entry.setdefault("pods", []).append(record)
            if "host" not in class_entry: class_entry["host"] = args.host
    else: record_key = "pod_number" if "pod_number" in config else "class_number"; record[record_key] = config.get(record_key, "unknown"); data["pod_details"].append(record)
    try: logger.info(f"Updating database with pod details: {data.get('pod_details', [])}"); update_database(data) # Call imported function
    except Exception as e: logger.error(f"Failed final database update: {e}", exc_info=True)

# --- Vendor Setup Orchestration ---
def vendor_setup(service_instance: VCenter, host_details: Dict[str, Any], args: argparse.Namespace, course_config: Dict[str, Any], selected_components: Optional[List[str]], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """Unified vendor setup routine."""
    vendor_shortcode = course_config.get("vendor_shortcode"); logger.info(f"Dispatch setup vendor '{vendor_shortcode}'. RunID: {operation_logger.run_id}"); data_accumulator = {"tag": args.tag, "course_name": args.course, "vendor": vendor_shortcode, "pod_details": []}; all_results = []
    if vendor_shortcode == "f5": # F5 Synchronous Build
        if not args.class_number: logger.error("F5 needs --class_number."); operation_logger.log_pod_status(pod_id=f"class-{args.class_number}", status="failed", step="missing_class_number", error="--class_number required."); return [{"identifier": f"class-{args.class_number}", "status": "failed", "error_message": "--class_number required."}]
        class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number); class_config.update({"host_fqdn": host_details["fqdn"], "class_number": args.class_number, "class_name": f"f5-class{args.class_number}", "vendor_shortcode": vendor_shortcode})
        logger.info(f"Building F5 class {args.class_number}..."); class_result = f5.build_class(service_instance, class_config, rebuild=args.re_build, full=args.full, selected_components=selected_components); class_success, class_step, class_error = class_result; class_id_str = f"class-{args.class_number}"
        operation_logger.log_pod_status(pod_id=class_id_str, status="success" if class_success else "failed", step=class_step, error=class_error, class_id=args.class_number); all_results.append({"identifier": class_id_str, "class_identifier": args.class_number, "status": "success" if class_success else "failed", "failed_step": class_step, "error_message": class_error})
        if class_success:
             logger.info(f"Class {args.class_number} built. Update monitor/DB.");
             try: update_monitor_and_database(class_config, args, data_accumulator, {"class_number": args.class_number})
             except Exception as e: logger.error(f"Error update monitor/DB Class {args.class_number}: {e}", exc_info=True); operation_logger.log_pod_status(pod_id=class_id_str, status="failed", step="update_monitor_db", error=str(e), class_id=args.class_number)
        else: logger.warning(f"F5 class {args.class_number} build failed.")
        if class_success and args.start_pod is not None and args.end_pod is not None:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                logger.info(f"Building F5 pod {pod}..."); pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number); pod_config.update({"host_fqdn": host_details["fqdn"], "class_number": args.class_number, "pod_number": pod, "vendor_shortcode": vendor_shortcode}); pod_id_str = str(pod)
                try:
                    pod_result = f5.build_pod(service_instance, pod_config, mem=args.memory, rebuild=args.re_build, full=args.full, selected_components=selected_components); pod_success, pod_step, pod_error = pod_result
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="success" if pod_success else "failed", step=pod_step, error=pod_error, class_id=args.class_number); all_results.append({"identifier": pod_id_str, "class_identifier": args.class_number, "status": "success" if pod_success else "failed", "failed_step": pod_step, "error_message": pod_error})
                    if pod_success: logger.info(f"Pod {pod} built. Update monitor/DB."); 
                    try: update_monitor_and_database(pod_config, args, data_accumulator, {"class_number": args.class_number}); 
                    except Exception as e: logger.error(f"Error update monitor/DB pod {pod}: {e}", exc_info=True); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="update_monitor_db", error=str(e), class_id=args.class_number)
                except Exception as e: logger.error(f"F5 build pod {pod} failed: {e}", exc_info=True); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="build_pod_exception", error=str(e), class_id=args.class_number); all_results.append({"identifier": pod_id_str, "class_identifier": args.class_number, "status": "failed", "failed_step": "build_pod_exception", "error_message": str(e)})
    else: # Other Vendors Async Build
        build_func = VENDOR_SETUP_MAP.get(vendor_shortcode)
        if not build_func: logger.error(f"Unsupported setup vendor: {vendor_shortcode}"); operation_logger.log_pod_status(pod_id=f"vendor-{vendor_shortcode}", status="failed", step="unsupported_vendor", error=f"Vendor setup invalid."); return [{"identifier": f"vendor-{vendor_shortcode}", "status": "failed", "error_message": "Vendor invalid"}]
        futures = []; pod_configs_map = {}
        with ThreadPoolExecutor(max_workers=args.thread) as executor:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                logger.debug(f"Prep config pod {pod}..."); pod_id_str = str(pod)
                try:
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod); pod_config.update({"host_fqdn": host_details["fqdn"], "pod_number": pod, "vendor_shortcode": vendor_shortcode}); course_name_lower = pod_config.get("course_name", "").lower(); pod_configs_map[pod_id_str] = pod_config; current_build_func = build_func
                    if vendor_shortcode == "pa": # Palo specific builds
                        if "cortex" in course_name_lower: current_build_func = palo.build_cortex_pod
                        elif "1100-210" in course_name_lower: current_build_func = palo.build_1100_210_pod
                        elif "1110" in course_name_lower: current_build_func = palo.build_1110_pod
                        elif "1100-220" in course_name_lower: current_build_func = palo.build_1100_220_pod
                        else: logger.error(f"Invalid Palo course: {course_name_lower}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Invalid Palo course: {course_name_lower}"); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": f"Invalid Palo course"}); continue
                    elif vendor_shortcode == "av": # Avaya specific builds
                        if "aura" in course_name_lower: current_build_func = avaya.build_aura_pod
                        elif "ipo" in course_name_lower: current_build_func = avaya.build_ipo_pod
                        elif "aep" in course_name_lower: current_build_func = avaya.build_aep_pod
                        else: logger.error(f"Invalid Avaya course: {course_name_lower}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Invalid Avaya course: {course_name_lower}"); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": f"Invalid Avaya course"}); continue
                    build_args = {"service_instance": service_instance, "pod_config": pod_config, "rebuild": args.re_build, "full": args.full, "selected_components": selected_components}
                    if vendor_shortcode == "cp": build_args["thread"] = args.thread
                    if vendor_shortcode == "av":
                        if "aura" in course_name_lower: build_args = {"service_instance": service_instance, "pod_config": pod_config}
                        elif "aep" in course_name_lower:
                             safe_selected = selected_components if isinstance(selected_components, list) else []
                             if "Student" not in safe_selected:
                                 common_config = pod_config.copy(); common_config["type"] = "common"; logger.info(f"Building AEP common for pod {pod}...")
                                 aep_res = avaya.build_aep_pod(service_instance, common_config, selected_components=selected_components); aep_succ, aep_step, aep_err = aep_res
                                 operation_logger.log_pod_status(pod_id=f"{pod_id_str}-common", status="success" if aep_succ else "failed", step=aep_step, error=aep_err); all_results.append({"identifier": f"{pod_id_str}-common", "status": "success" if aep_succ else "failed", "error_message": aep_err})
                                 if not aep_succ: logger.error("AEP common build failed, skip student."); continue
                             pod_config["type"] = "student"; build_args = {"service_instance": service_instance, "pod_config": pod_config, "selected_components": selected_components}
                        build_args.pop("full", None)
                    logger.info(f"Submitting build task pod {pod}..."); future = executor.submit(current_build_func, **build_args); future.pod_number = pod; future.class_number = None; futures.append(future)
                except ValueError as e: logger.error(f"Skip pod {pod} config error: {e}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e)); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
                except Exception as e: logger.error(f"Error submit task pod {pod}: {e}", exc_info=True); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e)); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
        task_results = wait_for_tasks(futures, description=f"{vendor_shortcode} pod builds")
        for res_data in task_results:
            pod_id = res_data["identifier"]; class_id = res_data["class_identifier"]; status = res_data["status"]; step = res_data["failed_step"]; error = res_data["error_message"]
            operation_logger.log_pod_status(pod_id=pod_id, status=status, step=step, error=error, class_id=class_id)
            pod_conf_upd = pod_configs_map.get(pod_id)
            if status == "success":
                if pod_conf_upd:
                    try: update_monitor_and_database(pod_conf_upd, args, data_accumulator); logger.info(f"Pod {pod_id} success. Monitor/DB updated.")
                    except Exception as upd_err: logger.error(f"Error update monitor/DB pod {pod_id}: {upd_err}", exc_info=True); operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db", error=str(upd_err), class_id=class_id); res_data["status"] = "failed"; res_data["failed_step"] = "update_monitor_db"; res_data["error_message"] = str(upd_err)
                else: logger.warning(f"No config found success pod {pod_id} for update."); operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db", error="Config not found", class_id=class_id); res_data["status"] = "failed"; res_data["failed_step"] = "update_monitor_db"; res_data["error_message"] = "Config not found"
        all_results.extend(task_results); logger.info(f"Finished setup pods {args.start_pod}-{args.end_pod}.")
    return all_results

# --- Vendor Teardown Orchestration ---
def vendor_teardown(service_instance: VCenter, host_details: Dict[str, Any], args: argparse.Namespace, course_config: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]:
    """Unified vendor teardown routine."""
    vendor_shortcode = course_config.get("vendor_shortcode"); course_name = course_config.get("course_name", args.course); logger.info(f"Dispatch teardown vendor '{vendor_shortcode}'. RunID: {operation_logger.run_id}"); all_results = []
    if vendor_shortcode == "f5": # F5 Synchronous Teardown
        if not args.class_number: logger.error("F5 teardown needs --class_number."); operation_logger.log_pod_status(pod_id=f"class-{args.class_number}", status="failed", step="missing_class_number", error="--class_number needed."); return [{"identifier": f"class-{args.class_number}", "status": "failed", "error_message": "--class_number needed."}]
        class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number); class_config.update({"host_fqdn": host_details["fqdn"], "class_number": args.class_number, "class_name": f"f5-class{args.class_number}"})
        class_id_str = f"class-{args.class_number}"; logger.info(f"Tearing down F5 class {args.class_number}..."); td_success = True; td_error = None; td_step = None
        try:
            f5.teardown_class(service_instance, class_config); logger.info(f"F5 class {args.class_number} teardown initiated.")
            with mongo_client() as client:
                if not client: logger.warning(f"Skip PRTG delete F5 class {args.class_number}: DB connect fail.")
                else:
                    prtg_url = get_prtg_url(args.tag, course_name, pod_number=None, class_number=args.class_number)
                    if prtg_url:
                        if PRTGManager.delete_monitor(prtg_url, client): logger.info(f"Deleted PRTG monitor F5 class {args.class_number}.")
                        else: logger.warning(f"Failed PRTG monitor delete F5 class {args.class_number}.")
                    else: logger.warning(f"No PRTG URL found F5 class {args.class_number}.")
            delete_from_database(args.tag, course_name=course_name, class_number=args.class_number); logger.info(f"DB entry deleted F5 class {args.class_number}.")
        except Exception as e: td_success = False; td_error = str(e); td_step = "teardown_class_exception"; logger.error(f"Error F5 class teardown {args.class_number}: {e}", exc_info=True)
        operation_logger.log_pod_status(pod_id=class_id_str, status="success" if td_success else "failed", step=td_step, error=td_error, class_id=args.class_number); all_results.append({"identifier": class_id_str, "class_identifier": args.class_number, "status": "success" if td_success else "failed", "failed_step": td_step, "error_message": td_error})
    else: # Other Vendors Async Teardown
        teardown_func = VENDOR_TEARDOWN_MAP.get(vendor_shortcode)
        if not teardown_func: logger.error(f"Unsupported teardown vendor: {vendor_shortcode}"); operation_logger.log_pod_status(pod_id=f"vendor-{vendor_shortcode}", status="failed", step="unsupported_vendor", error=f"Vendor '{vendor_shortcode}' teardown invalid."); return [{"identifier": f"vendor-{vendor_shortcode}", "status": "failed", "error_message": "Vendor invalid"}]
        futures = []
        with ThreadPoolExecutor(max_workers=args.thread) as executor:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod)
                try:
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod); pod_config.update({"host_fqdn": host_details["fqdn"], "pod_number": pod}); course_name_lower = pod_config.get("course_name", "").lower(); current_teardown_func = teardown_func
                    if vendor_shortcode == "pa": # Palo specific teardown
                        if "1110" in course_name_lower: current_teardown_func = palo.teardown_1110
                        elif "1100" in course_name_lower: current_teardown_func = palo.teardown_1100
                        elif "cortex" in course_name_lower: current_teardown_func = palo.teardown_cortex
                        else: logger.error(f"Invalid Palo teardown: {course_name_lower}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Invalid Palo teardown: {course_name_lower}"); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": f"Invalid Palo teardown"}); continue
                    elif vendor_shortcode == "av": # Avaya specific teardown
                         if "ipo" in course_name_lower: current_teardown_func = avaya.teardown_ipo
                         elif "aura" in course_name_lower: current_teardown_func = avaya.teardown_aura
                         else: logger.error(f"Invalid Avaya teardown: {course_name_lower}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Invalid Avaya teardown: {course_name_lower}"); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": f"Invalid Avaya teardown"}); continue
                    logger.info(f"Submitting teardown pod {pod}..."); future = executor.submit(current_teardown_func, service_instance, pod_config); future.pod_number = pod; future.class_number = None
                    try: # Delete DB/Monitor immediately
                        with mongo_client() as client:
                             if not client: logger.warning(f"Skip PRTG delete pod {pod}: DB connect fail.")
                             else:
                                 prtg_url = get_prtg_url(args.tag, course_name, pod_number=pod)
                                 if prtg_url:
                                     if PRTGManager.delete_monitor(prtg_url, client): logger.info(f"Deleted PRTG monitor pod {pod}.")
                                     else: logger.warning(f"Failed PRTG delete pod {pod}.")
                                 else: logger.warning(f"No PRTG URL found pod {pod}.")
                        delete_from_database(args.tag, course_name=course_name, pod_number=pod); logger.info(f"DB entry deleted pod {pod}.")
                    except Exception as db_mon_err: logger.error(f"Error deleting monitor/DB pod {pod}: {db_mon_err}", exc_info=True)
                    futures.append(future)
                except ValueError as e: logger.error(f"Skip pod {pod} teardown config error: {e}"); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e)); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
                except Exception as e: logger.error(f"Error submit teardown task pod {pod}: {e}", exc_info=True); operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e)); all_results.append({"identifier": pod_id_str, "status": "failed", "error_message": str(e)})
        task_results = wait_for_tasks(futures, description=f"{vendor_shortcode} pod teardowns")
        for res_data in task_results: operation_logger.log_pod_status(pod_id=res_data["identifier"], status=res_data["status"], step=res_data["failed_step"], error=res_data["error_message"], class_id=res_data["class_identifier"])
        all_results.extend(task_results); logger.info(f"Finished teardown pods {args.start_pod}-{args.end_pod}.")
    return all_results