# dashboard/routes/allocation_actions.py

import logging
import json
import threading
from dashboard.utils import get_next_monday_date_str

from flask import Response
from datetime import datetime

from dashboard.trainer_report_generator import fetch_trainer_pod_data, create_trainer_report_in_memory
from ..upcoming_report_generator import get_upcoming_report_data, generate_excel_in_memory as generate_upcoming_report
from ..report_generator import get_full_report_data, generate_excel_in_memory
from flask import (
    Blueprint, request, redirect, url_for, flash, jsonify, Response
)
from pymongo.errors import PyMongoError
from collections import defaultdict
from typing import List, Dict, Any, Optional
from datetime import datetime
from ..extensions import alloc_collection, db
from ..utils import update_power_state_in_db, get_next_monday_date_str
from ..tasks import run_labbuild_task
from ..trainer_report_generator import fetch_trainer_pod_data, create_trainer_report_in_memory
from ..report_generator import get_full_report_data, generate_excel_in_memory
from db_utils import delete_from_database

bp = Blueprint('allocation_actions', __name__, url_prefix='/allocations')
logger = logging.getLogger('dashboard.routes.allocation_actions')

def _dispatch_bulk_labbuild_tasks(
        tasks_to_run: List[Dict[str, Any]], # Each dict: {'args': list, 'description': str, 'db_update_info': tuple (optional)}
        action_name: str,
        operation_if_power_toggle: Optional[str] = None):
    """
    Helper to run a list of labbuild tasks sequentially in a background thread.
    Optionally updates power state in DB after each power toggle task.
    """
    if not tasks_to_run:
        logger.info(f"No tasks to dispatch for bulk {action_name}.")
        return

    def worker():
        total_tasks = len(tasks_to_run)
        logger.info(f"Starting bulk {action_name} worker for {total_tasks} tasks.")
        success_count = 0
        fail_count = 0

        for i, task_info in enumerate(tasks_to_run):
            args = task_info['args']
            description = task_info['description']
            db_update_info = task_info.get('db_update_info') # For power toggle

            logger.info(f"Executing task {i+1}/{total_tasks} for bulk {action_name}: {description} - CMD: {' '.join(args)}")
            try:
                # run_labbuild_task itself handles subprocess.run and logging
                # We assume it's synchronous for this sequential helper,
                # or we'd need a more complex Future-based approach here too.
                # For now, assuming run_labbuild_task blocks until completion.
                run_labbuild_task(args) # This runs the labbuild.py script
                
                # If it's a power toggle and task appeared successful (no exception from run_labbuild_task),
                # update the DB. A more robust way would be for run_labbuild_task to return success/failure.
                if action_name == "power_toggle" and db_update_info and operation_if_power_toggle:
                    tag, course, host, item_num, item_type, class_num_ctx = db_update_info
                    new_power_state_bool = (operation_if_power_toggle == 'start')
                    update_power_state_in_db(tag, course, host, item_num, item_type, new_power_state_bool, class_num_ctx)
                
                success_count += 1
            except Exception as e:
                logger.error(f"Error in bulk {action_name} task {description}: {e}", exc_info=True)
                fail_count += 1
            # Add a small delay between tasks if desired, e.g., time.sleep(1)
        
        logger.info(f"Bulk {action_name} worker finished. Success: {success_count}, Failed: {fail_count}.")
        # Flash messages from a background thread is tricky.
        # Usually, you'd use SSE or another mechanism to update UI.
        # For now, main route will flash a general submission message.

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    flash(f"Submitted {len(tasks_to_run)} tasks for bulk {action_name}. Check logs for progress.", "info")

def _find_all_matching_rules(rules: list, vendor: str, code: str, type_str: str) -> list[dict]:
    """
    Finds all build rules matching the course criteria.
    Assumes input 'rules' list is already sorted by priority (ascending).
    """
    matching_rules_list = []
    # Normalize inputs for case-insensitive matching
    cc_lower = code.lower() if code else ""
    ct_lower = type_str.lower() if type_str else ""
    v_lower = vendor.lower() if vendor else ""

    for rule in rules: 
        conditions = rule.get("conditions", {})
        
        match = True # Assume match until a condition fails

        # 1. Vendor check (must match if specified in rule)
        rule_vendor_cond = conditions.get("vendor")
        if rule_vendor_cond and rule_vendor_cond.lower() != v_lower:
            match = False
        
        # 2. Course Code Contains check (only if match is still true)
        if match and "course_code_contains" in conditions:
            terms = conditions["course_code_contains"]
            terms = [terms] if not isinstance(terms, list) else terms # Ensure list
            if not any(str(t).lower() in cc_lower for t in terms):
                match = False
        
        # 3. Course Code NOT Contains check (only if match is still true)
        if match and "course_code_not_contains" in conditions:
            terms = conditions["course_code_not_contains"]
            terms = [terms] if not isinstance(terms, list) else terms # Ensure list
            if any(str(t).lower() in cc_lower for t in terms):
                match = False

        # 4. Course Type Contains check (only if match is still true)
        if match and "course_type_contains" in conditions:
            terms = conditions["course_type_contains"]
            terms = [terms] if not isinstance(terms, list) else terms # Ensure list
            if not any(str(t).lower() in ct_lower for t in terms):
                match = False
        
        if match: # If all specific conditions passed (or no specific conditions to fail), add the rule
            matching_rules_list.append(rule)
            
    return matching_rules_list

def _get_memory_for_course_local(course_name: str, local_course_configs_map: Dict[str, Any]) -> float:
    if not course_name: return 0.0
    config = local_course_configs_map.get(course_name)
    if config:
        mem_gb_str = config.get('memory_gb_per_pod', config.get('memory', '0'))
        try:
            mem_gb = float(str(mem_gb_str).strip() or '0')
            return mem_gb if mem_gb > 0 else 0.0
        except (ValueError, TypeError):
            logger.warning(f"Could not parse memory '{mem_gb_str}' for course '{course_name}'.")
            return 0.0
    return 0.0

def _format_date_for_review(raw_date_str: Optional[str], context: str) -> str:
    # ... (Implementation from previous responses) ...
    if not raw_date_str or raw_date_str == "N/A": return "N/A"
    try: datetime.strptime(raw_date_str, "%Y-%m-%d"); return raw_date_str
    except ValueError:
        for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"):
            try: return datetime.datetime.strptime(raw_date_str, fmt).strftime("%Y-%m-%d")
            except ValueError: continue
        logger.warning(f"Could not parse date '{raw_date_str}' for {context}, keeping original.")
        return raw_date_str

@bp.route('/toggle-power', methods=['POST'])
def toggle_power():
    """Handles power toggle actions from the allocations page."""
    # Logic moved from original app.py
    scope = request.form.get('scope'); tag = request.form.get('tag')
    item_type = request.form.get('item_type'); item_number_str = request.form.get('item_number')
    host = request.form.get('host'); vendor = request.form.get('vendor'); course = request.form.get('course')
    item_class_number_str = request.form.get('item_class_number')

    if not scope or not tag: flash("Missing scope or tag for power toggle.", "danger"); return redirect(url_for('main.view_allocations', **request.args))
    logger.info(f"Power toggle request: Scope='{scope}', Tag='{tag}'")
    tasks_to_run = [] # List of tuples: (args_list, description, db_update_info)

    try:
        if db is None or alloc_collection is None: raise ConnectionError("Database unavailable.")
        if scope == 'tag':
            tag_doc = alloc_collection.find_one({"tag": tag});
            if not tag_doc: raise ValueError(f"Tag '{tag}' not found.")
            courses_in_tag = tag_doc.get("courses", [])
            if not isinstance(courses_in_tag, list): courses_in_tag = []
            for course_item in courses_in_tag:
                if not isinstance(course_item, dict): continue
                c_name, c_vendor, pod_details = course_item.get("course_name"), course_item.get("vendor"), course_item.get("pod_details", [])
                if not c_name or not c_vendor or not isinstance(pod_details, list): continue
                for pd in pod_details:
                    if not isinstance(pd, dict): continue
                    pd_host, pd_pod, pd_class = pd.get("host", pd.get("pod_host")), pd.get("pod_number"), pd.get("class_number")
                    current_power_on = str(pd.get("poweron", False)).lower() == 'true'
                    action = 'stop' if current_power_on else 'start'; new_power_state_bool = not current_power_on
                    if not pd_host: continue
                    is_f5 = c_vendor.lower() == 'f5'; item_detail = None
                    if is_f5 and pd_class is not None:
                        args = ['manage', '-v', c_vendor, '-g', c_name, '--host', pd_host, '-t', tag, '-o', action, '-cn', str(pd_class)]
                        desc = f"Tag '{tag}' - Manage {action} Class {pd_class}"; item_detail = (c_name, pd_host, pd_class, "f5_class", new_power_state_bool)
                    elif pd_pod is not None:
                        args = ['manage', '-v', c_vendor, '-g', c_name, '--host', pd_host, '-t', tag, '-o', action, '-s', str(pd_pod), '-e', str(pd_pod)]
                        if is_f5 and pd_class is not None: args.extend(['-cn', str(pd_class)])
                        desc = f"Tag '{tag}' - Manage {action} Pod {pd_pod}"; item_detail = (c_name, pd_host, pd_pod, "pod", new_power_state_bool)
                    if item_detail: tasks_to_run.append((args, desc, item_detail))
        elif scope == 'item':
            if not all([item_type, item_number_str, host, vendor, course]): raise ValueError("Missing item details.")
            item_number = int(item_number_str)
            current_state_is_on = request.form.get('current_state') == 'on'
            action = 'stop' if current_state_is_on else 'start'; new_power_state_bool = not current_state_is_on
            args = ['manage', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-o', action]
            item_detail = None; item_class_num = None
            if item_type == 'f5_class':
                args.extend(['-cn', str(item_number)]); desc = f"Item - Manage {action} Class {item_number}"
                item_detail = (course, host, item_number, "f5_class", new_power_state_bool)
            elif item_type == 'pod':
                args.extend(['-s', str(item_number), '-e', str(item_number)])
                if vendor.lower() == 'f5' and item_class_number_str: item_class_num = int(item_class_number_str); args.extend(['-cn', item_class_number_str])
                desc = f"Item - Manage {action} Pod {item_number}"; item_detail = (course, host, item_number, "pod", new_power_state_bool)
                if item_class_num is not None: item_detail = (course, host, item_number, "pod", new_power_state_bool, item_class_num)
            else: raise ValueError(f"Invalid item_type: {item_type}")
            if item_detail: tasks_to_run.append((args, desc, item_detail))
        else: raise ValueError(f"Invalid scope: {scope}")

        # --- Execute Tasks and Update DB ---
        if not tasks_to_run: flash(f"No valid manage tasks for Scope '{scope}', Tag '{tag}'.", "warning")
        else:
            logger.info(f"Submitting {len(tasks_to_run)} tasks for Scope '{scope}', Tag '{tag}'...")
            def run_sequential_manage_and_update(tasks):
                for i, (args, desc, db_info) in enumerate(tasks):
                    logger.info(f"Starting task {i+1}/{len(tasks)}: {desc}")
                    run_labbuild_task(args); logger.info(f"Finished task {i+1}/{len(tasks)}: {desc}")
                    if db_info:
                        try: update_power_state_in_db(tag, *db_info)
                        except Exception as db_err: logger.error(f"Failed DB update for {desc}: {db_err}")
                logger.info(f"All tasks submitted for Scope '{scope}', Tag '{tag}'.")
            thread = threading.Thread(target=run_sequential_manage_and_update, args=(tasks_to_run,), daemon=True)
            thread.start()
            flash(f"Submitted {len(tasks_to_run)} tasks for Scope '{scope}', Tag '{tag}'.", "info")

    except (ConnectionError, ValueError) as e: logger.error(f"Error during toggle for {tag}: {e}"); flash(f"Error: {e}", "danger")
    except Exception as e: logger.error(f"Unexpected error toggle for {tag}: {e}", exc_info=True); flash(f"Error submitting toggle: {e}", 'danger')

    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('main.view_allocations', **query_params)) # Redirect to main blueprint


@bp.route('/teardown-item', methods=['POST'])
def teardown_item():
    """
    Handles teardown/delete actions.
    MODIFIED: Returns JSON for AJAX calls and redirects for non-AJAX.
    """
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    try:
        tag = request.form.get('tag'); host = request.form.get('host'); vendor = request.form.get('vendor'); course = request.form.get('course')
        pod_num_str = request.form.get('pod_number'); class_num_str = request.form.get('class_number'); delete_level = request.form.get('delete_level')
        if not delete_level or not tag:
            msg = "Missing delete level or tag."
            if is_ajax: return jsonify({"success": False, "message": msg}), 400
            flash(msg, "danger"); return redirect(url_for('main.view_allocations'))
            
        pod_num = int(pod_num_str) if pod_num_str else None; class_num = int(class_num_str) if class_num_str else None

        # --- Handle DB-Only Deletion ---
        if delete_level.endswith('_db'):
            logger.info(f"AJAX DB delete request: Level='{delete_level}', Tag='{tag}'")
            success, item_desc = False, "Unknown"
            try:
                if delete_level == 'tag_db': success = delete_from_database(tag=tag); item_desc = f"Tag '{tag}'"
                elif delete_level == 'course_db': success = delete_from_database(tag=tag, course_name=course); item_desc = f"Course '{course}'"
                elif delete_level == 'class_db': success = delete_from_database(tag=tag, course_name=course, class_number=class_num); item_desc = f"Class {class_num}"
                elif delete_level == 'pod_db': success = delete_from_database(tag=tag, course_name=course, pod_number=pod_num, class_number=class_num); item_desc = f"Pod {pod_num}"
                else:
                     if is_ajax: return jsonify({"success": False, "message": "Invalid DB delete level."}), 400
                     flash("Invalid DB delete level.", "warning"); return redirect(url_for('main.view_allocations'))
                
                message = f"Removed DB entry for {item_desc}." if success else f"Failed DB removal for {item_desc}."
                flash(message, 'success' if success else 'danger')
                return jsonify({"success": success, "message": message, "action": "remove"})
            except Exception as e_db: 
                message = f"Error during DB delete: {e_db}"
                flash(message, 'danger'); logger.error(f"DB delete error: {e_db}", exc_info=True)
                return jsonify({"success": False, "message": message}), 500
        
        # --- Handle Full Infrastructure Teardown ---
        elif delete_level in ['class', 'pod']:
            if not all([vendor, course, host]):
                msg = "Vendor, Course, Host required for infrastructure teardown."
                if is_ajax: return jsonify({"success": False, "message": msg}), 400
                flash(msg, "danger"); return redirect(url_for('main.view_allocations'))

            args_list, item_desc = [], ""
            if delete_level == 'class' and vendor.lower() == 'f5' and class_num is not None: args_list = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-cn', str(class_num)]; item_desc = f"F5 Class {class_num}"
            elif delete_level == 'pod' and pod_num is not None:
                args_list = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]
                item_desc = f"Pod {pod_num}"
                if vendor.lower() == 'f5' and class_num is not None: args_list.extend(['-cn', str(class_num)]); item_desc += f" (Class {class_num})"
            
            if args_list:
                thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True); thread.start()
                message = f"Submitted infrastructure teardown for {item_desc}. The item will be removed from the list upon completion."
                flash(message, 'info')
                return jsonify({"success": True, "message": message, "action": "remove"})
            else:
                msg = "Failed to build teardown command."
                if is_ajax: return jsonify({"success": False, "message": msg}), 400
                flash(msg, "danger")
        else:
            msg = f"Unsupported teardown level: '{delete_level}'."
            if is_ajax: return jsonify({"success": False, "message": msg}), 400
            flash(msg, "warning")

    except Exception as e:
        logger.error(f"Error processing teardown/delete request: {e}", exc_info=True)
        message = f"Error processing request: {e}"
        if is_ajax: return jsonify({"success": False, "message": message}), 500
        flash(message, 'danger')
    
    # Fallback redirect for non-AJAX or unexpected errors
    query_params = {k: v for k, v in request.form.items() if k.startswith('filter_')}
    return redirect(url_for('main.view_allocations', **query_params))


@bp.route('/teardown-tag', methods=['POST'])
def teardown_tag():
    """
    Handles teardown of an entire tag.
    MODIFIED: Now returns a JSON response for AJAX calls.
    """
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    tag = request.form.get('tag')
    
    if not tag:
        msg = "Tag identifier is missing."
        if is_ajax: return jsonify({"success": False, "message": msg}), 400
        flash(msg, "danger"); return redirect(url_for('main.view_allocations'))
    
    logger.info(f"Initiating FULL teardown for Tag: '{tag}'")
    tasks_to_run = []

    try:
        if db is None or alloc_collection is None: raise ConnectionError("DB unavailable.")
        tag_doc = alloc_collection.find_one({"tag": tag})
        if not tag_doc:
            msg = f"Tag '{tag}' not found."
            if is_ajax: return jsonify({"success": False, "message": msg}), 404
            flash(msg, "warning"); return redirect(url_for('main.view_allocations'))

        courses_in_tag = tag_doc.get("courses", [])
        if not isinstance(courses_in_tag, list): courses_in_tag = []

        for course in courses_in_tag:
            if not isinstance(course, dict): continue
            course_name, vendor, pod_details = course.get("course_name"), course.get("vendor"), course.get("pod_details", [])
            if not course_name or not vendor or not isinstance(pod_details, list): continue
            
            items_by_host = defaultdict(lambda: {"pods": set(), "classes": set()})
            for pd in pod_details:
                if not isinstance(pd, dict): continue
                host, pod_num, class_num = pd.get("host", pd.get("pod_host")), pd.get("pod_number"), pd.get("class_number")
                if not host: continue
                is_f5 = vendor.lower() == 'f5'
                if is_f5 and class_num is not None: items_by_host[host]["classes"].add(class_num)
                elif pod_num is not None: items_by_host[host]["pods"].add(pod_num)

            for host, items in items_by_host.items():
                for class_num in sorted(list(items["classes"])):
                    args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-cn', str(class_num)]
                    tasks_to_run.append({'args': args, 'description': f"Tag '{tag}', Class '{class_num}'"})
                if items["pods"]:
                    for pod_num in sorted(list(items["pods"])):
                        args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]
                        tasks_to_run.append({'args': args, 'description': f"Tag '{tag}', Pod '{pod_num}'"})

        if not tasks_to_run:
            message = f"No teardown tasks found for Tag '{tag}'."
            if is_ajax: return jsonify({"success": True, "message": message}) # Success, but nothing to do
            flash(message, "warning"); return redirect(url_for('main.view_allocations'))
            
        logger.info(f"Submitting {len(tasks_to_run)} tasks for Tag '{tag}' sequentially...")
        
        def run_sequential_tasks(tasks):
            for i, task_info in enumerate(tasks):
                args = task_info['args']
                desc = task_info['description']
                logger.info(f"Starting task {i+1}/{len(tasks)}: {desc}")
                run_labbuild_task(args)
                logger.info(f"Finished task {i+1}/{len(tasks)}: {desc}")
            logger.info(f"All teardown tasks submitted for Tag '{tag}'.")
            
        thread = threading.Thread(target=run_sequential_tasks, args=(tasks_to_run,), daemon=True)
        thread.start()
        
        message = f"Submitted {len(tasks_to_run)} teardown tasks for Tag '{tag}'. The group will be removed upon completion."
        flash(message, "info")
        return jsonify({"success": True, "message": message, "action": "remove"})
        
    except (ConnectionError, Exception) as e:
        logger.error(f"Error processing teardown Tag '{tag}': {e}", exc_info=True)
        message = f"Error submitting teardown for Tag '{tag}': {e}"
        if is_ajax: return jsonify({"success": False, "message": message}), 500
        flash(message, 'danger')
        return redirect(url_for('main.view_allocations'))


@bp.route('/update-summary', methods=['POST'])
def update_allocation_summary():
    """
    API endpoint to handle in-line edits of an allocation group's summary.
    """
    if not request.is_json:
        return jsonify({"success": False, "error": "Invalid request format, JSON expected."}), 415

    data = request.json
    logger.info(f"Received allocation summary update request: {data}")

    # --- 1. Extract and Validate Identifiers ---
    tag = data.get('tag')
    # The group is identified by its first course name for the update query
    course_name = data.get('course_name')

    if not tag or not course_name:
        return jsonify({"success": False, "error": "Missing tag or course_name identifier."}), 400

    # --- 2. Build the MongoDB Update Payload ---
    update_payload = {}
    # Create a mapping from the keys received from JS to the DB field names
    field_map = {
        'start_date': 'courses.$.start_date',
        'end_date': 'courses.$.end_date',
        'trainer_name': 'courses.$.trainer_name',
        'apm_username': 'courses.$.apm_username',
        'apm_password': 'courses.$.apm_password'
    }

    for key, db_field in field_map.items():
        if key in data: # Check if the key exists in the request
            update_payload[db_field] = data[key]

    if not update_payload:
        return jsonify({"success": True, "message": "No changes detected."})

    # --- 3. Perform the Database Update ---
    if alloc_collection is None:
        return jsonify({"success": False, "error": "Database service unavailable."}), 503

    try:
        result = alloc_collection.update_one(
            {"tag": tag, "courses.course_name": course_name},
            {"$set": update_payload}
        )

        if result.matched_count == 0:
            logger.warning(f"No allocation found for tag '{tag}' and course '{course_name}' to update.")
            return jsonify({"success": False, "error": "Allocation not found. It may have been recently deleted."}), 404
        
        if result.modified_count == 0:
            logger.info(f"Allocation for tag '{tag}' matched but no fields were changed.")
            return jsonify({"success": True, "message": "No changes made."})

        logger.info(f"Successfully updated allocation summary for tag '{tag}', course '{course_name}'.")
        return jsonify({"success": True, "message": "Allocation updated successfully."})

    except PyMongoError as e:
        logger.error(f"DB error updating allocation summary for tag '{tag}': {e}", exc_info=True)
        return jsonify({"success": False, "error": f"Database error: {e}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error updating allocation summary for tag '{tag}': {e}", exc_info=True)
        return jsonify({"success": False, "error": "An unexpected server error occurred."}), 500


@bp.route('/toggle-tag-extend', methods=['POST'])
def toggle_tag_extend():
    """
    Toggles the 'extend' field for a given tag.
    MODIFIED: Now returns a JSON response for AJAX calls.
    """
    tag_name = request.form.get('tag_name')
    current_status_str = request.form.get('current_extend_status', 'false')
    logger.info(f"AJAX toggle request for tag: '{tag_name}', current status: '{current_status_str}'")

    if not tag_name:
        return jsonify({"success": False, "message": "Tag name missing."}), 400
    if alloc_collection is None:
        logger.error("alloc_collection is None in toggle_tag_extend.")
        return jsonify({"success": False, "message": "Database service unavailable."}), 503

    try:
        current_status_bool = current_status_str.lower() == 'true'
        new_status_bool = not current_status_bool
        new_status_str = "true" if new_status_bool else "false"
    except Exception as e:
        logger.error(f"Error determining new status for tag '{tag_name}': {e}")
        return jsonify({"success": False, "message": "Internal error processing status."}), 500

    logger.info(f"Attempting to update tag '{tag_name}' extend status to: '{new_status_str}'")
    try:
        result = alloc_collection.update_one(
            {"tag": tag_name},
            {"$set": {"extend": new_status_str}}
        )
        if result.matched_count == 0:
            return jsonify({"success": False, "message": f"Tag '{tag_name}' not found."}), 404
        
        new_status_display = "NO REUSE (Locked)" if new_status_str == "true" else "ALLOW REUSE (Unlocked)"
        flash(f"Tag '{tag_name}' updated. Pod reuse policy set to: {new_status_display}.", "success")
        return jsonify({
            "success": True, 
            "message": f"Tag '{tag_name}' updated.",
            "new_status": new_status_str,
            "new_title": "Toggle Pod Reuse (Currently Extended)" if new_status_bool else "Toggle Pod Reuse (Not Extended)"
        })

    except PyMongoError as e:
        logger.error(f"Database error toggling extend status for tag '{tag_name}': {e}", exc_info=True)
        return jsonify({"success": False, "message": "Database error updating tag status."}), 500
    except Exception as e:
        logger.error(f"Unexpected error toggling extend status for tag '{tag_name}': {e}", exc_info=True)
        return jsonify({"success": False, "message": "An unexpected server error occurred."}), 500


@bp.route('/bulk-toggle-power', methods=['POST'])
def bulk_toggle_power():
    selected_items_json = request.form.get('selected_items_json')
    operation = request.args.get('operation') # 'start' or 'stop'

    if not selected_items_json or not operation or operation not in ['start', 'stop']:
        flash("Invalid bulk power toggle request.", "danger")
        return redirect(url_for('main.view_allocations', **request.form)) # Pass back filters

    try:
        selected_items = json.loads(selected_items_json)
        if not isinstance(selected_items, list):
            raise ValueError("Selected items data is not a list.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Error processing selected items: {e}", "danger")
        return redirect(url_for('main.view_allocations', **request.form))

    tasks_to_run = []
    for item in selected_items:
        # Validate item structure (basic)
        if not all(k in item for k in ['vendor', 'course', 'host', 'item_type', 'item_number', 'tag']):
            logger.warning(f"Skipping invalid item in bulk power toggle: {item}")
            continue

        args = [
            'manage',
            '-v', item['vendor'],
            '-g', item['course'],
            '--host', item['host'],
            '-t', item['tag'],
            '-o', operation
        ]
        item_num_str = str(item['item_number'])
        if item['item_type'] == 'f5_class':
            args.extend(['-cn', item_num_str])
        elif item['item_type'] == 'pod':
            args.extend(['-s', item_num_str, '-e', item_num_str])
            if item.get('vendor', '').lower() == 'f5' and item.get('class_number'):
                args.extend(['-cn', str(item['class_number'])])
        else:
            logger.warning(f"Unknown item_type '{item['item_type']}' for bulk power toggle. Skipping item: {item.get('tag')}/{item.get('course')}/{item_num_str}")
            continue
        
        description = f"Power {operation} for {item['item_type']} {item_num_str} (Tag: {item['tag']}, Course: {item['course']})"
        db_update_info = (
            item['tag'], item['course'], item['host'], item['item_number'],
            item['item_type'], item.get('class_number') 
        )
        tasks_to_run.append({'args': args, 'description': description, 'db_update_info': db_update_info})

    if tasks_to_run:
        _dispatch_bulk_labbuild_tasks(tasks_to_run, "power_toggle", operation_if_power_toggle=operation)
    else:
        flash("No valid items found to process for bulk power toggle.", "info")
    
    # Preserve filters from the form submission for redirection
    preserved_filters = {k: v for k, v in request.form.items() if k.startswith('filter_') or k in ['page', 'per_page']}
    return redirect(url_for('main.view_allocations', **preserved_filters))


@bp.route('/bulk-teardown-items', methods=['POST'])
def bulk_teardown_items():
    selected_items_json = request.form.get('selected_items_json')
    if not selected_items_json:
        flash("No items selected for bulk teardown.", "warning")
        return redirect(url_for('main.view_allocations', **request.form))

    try:
        selected_items = json.loads(selected_items_json)
        if not isinstance(selected_items, list): raise ValueError
    except:
        flash("Error processing selected items for teardown.", "danger")
        return redirect(url_for('main.view_allocations', **request.form))

    # --- Grouping and Range Merging Logic ---
    tasks_to_run = []
    # Group items by a tuple key: (vendor, course, host, tag, class_number_for_f5)
    grouped_items = defaultdict(lambda: {'pods': set(), 'f5_classes': set()})

    for item in selected_items:
        try:
            if not all(k in item for k in ['vendor', 'course', 'host', 'item_type', 'item_number', 'tag']):
                logger.warning(f"Skipping invalid item in bulk teardown: {item}")
                continue
            
            # **FIX**: Ensure item_number is converted to an integer immediately.
            item_number_int = int(item['item_number'])

            class_context = int(item.get('class_number')) if item.get('vendor', '').lower() == 'f5' and item.get('class_number') else None
            group_key = (item['vendor'], item['course'], item['host'], item['tag'], class_context)

            if item['item_type'] == 'f5_class':
                grouped_items[group_key]['f5_classes'].add(item_number_int)
            elif item['item_type'] == 'pod':
                grouped_items[group_key]['pods'].add(item_number_int)
        except (ValueError, TypeError) as e:
             logger.warning(f"Skipping item due to data conversion error: {e}. Item: {item}")
             continue
    
    logger.info(f"Bulk Teardown: Grouped selected items into {len(grouped_items)} potential jobs.")

    # Process each group to create labbuild commands
    for group_key, items in grouped_items.items():
        vendor, course, host, tag, class_num = group_key

        # 1. Process F5 Classes (each is a separate job)
        for f5_class_to_tear in sorted(list(items['f5_classes'])):
            args = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-cn', str(f5_class_to_tear)]
            desc = f"Teardown F5 Class {f5_class_to_tear} (Tag: {tag})"
            tasks_to_run.append({'args': args, 'description': desc})

        # 2. Process Pods (merge contiguous ranges)
        if items['pods']:
            sorted_pods = sorted(list(items['pods']))
            
            if not sorted_pods: continue

            # **FIX**: Ensure start_range and end_range are always integers.
            start_range = sorted_pods[0]
            end_range = sorted_pods[0]

            for i in range(1, len(sorted_pods)):
                # Now this comparison is safe (int vs int)
                if sorted_pods[i] != end_range + 1:
                    # Create job for the completed range
                    args = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-s', str(start_range), '-e', str(end_range)]
                    if class_num is not None: args.extend(['-cn', str(class_num)])
                    desc = f"Teardown Pods {start_range}-{end_range} (Tag: {tag})"
                    tasks_to_run.append({'args': args, 'description': desc})
                    
                    # Start a new range (with an integer)
                    start_range = sorted_pods[i]
                    end_range = sorted_pods[i]
                else:
                    # Extend the current range
                    end_range = sorted_pods[i]
            
            # Add the last range after the loop finishes
            args = ['teardown', '-v', vendor, '-g', course, '--host', host, '-t', tag, '-s', str(start_range), '-e', str(end_range)]
            if class_num is not None: args.extend(['-cn', str(class_num)])
            desc = f"Teardown Pods {start_range}-{end_range} (Tag: {tag})"
            tasks_to_run.append({'args': args, 'description': desc})

    # --- Dispatch the generated tasks ---
    if tasks_to_run:
        _dispatch_bulk_labbuild_tasks(tasks_to_run, "infrastructure_teardown")
    else:
        flash("No valid items found to process for bulk infrastructure teardown.", "info")

    preserved_filters = {k: v for k, v in request.form.items() if k.startswith('filter_') or k in ['page', 'per_page']}
    return redirect(url_for('main.view_allocations', **preserved_filters))


@bp.route('/bulk-teardown-tags', methods=['POST'])
def bulk_teardown_tags():
    """
    Handles bulk teardown requests for multiple tags from the notifications page.
    """
    tags_to_teardown = request.form.getlist('tags_to_teardown')
    if not tags_to_teardown:
        flash("No tags were selected for teardown.", "warning")
        return redirect(url_for('main.all_notifications'))

    logger.info(f"Initiating BULK teardown for {len(tags_to_teardown)} tags: {', '.join(tags_to_teardown)}")
    
    tasks_to_run = []
    
    if alloc_collection is None:
        flash("Database unavailable. Cannot process teardown.", "danger")
        return redirect(url_for('main.all_notifications'))
        
    try:
        # Fetch all details for the selected tags in a single query
        tag_docs_cursor = alloc_collection.find({"tag": {"$in": tags_to_teardown}})
        
        for tag_doc in tag_docs_cursor:
            tag = tag_doc.get("tag")
            for course in tag_doc.get("courses", []):
                course_name = course.get("course_name")
                vendor = course.get("vendor")
                if not all([course_name, vendor]): continue

                # Group pod details by host to create efficient teardown commands
                items_by_host = defaultdict(lambda: {"pods": set(), "classes": set()})
                for pd in course.get("pod_details", []):
                    host = pd.get("host")
                    if not host: continue
                    if vendor.lower() == 'f5' and pd.get("class_number") is not None:
                        items_by_host[host]["classes"].add(pd.get("class_number"))
                    elif pd.get("pod_number") is not None:
                        items_by_host[host]["pods"].add(pd.get("pod_number"))
                
                # Create teardown tasks
                for host, items in items_by_host.items():
                    for class_num in sorted(list(items["classes"])):
                        args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-cn', str(class_num)]
                        tasks_to_run.append({'args': args, 'description': f"Tag '{tag}', Class {class_num}"})
                    if items["pods"]:
                        # This could be further optimized to merge ranges, but individual calls are safer
                        for pod_num in sorted(list(items["pods"])):
                            args = ['teardown', '-v', vendor, '-g', course_name, '--host', host, '-t', tag, '-s', str(pod_num), '-e', str(pod_num)]
                            tasks_to_run.append({'args': args, 'description': f"Tag '{tag}', Pod {pod_num}"})

    except Exception as e:
        logger.error(f"Error preparing bulk teardown tasks: {e}", exc_info=True)
        flash("An error occurred while preparing the teardown jobs.", "danger")
        return redirect(url_for('main.all_notifications'))

    if tasks_to_run:
        _dispatch_bulk_labbuild_tasks(tasks_to_run, "bulk tag teardown")
    else:
        flash("No valid items found within the selected tags to tear down.", "info")

    return redirect(url_for('main.all_notifications'))


@bp.route('/bulk-db-delete-items', methods=['POST'])
def bulk_db_delete_items():
    selected_items_json = request.form.get('selected_items_json')
    if not selected_items_json:
        flash("No items selected for bulk DB deletion.", "warning")
        return redirect(url_for('main.view_allocations', **request.form))

    try:
        selected_items = json.loads(selected_items_json)
        if not isinstance(selected_items, list): raise ValueError
    except:
        flash("Error processing selected items for DB deletion.", "danger")
        return redirect(url_for('main.view_allocations', **request.form))

    deleted_count = 0
    failed_count = 0
    processed_tags_courses = set() # To avoid redundant messages for same course in a tag

    for item in selected_items:
        try:
            tag = item.get('tag')
            course_name = item.get('course')
            item_type = item.get('item_type')
            # item_number holds pod_number for type 'pod' or class_number for type 'f5_class'
            item_number = int(item['item_number']) if item.get('item_number') else None
            
            # For F5 pods, class_number context is in item['class_number']
            # For F5 class itself, item_number is class_number and item['class_number'] is also class_number
            class_number_context = int(item['class_number']) if item.get('class_number') else None

            pod_to_delete = None
            class_to_delete = None

            if item_type == 'f5_class':
                class_to_delete = item_number
            elif item_type == 'pod':
                pod_to_delete = item_number
                if item.get('vendor', '').lower() == 'f5': # F5 pod needs class context
                    class_to_delete = class_number_context 
                    if class_to_delete is None: # Should not happen if data is consistent
                        logger.warning(f"F5 pod {pod_to_delete} missing class context for DB delete. Item: {item}")
                        # This might delete all pods with this number if class_to_delete is None in delete_from_database
                        # For safety, skip if F5 pod and class context is missing from the selected item data
                        failed_count +=1
                        continue
            
            if not tag or not course_name:
                logger.warning(f"Skipping item for DB delete due to missing tag/course: {item}")
                failed_count += 1
                continue

            # Call delete_from_database for each distinct item
            # delete_from_database handles if pod_number or class_number is None
            if delete_from_database(tag, course_name=course_name, pod_number=pod_to_delete, class_number=class_to_delete):
                deleted_count += 1
                log_msg_key = (tag, course_name, pod_to_delete, class_to_delete)
                if log_msg_key not in processed_tags_courses:
                    logger.info(f"DB entry deleted for: Tag {tag}, Course {course_name}, Pod {pod_to_delete}, Class {class_to_delete}")
                    processed_tags_courses.add(log_msg_key)
            else:
                failed_count += 1
                logger.error(f"Failed DB delete for: Tag {tag}, Course {course_name}, Pod {pod_to_delete}, Class {class_to_delete}")

        except Exception as e:
            failed_count += 1
            logger.error(f"Error during bulk DB delete for item {item}: {e}", exc_info=True)

    if deleted_count > 0:
        flash(f"Successfully deleted {deleted_count} DB entries.", "success")
    if failed_count > 0:
        flash(f"Failed to delete {failed_count} DB entries. Check logs.", "danger")
    if deleted_count == 0 and failed_count == 0:
        flash("No valid items found to process for DB deletion.", "info")
        
    preserved_filters = {k: v for k, v in request.form.items() if k.startswith('filter_') or k in ['page', 'per_page']}
    return redirect(url_for('main.view_allocations', **preserved_filters))


@bp.route('/export-current-lab-report')
def export_current_lab_report():
    """
    Generates and serves the full, comprehensive labbuild excel report.
    This replaces the old dummy function.
    """
    logger.info("Request received for 'Current Lab Report', generating full report.")

    try:
        # 1. Fetch and process all the data using our new helper
        course_allocs, trainer_pods, extended_pods, host_map = get_full_report_data(db)

        # 2. Generate the Excel file in memory using our new helper
        excel_stream = generate_excel_in_memory(course_allocs, trainer_pods, extended_pods, host_map)

        # 3. Prepare the filename
        next_monday_str = get_next_monday_date_str("%Y%m%d")
        filename = f"Lab Build - {next_monday_str}.xlsx"

        # 4. Create and return a Flask Response to trigger the download
        return Response(
            excel_stream,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment;filename={filename}'
            }
        )
    except Exception as e:
        logger.error(f"Failed to generate full lab report: {e}", exc_info=True)
        flash(f"Could not generate the report. Error: {e}", 'danger')
        # Redirect back to the page where the button was
        return redirect(url_for('main.view_allocations'))


# In allocation_actions.py

@bp.route('/export-trainer-pod-allocation')
def export_trainer_pod_allocation():
    """
    Generates and serves the structured trainer pod allocation report.
    """
    logger.info("Request received for 'Trainer Pod Allocation' report.")
    try:
        # 1. Fetch and process the specific trainer pod data
        trainer_pods_data = fetch_trainer_pod_data(db)

        # 2. Generate the Excel file in memory
        if not trainer_pods_data:
            flash("No trainer pod data was found to generate the report.", "warning")
            return redirect(url_for('main.view_allocations'))

        excel_stream = create_trainer_report_in_memory(trainer_pods_data)

        # 3. Prepare the filename
        next_monday_str = get_next_monday_date_str("%Y%m%d")
        filename = f"Trainer Pod Allocation - {next_monday_str}.xlsx"

        # 4. Serve the file
        return Response(
            excel_stream,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment;filename={filename}'}
        )
    except Exception as e:
        logger.error(f"Failed to generate Trainer Pod Allocation report: {e}", exc_info=True)
        flash(f"Could not generate the trainer pod report. Error: {e}", 'danger')
        return redirect(url_for('main.view_allocations')) # Adjust redirect as needed

@bp.route('/test-tag', methods=['POST'])
def test_tag():
    """
    Handles a 'test' action for a given tag from the allocations page.
    """
    tag = request.form.get('tag')
    if not tag:
        flash("Tag identifier is missing for the test action.", "danger")
        return redirect(url_for('main.view_allocations'))

    logger.info(f"Received 'test' request for tag: {tag}")
    
    # Construct the argument list for the labbuild.py script
    args_list = ['test', '-t', tag]

    try:
        # Run the command in a background thread so the UI doesn't block
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True)
        thread.start()
        
        flash(f"Submitted 'test' command for tag '{tag}'. Check logs for progress.", 'info')
        logger.info(f"Dispatched background task for 'test -t {tag}'")

    except Exception as e:
        logger.error(f"Failed to start thread for 'test' command on tag '{tag}': {e}", exc_info=True)
        flash("Error starting the test task. Check server logs.", 'danger')

    # Preserve any existing filters when redirecting
    query_params = {k: v for k, v in request.args.items() if k.startswith('filter_')}
    return redirect(url_for('main.view_allocations', **query_params))


@bp.route('/export-upcoming-lab-report')
def export_upcoming_lab_report():
    """
    Generates and serves the Upcoming Lab Report, which uses data from
    the interim allocation collection.
    """
    logger.info("Request received for 'Upcoming Lab Report'.")

    try:
        # 1. Fetch data specifically for the upcoming report using its dedicated helper
        course_allocs, trainer_pods, extended_pods, host_map = get_upcoming_report_data(db)

        # 2. Generate the Excel file in memory using the aliased generator function
        excel_stream = generate_upcoming_report(course_allocs, trainer_pods, extended_pods, host_map)

        # 3. Prepare the filename
        next_monday_str = get_next_monday_date_str("%Y%m%d")
        filename = f"Lab Build - {next_monday_str}.xlsx"

        # 4. Create and return a Flask Response to trigger the download
        return Response(
            excel_stream,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment;filename={filename}'
            }
        )
    except Exception as e:
        logger.error(f"Failed to generate upcoming lab report: {e}", exc_info=True)
        flash(f"Could not generate the upcoming report. Error: {e}", 'danger')
        # Redirect back to the page where the button was clicked
        return redirect(url_for('main.view_allocations'))