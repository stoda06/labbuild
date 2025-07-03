import logging
import json
import threading
import datetime
import pytz
import re
from flask import (
    Blueprint, request, redirect, url_for, flash, render_template, jsonify
)
from pymongo import ASCENDING, DESCENDING, UpdateOne
from pymongo.errors import PyMongoError, BulkWriteError
from collections import defaultdict
from typing import List, Dict, Optional, Tuple, Any, Union
import os
import string
import random
import requests
import pymongo
from apscheduler.triggers.date import DateTrigger
from ..extensions import (
    scheduler, db, interim_alloc_collection, 
    host_collection, course_config_collection, 
    build_rules_collection, alloc_collection, 
    locations_collection
)
from ..tasks import run_labbuild_task
from ..utils import get_hosts_available_memory_parallel
# Note: You might need to adjust the import path depending on your project structure.
# If these helpers are also moved, they should be imported from their new location.
from .allocation_actions import _find_all_matching_rules, _get_memory_for_course_local, _format_date_for_review
from .apm_actions import _generate_apm_data_for_plan as generate_apm_helper
from constants import SUBSEQUENT_POD_MEMORY_FACTOR
from bson import ObjectId
from bson.errors import InvalidId

bp = Blueprint('build_planner_actions', __name__, url_prefix='/build-planner')
logger = logging.getLogger('dashboard.routes.build_planner')


@bp.route('/build-row', methods=['POST'])
def build_row():
    """Handles 'Build' action from the upcoming courses page."""
    try:
        data = request.json
        if not data: return jsonify({"status": "error", "message": "No data received."}), 400

        labbuild_course = data.get('labbuild_course')
        start_pod = data.get('start_pod')
        end_pod = data.get('end_pod')
        host = data.get('host')
        vendor = data.get('vendor')
        sf_course_code = data.get('sf_course_code')

        if not all([labbuild_course, start_pod, end_pod, host, vendor]):
            return jsonify({"status": "error", "message": "Missing required fields."}), 400
        
        try:
            s_pod, e_pod = int(start_pod), int(end_pod)
            if not (s_pod >= 0 and e_pod >= s_pod):
                raise ValueError("Invalid pod range")
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "Invalid Pod numbers."}), 400
        
        tag = f"uc_{sf_course_code.replace('/', '_')}"[:50] if sf_course_code else "uc_dashboard"
        
        args_list = [
            'setup', '-v', vendor, '-g', labbuild_course, '--host', host,
            '-s', str(s_pod), '-e', str(e_pod), '-t', tag
        ]
        
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True)
        thread.start()
        
        # Flash is useful for UI feedback on the next page load
        flash(f"Submitted build for {labbuild_course} (Pods {s_pod}-{e_pod}) on {host}.", "info")
        return jsonify({"status": "success", "message": "Build submitted successfully."}), 200

    except Exception as e:
        logger.error(f"Error in /build-row: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Internal server error."}), 500

@bp.route('/intermediate-review', methods=['POST'])
def intermediate_build_review():
    """
    Handles the intermediate build review process. Includes special logic for "Maestro"
    split-build courses, creating separate build items for each component type.
    """
    current_theme = request.cookies.get('theme', 'light')
    selected_courses_json = request.form.get('selected_courses')

    # --- 1. NESTED HELPER FUNCTIONS (UNCHANGED) ---

    def _fetch_and_prepare_initial_data() -> Optional[Dict[str, Any]]:
        # This helper function remains the same as the previous version.
        initial_data = {
            "build_rules": [], "all_available_host_names": [], "host_memory_start_of_batch": {},
            "db_locked_pods": defaultdict(set),
            "course_configs": [], "available_lab_courses": defaultdict(set),
            "f5_highest_class_number_used": 0,
            "locations_map": {}
        }
        try:
            initial_data["build_rules"] = list(build_rules_collection.find().sort("priority", pymongo.ASCENDING))
            hosts_docs = list(host_collection.find({"include_for_build": "true"}))
            initial_data["all_available_host_names"] = sorted([h['host_name'] for h in hosts_docs if 'host_name' in h])
            initial_data["course_configs"] = list(course_config_collection.find({}, {"course_name": 1, "vendor_shortcode": 1, "memory_gb_per_pod": 1, "memory": 1, "_id": 0}))
            for cfg in initial_data["course_configs"]: initial_data["available_lab_courses"][cfg.get('vendor_shortcode', '').lower()].add(cfg.get('course_name'))
            db_allocs_cursor = alloc_collection.find({"extend": "true"})
            for tag_doc in db_allocs_cursor:
                for course in tag_doc.get("courses", []):
                    vendor = course.get("vendor", "").lower()
                    if not vendor: continue
                    for pd in course.get("pod_details", []):
                        try:
                            if vendor == 'f5':
                                if pd.get("class_number") is not None:
                                    class_num_int = int(pd["class_number"])
                                    initial_data["db_locked_pods"]['f5'].add(class_num_int)
                                    initial_data["f5_highest_class_number_used"] = max(initial_data["f5_highest_class_number_used"], class_num_int)
                                for nested in pd.get("pods", []):
                                    if nested.get("pod_number") is not None: initial_data["db_locked_pods"]['f5'].add(int(nested["pod_number"]))
                            elif pd.get("pod_number") is not None: initial_data["db_locked_pods"][vendor].add(int(pd["pod_number"]))
                        except (ValueError, TypeError): continue
            initial_data["db_locked_pods"]['f5'].add(17)
            logger.info("Reserved pod number 17 for vendor 'f5'. It will not be allocated.")
            raw_caps_gb = get_hosts_available_memory_parallel(hosts_docs) if hosts_docs else {}
            for h_doc in hosts_docs:
                h_name = h_doc.get("host_name")
                vc_cap = raw_caps_gb.get(h_name)
                if h_name and vc_cap is not None: initial_data["host_memory_start_of_batch"][h_name] = round(vc_cap, 2)

            if locations_collection is not None:
                locations_cursor = locations_collection.find({})
                # Create a map of 'code' -> 'name'
                initial_data["locations_map"] = {loc['code']: loc['name'] for loc in locations_cursor if 'code' in loc and 'name' in loc}
                logger.info(f"Loaded {len(initial_data['locations_map'])} locations from the database.")
            else:
                logger.warning("Locations collection is not available.")

            return initial_data
        except Exception as e:
            logger.error(f"Error during initial data fetch: {e}", exc_info=True)
            flash("Error fetching configuration data. Cannot proceed.", "danger")
            return None

    def _propose_assignments(
        num_pods_to_allocate: int,
        hosts_to_try: List[str],
        memory_per_pod: float,
        vendor_code: str,
        start_pod_suggestion: int,
        pods_assigned_in_this_batch: Dict[str, set],
        db_locked_pods: Dict[str, set],
        current_host_capacities_gb: Dict[str, float]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        # This helper function remains the same as the previous version.
        assignments: List[Dict[str, Any]] = []
        warning_message: Optional[str] = None
        flat_hosts_list = []
        if isinstance(hosts_to_try, list):
            for item in hosts_to_try:
                if isinstance(item, str):
                    flat_hosts_list.append(item)
                elif isinstance(item, list):
                    # If we find a nested list, extend the flat list with its string elements
                    flat_hosts_list.extend([str(sub_item) for sub_item in item if isinstance(sub_item, str)])
        elif isinstance(hosts_to_try, str):
            # Handle the case where a single string is passed
            flat_hosts_list.append(hosts_to_try)
        
        if num_pods_to_allocate <= 0: return [], None
        if memory_per_pod <= 0: return [], "Memory for LabBuild course is 0, cannot allocate."
        unavailable_pods = db_locked_pods.get(vendor_code, set()).union(pods_assigned_in_this_batch.get(vendor_code, set()))
        best_overallocated_contiguous_option: Optional[Tuple[str, int]] = None
        for host_target in hosts_to_try:
            candidate_start_pod = start_pod_suggestion
            attempts = 0
            while attempts < 1000:
                attempts += 1
                potential_range = range(candidate_start_pod, candidate_start_pod + num_pods_to_allocate)
                first_conflict = next((p for p in potential_range if p in unavailable_pods), -1)
                if first_conflict == -1:
                    start_pod, end_pod = potential_range.start, potential_range.stop - 1
                    memory_needed = memory_per_pod + (num_pods_to_allocate - 1) * memory_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR
                    host_capacity = current_host_capacities_gb.get(host_target, 0.0)
                    if host_capacity >= memory_needed:
                        assignments.append({"host": host_target, "start_pod": start_pod, "end_pod": end_pod})
                        for i in range(start_pod, end_pod + 1): pods_assigned_in_this_batch[vendor_code].add(i)
                        current_host_capacities_gb[host_target] -= memory_needed
                        return assignments, None
                    elif best_overallocated_contiguous_option is None:
                        best_overallocated_contiguous_option = (host_target, start_pod)
                    break
                else:
                    candidate_start_pod = first_conflict + 1
        if best_overallocated_contiguous_option:
            host_target, start_pod = best_overallocated_contiguous_option
            end_pod = start_pod + num_pods_to_allocate - 1
            assignments.append({"host": host_target, "start_pod": start_pod, "end_pod": end_pod})
            memory_needed = memory_per_pod + (num_pods_to_allocate - 1) * memory_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR
            warning_message = f"Memory Warning: Host '{host_target}' may be over-allocated."
            for i in range(start_pod, end_pod + 1): pods_assigned_in_this_batch[vendor_code].add(i)
            current_host_capacities_gb[host_target] -= memory_needed
            return assignments, warning_message
        pods_left_to_assign = num_pods_to_allocate
        for host_target in hosts_to_try:
            if pods_left_to_assign <= 0: break
            while pods_left_to_assign > 0:
                search_start_pod = start_pod_suggestion
                while search_start_pod in unavailable_pods: search_start_pod += 1
                potential_block_size = 0
                for i in range(pods_left_to_assign):
                    if (search_start_pod + i) in unavailable_pods: break
                    potential_block_size += 1
                if potential_block_size > 0:
                    start_pod, end_pod = search_start_pod, search_start_pod + potential_block_size - 1
                    assignments.append({"host": host_target, "start_pod": start_pod, "end_pod": end_pod})
                    mem_consumed = memory_per_pod + (potential_block_size - 1) * memory_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR
                    if current_host_capacities_gb.get(host_target, 0.0) < mem_consumed:
                        warn = f"Memory Warning: Host '{host_target}' may be over-allocated by block {start_pod}-{end_pod}."
                        warning_message = (warning_message + " " if warning_message else "") + warn
                    current_host_capacities_gb[host_target] -= mem_consumed
                    pods_left_to_assign -= potential_block_size
                    for i in range(start_pod, end_pod + 1): unavailable_pods.add(i)
                else:
                    break
        if pods_left_to_assign > 0:
            warn_final = f"Could not allocate all pods; {pods_left_to_assign} remain unassigned."
            warning_message = (warning_message + " " if warning_message else "") + warn_final
        return assignments, warning_message.strip() if warning_message else None

    def _process_single_course(course_input, initial_data, batch_state, batch_review_id):
        """
        Processes a single Salesforce course. If it's a Maestro course, it "explodes" it
        into multiple build items. Otherwise, it processes it as a standard course.
        This version includes the location lookup logic.

        Returns a LIST of (interim_doc, display_doc) tuples.
        """
        sf_code = course_input.get('sf_course_code', 'UNKNOWN')
        vendor = course_input.get('vendor', '').lower()
        sf_course_type = course_input.get('sf_course_type', 'N/A')
        
        # --- 1. Resolve Location from SF Course Code ---
        location_name = "Virtual" # Default value
        if len(sf_code) >= 8:
            # Extract characters from 6th to 8th position (index 5 to 7)
            location_code_from_sf = sf_code[5:8].upper()
            # Look up the code in the map fetched earlier in initial_data
            location_name = initial_data["locations_map"].get(location_code_from_sf, "Virtual")
            logger.debug(f"For SF code '{sf_code}', found location code '{location_code_from_sf}', resolved to '{location_name}'.")
        else:
            logger.debug(f"SF code '{sf_code}' is too short for location lookup. Defaulting to 'Virtual'.")

        # --- 2. Apply Build Rules ---
        matched_rules = _find_all_matching_rules(initial_data["build_rules"], vendor, sf_code, sf_course_type)

        maestro_rule = None
        rule_based_actions = {}
        # Loop in REVERSE (from highest priority number to lowest) to find the first Maestro rule
        for rule in reversed(matched_rules):
            if "maestro_split_build" in rule.get("actions", {}):
                maestro_rule = rule
                break # Found the highest priority Maestro rule, stop searching for it

        # Now loop normally (lowest priority to highest) to gather other generic actions
        for rule in matched_rules:
            # Exclude the maestro_split_build action itself from this generic collection
            for key in ["set_labbuild_course", "allow_spillover", "start_pod_number", "set_max_pods", "override_pods_req"]:
                if key not in rule_based_actions and key in rule.get("actions", {}):
                    rule_based_actions[key] = rule["actions"][key]
            # Collect host_priority only if it's NOT from our chosen maestro_rule
            if "host_priority" in rule.get("actions", {}) and not rule_based_actions.get("host_priority"):
                if not maestro_rule or rule.get('_id') != maestro_rule.get('_id'):
                    rule_based_actions["host_priority"] = rule["actions"]["host_priority"]

        # --- 3. Handle Maestro Split Build Logic ---
        if maestro_rule:
            maestro_rule_config = maestro_rule.get("actions", {}).get("maestro_split_build", {})
            logger.info(f"Applying Maestro Split Build logic for SF Course '{sf_code}' from rule '{maestro_rule.get('rule_name')}'.")
            processed_items = []

            
            base_interim_doc = {
                "batch_review_id": batch_review_id, "created_at": datetime.datetime.now(pytz.utc),
                "status": "pending_student_review", "sf_course_code": sf_code,
                "sf_course_type": sf_course_type, "sf_trainer_name": course_input.get('sf_trainer_name', 'N/A'),
                "sf_start_date": _format_date_for_review(course_input.get('sf_start_date'), sf_code),
                "sf_end_date": _format_date_for_review(course_input.get('sf_end_date'), sf_code),
                "sf_pax_count": int(course_input.get('sf_pax_count', 0)), "vendor": vendor,
                "location": location_name,
                "maestro_split_config_details_rule": maestro_rule_config
            }
            
            # --- HIGHLIGHTED MODIFICATION: Explicit Host List Handling ---
            
            # Get the host(s) for the RACK components
            rack_hosts_from_rule = maestro_rule_config.get("rack_host", [])
            rack_hosts_list = [rack_hosts_from_rule] if isinstance(rack_hosts_from_rule, str) else rack_hosts_from_rule

            # --- HIGHLIGHTED FIX: Get host_priority DIRECTLY from the Maestro rule ---
            # Do not use the generic rule_based_actions for this.
            main_host_priority_list = maestro_rule.get("actions", {}).get("host_priority", [])
            
            if not main_host_priority_list:
                logger.error(f"Maestro rule '{maestro_rule.get('rule_name')}' is missing the 'host_priority' action for main pods.")
            
            rack_hosts_set_lower = {h.lower() for h in rack_hosts_list}
            main_hosts_for_allocation = [h for h in main_host_priority_list if h.lower() not in rack_hosts_set_lower]

            logger.debug(f"Maestro Processing for '{sf_code}':")
            logger.debug(f"  - Rack Hosts defined: {rack_hosts_list}")
            logger.debug(f"  - Main Pod Host Priority (from Maestro Rule): {main_host_priority_list}")
            logger.debug(f"  - Final Main Pod Host list for allocation: {main_hosts_for_allocation}")
            
            # --- END MODIFICATION ---

            split_parts = [
                {"name": "Rack 1", "pods": 1, "course": maestro_rule_config.get("rack1_course"), "hosts": rack_hosts_list},
                {"name": "Rack 2", "pods": 1, "course": maestro_rule_config.get("rack2_course"), "hosts": rack_hosts_list},
                {"name": "Main Pods", "pods": 2, "course": maestro_rule_config.get("main_course"), "hosts": main_hosts_for_allocation}
            ]

            for part in split_parts:
                part_doc = base_interim_doc.copy()
                part_doc["_id"] = ObjectId()
                part_doc["maestro_part_name"] = part["name"]
                part_doc["final_labbuild_course"] = part["course"]
                part_doc["effective_pods_req"] = part["pods"]
                
                mem = _get_memory_for_course_local(part["course"], {c['course_name']: c for c in initial_data["course_configs"]})
                part_doc["memory_gb_one_pod"] = mem

                # The _propose_assignments function will now be called with the correct host list for each part
                assignments, warn = _propose_assignments(
                    num_pods_to_allocate=part["pods"],
                    hosts_to_try=part["hosts"], # Pass the correctly filtered host list
                    memory_per_pod=mem,
                    vendor_code=vendor,
                    start_pod_suggestion=rule_based_actions.get("start_pod_number", 1),
                    pods_assigned_in_this_batch=batch_state["pods_assigned"],
                    db_locked_pods=initial_data["db_locked_pods"],
                    current_host_capacities_gb=batch_state["capacities"]
                )

                display_doc = part_doc.copy()
                display_doc["initial_interactive_sub_rows_student"] = assignments
                display_doc["student_assignment_warning"] = warn
                processed_items.append((part_doc, display_doc))

            return processed_items
            
        # --- 4. Handle Standard (non-Maestro) Build Logic ---
        else:
            final_lb_course, warning = None, None
            available_courses = initial_data["available_lab_courses"].get(vendor, set())
            user_lb_course = course_input.get('labbuild_course')
            if user_lb_course and user_lb_course in available_courses:
                final_lb_course = user_lb_course
            else:
                if user_lb_course: warning = f"User selection '{user_lb_course}' invalid. "
                rule_course = rule_based_actions.get('set_labbuild_course')
                if rule_course and rule_course in available_courses:
                    final_lb_course = rule_course
                elif rule_course:
                    warning = (warning or "") + f"Rule course '{rule_course}' is invalid."
            
            pods_req_str = str(course_input.get('sf_pods_req', '1'))
            pods_req_calc_match = re.match(r"^\s*(\d+)", pods_req_str)
            eff_pods_req = int(pods_req_calc_match.group(1)) if pods_req_calc_match else 1
            if rule_based_actions.get("override_pods_req") is not None:
                eff_pods_req = int(rule_based_actions.get("override_pods_req"))
            if rule_based_actions.get("set_max_pods") is not None:
                eff_pods_req = min(eff_pods_req, int(rule_based_actions.get("set_max_pods")))
            
            mem_per_pod = _get_memory_for_course_local(final_lb_course, {c['course_name']: c for c in initial_data["course_configs"]}) if final_lb_course else 0.0

            hosts_to_try = rule_based_actions.get("host_priority", initial_data["all_available_host_names"])
            if rule_based_actions.get("allow_spillover", True):
                hosts_to_try = list(dict.fromkeys(hosts_to_try + initial_data["all_available_host_names"]))

            assignments, auto_assign_warn = _propose_assignments(
                num_pods_to_allocate=max(0, eff_pods_req), hosts_to_try=hosts_to_try, memory_per_pod=mem_per_pod,
                vendor_code=vendor, start_pod_suggestion=max(1, int(rule_based_actions.get('start_pod_number', 1))),
                pods_assigned_in_this_batch=batch_state["pods_assigned"], db_locked_pods=initial_data["db_locked_pods"],
                current_host_capacities_gb=batch_state["capacities"]
            )
            
            interim_doc = {
                "_id": ObjectId(), "batch_review_id": batch_review_id,
                "created_at": datetime.datetime.now(pytz.utc), "status": "pending_student_review",
                "sf_course_code": sf_code, "sf_course_type": sf_course_type,
                "sf_trainer_name": course_input.get('sf_trainer_name', 'N/A'),
                "sf_start_date": _format_date_for_review(course_input.get('sf_start_date'), sf_code),
                "sf_end_date": _format_date_for_review(course_input.get('sf_end_date'), sf_code),
                "sf_pax_count": int(course_input.get('sf_pax_count', 0)),
                "location": location_name,  # <-- Add the resolved location
                "vendor": vendor, "final_labbuild_course": final_lb_course,
                "effective_pods_req": max(0, eff_pods_req),
                "memory_gb_one_pod": mem_per_pod, "assignments": []
            }
            
            if vendor == 'f5':
                next_class_num = batch_state['f5_class_number_cursor']
                while next_class_num in initial_data['db_locked_pods'].get('f5', set()):
                    next_class_num += 1
                interim_doc['f5_class_number'] = next_class_num
                batch_state['f5_class_number_cursor'] = next_class_num + 1

            final_warning = (warning + ". " if warning else "") + (auto_assign_warn or "")
            if not assignments and interim_doc["effective_pods_req"] > 0:
                final_warning += " Could not auto-assign any pods."
            
            display_doc = interim_doc.copy()
            display_doc["initial_interactive_sub_rows_student"] = assignments
            display_doc["student_assignment_warning"] = final_warning.strip() or None
            
            return [(interim_doc, display_doc)]

    def _save_interim_proposals(proposals, batch_review_id):
        if not proposals: return True
        try:
            update_ops = [UpdateOne({"_id": p["_id"]}, {"$set": p}, upsert=True) for p in proposals]
            result = interim_alloc_collection.bulk_write(update_ops)
            logger.info(f"Interim proposals saved to batch '{batch_review_id}': Upserted: {result.upserted_count}")
            return True
        except (PyMongoError, BulkWriteError) as e:
            logger.error(f"Error saving proposals to interim DB: {e}", exc_info=True)
            flash("Error saving initial review session data.", "danger")
            return False

    # --- 2. Main execution flow of the route ---
    if not selected_courses_json:
        flash("No courses selected for review.", "warning")
        return redirect(url_for('main.view_upcoming_courses'))
    try:
        selected_courses_input = json.loads(selected_courses_json)
        if not isinstance(selected_courses_input, list): raise ValueError
    except (json.JSONDecodeError, ValueError):
        flash("Invalid data received for review.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    initial_data = _fetch_and_prepare_initial_data()
    if initial_data is None:
        return redirect(url_for('main.view_upcoming_courses'))
    
    batch_review_id = str(ObjectId())
    batch_state = {
        "capacities": initial_data["host_memory_start_of_batch"].copy(),
        "pods_assigned": defaultdict(set),
        "f5_class_number_cursor": initial_data.get("f5_highest_class_number_used", 0) + 1
    }
    
    try:
        if interim_alloc_collection is not None:
            del_res = interim_alloc_collection.delete_many({})
            logger.info(f"Purged {del_res.deleted_count} old interim allocation documents. Starting new batch: {batch_review_id}")
    except PyMongoError as e:
        logger.error(f"Error purging old interim items: {e}", exc_info=True)
        
    db_docs_to_save, display_docs_for_template = [], []
    for course_input in selected_courses_input:
        processed_items = _process_single_course(course_input, initial_data, batch_state, batch_review_id)
        
        for interim_db_doc, display_template_doc in processed_items:
            db_docs_to_save.append(interim_db_doc)
            display_template_doc['interim_doc_id'] = str(interim_db_doc['_id'])
            display_template_doc['final_labbuild_course_student'] = display_template_doc.get('final_labbuild_course')
            display_template_doc['effective_pods_req_student'] = display_template_doc.get('effective_pods_req')
            display_docs_for_template.append(display_template_doc)
            
    if not _save_interim_proposals(db_docs_to_save, batch_review_id):
        return redirect(url_for('main.view_upcoming_courses'))

    return render_template(
        'intermediate_build_review.html',
        selected_courses=display_docs_for_template,
        batch_review_id=batch_review_id,
        current_theme=current_theme,
        all_hosts=initial_data["all_available_host_names"],
        course_configs_for_memory=initial_data["course_configs"],
        subsequent_pod_memory_factor=SUBSEQUENT_POD_MEMORY_FACTOR
    )


@bp.route('/prepare-trainer-pods', methods=['POST'])
def prepare_trainer_pods():
    """
    Receives finalized student assignments, saves them, and then proposes
    assignments for the corresponding trainer pods for a final review.
    This version ensures contiguous allocation of trainer pods for each vendor
    and allows allocation on any available host if priority hosts are full.
    """
    current_theme = request.cookies.get('theme', 'light')
    final_student_plan_json = request.form.get('final_build_plan')
    batch_review_id = request.form.get('batch_review_id')

    # --- 1. Internal Helper Functions ---

    def _update_finalized_student_assignments(batch_id, student_plan_data):
        """
        STEP 1: Persist Student Assignments.
        Updates the interim DB to mark student assignments as 'student_confirmed'
        and returns the full, updated documents for the next step.
        """
        if not student_plan_data:
            return None, "No student plan data provided."
        
        update_ops = []
        doc_ids_to_fetch = []
        for item in student_plan_data:
            doc_id_str = item.get('interim_doc_id')
            if not doc_id_str:
                logger.warning("Missing 'interim_doc_id' in student data.")
                continue
            
            doc_ids_to_fetch.append(ObjectId(doc_id_str))
            # Create a lean payload containing only the user's final decisions
            update_payload = {
                "assignments": item.get("interactive_assignments", []),
                "status": "student_confirmed", 
                "updated_at": datetime.datetime.now(pytz.utc)
            }
            update_ops.append(UpdateOne({"_id": ObjectId(doc_id_str), "batch_review_id": batch_id}, {"$set": update_payload}))

        try:
            if update_ops:
                interim_alloc_collection.bulk_write(update_ops)
            
            confirmed_docs = list(interim_alloc_collection.find({"_id": {"$in": doc_ids_to_fetch}}))
            return confirmed_docs, None
        except (PyMongoError, BulkWriteError, InvalidId) as e:
            logger.error(f"Error updating student assignments in interim DB for batch '{batch_id}': {e}", exc_info=True)
            return None, "Error saving confirmed student assignments."

    def _prepare_data_for_trainer_logic(confirmed_student_courses):
        """
        Aggregates all data needed for the trainer pod allocation logic.
        """
        prep_data = {
            "build_rules": [], "all_hosts_dd": [], "working_host_caps_gb": {},
            "taken_pods_vendor": defaultdict(set), "lb_courses_on_host": defaultdict(set),
            "course_configs_for_mem": []
        }
        try:
            prep_data["build_rules"] = list(build_rules_collection.find().sort("priority", ASCENDING))
            
            hosts_docs = list(host_collection.find({"include_for_build": "true"}))
            prep_data["all_hosts_dd"] = sorted([hd['host_name'] for hd in hosts_docs if 'host_name' in hd])
            initial_host_caps = get_hosts_available_memory_parallel(hosts_docs) if hosts_docs else {}
            prep_data["working_host_caps_gb"] = {h: cap for h, cap in initial_host_caps.items() if cap is not None}
            
            db_cursor = alloc_collection.find({"extend": "true"}, {"courses.vendor": 1, "courses.pod_details": 1})
            for tag_doc in db_cursor:
                for course in tag_doc.get("courses", []):
                    vendor = course.get("vendor", "").lower()
                    if not vendor: continue
                    for pd in course.get("pod_details", []):
                        if pd.get("pod_number") is not None: prep_data["taken_pods_vendor"][vendor].add(pd.get("pod_number"))
                        if vendor == 'f5':
                            if pd.get("class_number") is not None: prep_data["taken_pods_vendor"][vendor].add(pd.get("class_number"))
                            for np in pd.get("pods", []):
                                if np.get("pod_number") is not None: prep_data["taken_pods_vendor"][vendor].add(np.get("pod_number"))
            
            for stud_course in confirmed_student_courses:
                vendor, lb_course = stud_course.get("vendor", "").lower(), stud_course.get("final_labbuild_course")
                mem_one_pod = float(stud_course.get("memory_gb_one_pod", 0.0))
                for asgn in stud_course.get("assignments", []):
                    host = asgn.get("host")
                    if not all([host, vendor, lb_course]): continue
                    prep_data["lb_courses_on_host"][host].add(lb_course)
                    try:
                        s_pod, e_pod = int(asgn.get('start_pod', 0)), int(asgn.get('end_pod', 0))
                        if s_pod > 0 and e_pod >= s_pod:
                            num_pods = e_pod - s_pod + 1
                            for i in range(s_pod, e_pod + 1): prep_data["taken_pods_vendor"][vendor].add(i)
                            mem_deduct = mem_one_pod + ((num_pods - 1) * mem_one_pod * SUBSEQUENT_POD_MEMORY_FACTOR if num_pods > 1 else 0)
                            prep_data["working_host_caps_gb"][host] = max(0, round(prep_data["working_host_caps_gb"].get(host, 0.0) - mem_deduct, 2))
                    except (ValueError, TypeError): pass
            
            prep_data["course_configs_for_mem"] = list(course_config_collection.find({}, {"course_name": 1, "memory_gb_per_pod": 1, "memory": 1, "_id": 0}))
            return prep_data, None
        except (PyMongoError, Exception) as e:
            logger.error(f"Error preparing data for trainer logic: {e}", exc_info=True)
            return None, "Error preparing data for trainer pod assignment."

    def _propose_trainer_pod_for_course(student_course_doc, prep_data, batch_state):
        """
        STEP 3: Propose a single Trainer Pod using a per-vendor cursor and flexible host allocation.
        Determines the assignment for a single trainer pod based on rules and available resources.
        Modifies batch_state in place and returns the DB payload and display data.
        """
        student_sf_code, vendor, sf_type, f5_class_number = student_course_doc.get('sf_course_code'), student_course_doc.get('vendor', '').lower(), student_course_doc.get('sf_course_type', ''), student_course_doc.get('f5_class_number')
        tp_sf_code = f"{student_sf_code}-TP"
        
        relevant_rules = _find_all_matching_rules(prep_data["build_rules"], vendor, student_sf_code, sf_type)
        is_disabled = any(r.get("actions", {}).get("disable_trainer_pod") for r in relevant_rules)

        db_update_payload = {"updated_at": datetime.datetime.now(pytz.utc)}
        display_data = {"interim_doc_id": str(student_course_doc["_id"]), "sf_course_code": tp_sf_code, "vendor": vendor.upper(), "f5_class_number": f5_class_number}

        if is_disabled:
            db_update_payload.update({"trainer_labbuild_course": None, "trainer_assignment": None, "trainer_assignment_warning": "Trainer pod build explicitly disabled by rule."})
            display_data.update({"labbuild_course": "N/A - Disabled", "error_message": db_update_payload["trainer_assignment_warning"]})
        else:
            lb_course, mem_one_pod = student_course_doc.get('final_labbuild_course'), float(student_course_doc.get('memory_gb_one_pod', 0.0))
            tp_rules = _find_all_matching_rules(prep_data["build_rules"], vendor, tp_sf_code, sf_type)
            eff_actions_tp = {}
            for r in tp_rules:
                for k in ["set_labbuild_course", "host_priority", "start_pod_number"]:
                    if k not in eff_actions_tp and k in r.get("actions", {}): eff_actions_tp[k] = r["actions"][k]
            
            if eff_actions_tp.get("set_labbuild_course"): lb_course = eff_actions_tp.get("set_labbuild_course")
            if lb_course != student_course_doc.get('final_labbuild_course'): mem_one_pod = _get_memory_for_course_local(lb_course, {c['course_name']: c for c in prep_data["course_configs_for_mem"]})

            db_update_payload.update({"trainer_labbuild_course": lb_course, "trainer_memory_gb_one_pod": mem_one_pod})
            display_data.update({"labbuild_course": lb_course, "memory_gb_one_pod": mem_one_pod})
            
            host, pod, warning = None, None, None
            if lb_course and mem_one_pod > 0:
                # --- MODIFIED HOST SELECTION LOGIC ---
                priority_hosts_from_rule = eff_actions_tp.get("host_priority")
                all_available_hosts = prep_data.get("all_hosts_dd", [])
                
                if isinstance(priority_hosts_from_rule, list) and priority_hosts_from_rule:
                    # Construct a final list that tries priority hosts first, then spills over to all others.
                    # dict.fromkeys preserves order and removes duplicates.
                    hosts_to_try = list(dict.fromkeys(priority_hosts_from_rule + all_available_hosts))
                    logger.info(f"Trainer pod for '{tp_sf_code}' will try priority hosts first: {priority_hosts_from_rule}, then spill over.")
                else:
                    # If no priority list is defined, simply try all available hosts.
                    hosts_to_try = all_available_hosts
                    logger.info(f"Trainer pod for '{tp_sf_code}' will try all available hosts: {hosts_to_try}")
                # --- END OF MODIFICATION ---

                start_pod_sugg_from_rule = eff_actions_tp.get("start_pod_number", 1)
                cand_pod = batch_state["vendor_pod_cursors"].get(vendor, start_pod_sugg_from_rule)
                
                pod_found = False
                for host_cand in hosts_to_try:
                    search_pod = cand_pod
                    
                    while True: # Keep searching for the next available pod number
                        if search_pod not in batch_state["taken_pods_vendor"].get(vendor, set()):
                            is_first = lb_course not in batch_state["lb_courses_on_host"].get(host_cand, set())
                            mem_needed = round(mem_one_pod * (SUBSEQUENT_POD_MEMORY_FACTOR if not is_first else 1.0), 2)
                            host_cap = batch_state["working_host_caps_gb"].get(host_cand, 0.0)
                            
                            if host_cap >= mem_needed:
                                host, pod = host_cand, search_pod
                                pod_found = True
                                break # Found a spot on this host
                            else:
                                # Not enough memory on this host for any pod of this type, try next host.
                                break 
                        search_pod += 1
                    
                    if pod_found:
                        break # Exit the host loop since we found a spot

                if pod_found:
                    batch_state["taken_pods_vendor"][vendor].add(pod)
                    is_first_on_host = lb_course not in batch_state["lb_courses_on_host"].get(host, set())
                    mem_consumed = round(mem_one_pod * (SUBSEQUENT_POD_MEMORY_FACTOR if not is_first_on_host else 1.0), 2)
                    batch_state["working_host_caps_gb"][host] = max(0, round(batch_state["working_host_caps_gb"].get(host, 0.0) - mem_consumed, 2))
                    batch_state["lb_courses_on_host"][host].add(lb_course)
                    batch_state["vendor_pod_cursors"][vendor] = pod + 1
                    logger.info(f"Assigned trainer pod {pod} to {host}. Next cursor for '{vendor}' is {pod + 1}.")
                else:
                    warning = "No suitable host found with available capacity and a free contiguous pod number."
            else:
                warning = "Cannot assign trainer pod: Missing LabBuild course or memory is zero."
            
            db_update_payload.update({"trainer_assignment": {"host": host, "start_pod": pod, "end_pod": pod} if host and pod is not None else None, "trainer_assignment_warning": warning})
            display_data.update({"assigned_host": host, "start_pod": pod, "end_pod": pod, "error_message": warning})
            
        return db_update_payload, display_data

    def _save_trainer_proposals(trainer_updates, batch_id):
        """STEP 4: Save Trainer Proposals to the Database."""
        if not trainer_updates: return True
        try:
            update_ops = [UpdateOne({"_id": item["_id"], "batch_review_id": batch_id}, {"$set": item["payload"]}) for item in trainer_updates]
            result = interim_alloc_collection.bulk_write(update_ops)
            logger.info(f"Updated {result.modified_count} interim docs with trainer info for batch '{batch_id}'.")
            return True
        except (PyMongoError, BulkWriteError) as e:
            logger.error(f"Error saving trainer proposals to interim DB: {e}", exc_info=True)
            flash("Error saving trainer pod proposals.", "danger")
            return False

    # --- Main execution flow of the route ---
    if not batch_review_id or not final_student_plan_json:
        flash("Session ID or student plan missing.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))
    
    try:
        student_plan_data = json.loads(final_student_plan_json)
        if not isinstance(student_plan_data, list): raise ValueError
    except (json.JSONDecodeError, ValueError):
        flash("Error processing submitted student assignments.", "danger")
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    confirmed_student_courses, err = _update_finalized_student_assignments(batch_review_id, student_plan_data)
    if err:
        flash(err, "danger"); return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    confirmed_student_courses.sort(key=lambda x: (x.get('vendor', 'zzz'), x.get('sf_start_date', '')))

    prep_data, err = _prepare_data_for_trainer_logic(confirmed_student_courses)
    if err:
        flash(err, "danger"); return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    batch_trainer_state = {
        "working_host_caps_gb": prep_data["working_host_caps_gb"],
        "taken_pods_vendor": prep_data["taken_pods_vendor"],
        "lb_courses_on_host": prep_data["lb_courses_on_host"],
        "vendor_pod_cursors": defaultdict(lambda: 100)
    }
    
    trainer_updates_for_db = []
    display_items_for_template = []
    
    for student_doc in confirmed_student_courses:
        trainer_payload, trainer_display_data = _propose_trainer_pod_for_course(student_doc, prep_data, batch_trainer_state)
        trainer_updates_for_db.append({"_id": student_doc["_id"], "payload": trainer_payload})
        student_doc.update(trainer_payload)
        display_items_for_template.append(student_doc)

    if not _save_trainer_proposals(trainer_updates_for_db, batch_review_id):
        return redirect(url_for('actions.intermediate_build_review', batch_review_id=batch_review_id))

    return render_template(
        'trainer_pod_review.html',
        courses_and_trainer_pods=display_items_for_template,
        batch_review_id=batch_review_id,
        current_theme=current_theme,
        all_hosts=prep_data["all_hosts_dd"],
        course_configs_for_memory=prep_data["course_configs_for_mem"],
        subsequent_pod_memory_factor=SUBSEQUENT_POD_MEMORY_FACTOR
    )

@bp.route('/finalize-and-display-plan', methods=['POST'])
def finalize_and_display_build_plan():
    """
    Receives final trainer decisions, updates the DB, and then calls the
    refactored helper to display the final build/teardown plan.
    This is the entry point after the "trainer pod review" page.
    """
    batch_review_id = request.form.get('batch_review_id')
    final_trainer_plan_json = request.form.get('final_trainer_assignments_for_review')
    logger.info(f"Finalizing plan for Batch ID: {batch_review_id}")

    if not batch_review_id or not final_trainer_plan_json:
        flash("Session ID or final plan missing.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    try:
        trainer_plan_data = json.loads(final_trainer_plan_json)
    except json.JSONDecodeError:
        flash("Error processing final trainer assignments.", "danger")
        # Redirect back to the trainer pod review page
        return redirect(url_for('build_planner_actions.prepare_trainer_pods', batch_review_id=batch_review_id))

    # This is a private helper within this route's context
    def _update_finalized_trainer_assignments(batch_id, trainer_assignments_data):
        if not trainer_assignments_data:
            return True, "No trainer assignments to update."
        update_ops: List[UpdateOne] = []
        try:
            if interim_alloc_collection is None:
                return False, "Database service unavailable."
            for trainer_data in trainer_assignments_data:
                doc_id_str = trainer_data.get('interim_doc_id')
                build_trainer = trainer_data.get('build_trainer', False)
                if not doc_id_str:
                    logger.warning("Missing 'interim_doc_id' in trainer data.")
                    continue

                payload: Dict[str, Any] = {"updated_at": datetime.datetime.now(pytz.utc)}
                if build_trainer:
                    payload.update({
                        "trainer_labbuild_course": trainer_data.get("labbuild_course"),
                        "trainer_memory_gb_one_pod": trainer_data.get("memory_gb_one_pod"),
                        "trainer_assignment": trainer_data.get("interactive_assignments", []),
                        "trainer_assignment_warning": trainer_data.get("error_message"),
                        "status": "trainer_confirmed"
                    })
                else:
                    payload.update({
                        "trainer_assignment": None,
                        "status": "trainer_skipped_by_user",
                        "trainer_assignment_warning": "Build skipped by user."
                    })
                update_ops.append(UpdateOne({"_id": ObjectId(doc_id_str), "batch_review_id": batch_id}, {"$set": payload}))

            if update_ops:
                result = interim_alloc_collection.bulk_write(update_ops)
                logger.info(f"Updated {result.modified_count} trainer assignments in interim (batch '{batch_id}').")
            return True, None
        except Exception as e:
            logger.error(f"Error saving finalized trainer assignments to interim DB: {e}", exc_info=True)
            return False, "Error saving finalized trainer assignments."

    # First, save the user's final decisions for trainer pods
    success, error = _update_finalized_trainer_assignments(batch_review_id, trainer_plan_data)
    if not success:
        flash(error, "danger")
        return redirect(url_for('build_planner_actions.prepare_trainer_pods', batch_review_id=batch_review_id))

    # Now, call the main rendering helper, ensuring APM data is regenerated
    return _prepare_and_render_final_review(batch_review_id, regenerate_apm=True)

@bp.route('/execute-scheduled-builds', methods=['POST'])
def execute_scheduled_builds():
    """
    Receives the confirmed build plan, teardown preference, and scheduling options,
    then schedules the labbuild setup (and optionally teardown) commands.
    """
    confirmed_plan_json = request.form.get('confirmed_build_plan_data')
    schedule_option = request.form.get('schedule_option', 'now')
    batch_review_id = request.form.get('batch_review_id')
    perform_teardown_first = request.form.get('perform_teardown_first') == 'yes'

    logger.info(f"Execute Scheduled Builds for Batch ID '{batch_review_id}': "
                f"Teardown before setup = {perform_teardown_first}, "
                f"Schedule Option = {schedule_option}")

    schedule_start_time_str: Optional[str] = None
    stagger_minutes: int = 30

    if schedule_option in ['now', 'staggered']:
        # Always apply a short auto-stagger when 'now' is chosen
        if schedule_option == 'now':
            stagger_minutes = int(os.getenv("AUTO_STAGGER_MINUTES", "5"))
            schedule_option  = 'staggered'      # treat as staggered under the hood

    if schedule_option == 'specific_time_all':
        schedule_start_time_str = request.form.get('schedule_start_time')
    elif schedule_option == 'staggered':
        schedule_start_time_str = request.form.get('schedule_start_time_staggered')
        try:
            stagger_minutes = int(request.form.get('schedule_stagger_minutes', '30'))
            if stagger_minutes < 1:
                stagger_minutes = 1
        except (ValueError, TypeError):
            stagger_minutes = 30

    if not confirmed_plan_json:
        flash("No confirmed build plan received to schedule.", "danger")
        return redirect(url_for('actions.finalize_and_display_build_plan', batch_review_id=batch_review_id))

    try:
        buildable_items_from_form = json.loads(confirmed_plan_json)
        if not isinstance(buildable_items_from_form, list):
            raise ValueError("Invalid confirmed build plan format.")
    except (json.JSONDecodeError, ValueError) as e:
        flash(f"Error processing confirmed build plan: {e}", "danger")
        return redirect(url_for('actions.finalize_and_display_build_plan', batch_review_id=batch_review_id))

    if scheduler is None or not scheduler.running:
        flash("Scheduler not running. Cannot schedule builds.", "danger")
        return redirect(url_for('main.index'))

    logger.info(f"Received {len(buildable_items_from_form)} buildable items to schedule for "
                f"batch '{batch_review_id}' with option: '{schedule_option}'.")
    
    scheduler_tz_obj = scheduler.timezone
    base_run_time_utc: datetime.datetime = datetime.datetime.now(pytz.utc) # Default to now in UTC

    if schedule_option != 'now' and schedule_start_time_str:
        try:
            # 1. Parse the naive datetime string from the form
            naive_dt_from_form = datetime.datetime.fromisoformat(schedule_start_time_str)
            
            # 2. Localize the naive datetime using the scheduler's configured timezone.
            #    This correctly handles DST and attaches the right timezone info.
            localized_dt = scheduler_tz_obj.localize(naive_dt_from_form, is_dst=None)
            
            # 3. Convert the now-aware local time to UTC for scheduling. APScheduler works best with UTC.
            base_run_time_utc = localized_dt.astimezone(pytz.utc)
            
            logger.info(f"Scheduling base time set. Input: '{schedule_start_time_str}', "
                        f"Interpreted in '{scheduler_tz_obj}': {localized_dt.isoformat()}, "
                        f"Scheduled for (UTC): {base_run_time_utc.isoformat()}")

        except (ValueError, pytz.exceptions.NonExistentTimeError, pytz.exceptions.AmbiguousTimeError) as e_date_sched:
            error_msg = f"Invalid or ambiguous schedule time '{schedule_start_time_str}'. Scheduling for 'now'. Error: {e_date_sched}"
            logger.error(error_msg)
            flash(error_msg, "warning")
            base_run_time_utc = datetime.datetime.now(pytz.utc) # Fallback to now

    current_operation_block_start_time_utc = base_run_time_utc
    stagger_delta = datetime.timedelta(minutes=stagger_minutes)
    setup_delay_after_teardown = datetime.timedelta(minutes=2)
    
    scheduled_ops_count = 0
    failed_ops_count = 0
    scheduled_op_details_for_interim: List[Dict[str, Any]] = []

    for item_idx, item_to_build in enumerate(buildable_items_from_form):
        item_sf_code = item_to_build.get('original_sf_course_code', item_to_build.get('sf_course_code', f'UNKNOWN_SF_{item_idx}'))
        item_vendor = item_to_build.get('vendor')
        item_lb_course = item_to_build.get('labbuild_course')

        if not item_vendor or not item_lb_course:
            failed_ops_count += 1
            continue
            
        assignments = item_to_build.get("assignments", [])
        if not assignments:
            failed_ops_count += 1
            continue

        for assignment_idx, assignment_block in enumerate(assignments):
            host = assignment_block.get('host')
            start_pod = assignment_block.get('start_pod')
            end_pod = assignment_block.get('end_pod')

            if not host or start_pod is None or end_pod is None:
                failed_ops_count +=1
                continue

            if schedule_option == 'staggered' and (item_idx > 0 or assignment_idx > 0):
                current_operation_block_start_time_utc += stagger_delta
            elif schedule_option == 'now':
                current_operation_block_start_time_utc = datetime.datetime.now(pytz.utc)
            
            is_student_build = item_to_build.get('type') == 'Student Build'
            tag = f"{item_sf_code.replace('/', '_').replace(' ', '_')}"
            if not is_student_build:
                tag += "_TP"
            
            base_args = ['-v', item_vendor, '-g', item_lb_course, '--host', host, '-s', str(start_pod), '-e', str(end_pod), '-t', tag[:45]]
            
            if item_vendor.lower() == 'f5' and item_to_build.get('f5_class_number'):
                 base_args.extend(['-cn', str(item_to_build['f5_class_number'])])

            teardown_job_id = None
            actual_teardown_run_time_utc = current_operation_block_start_time_utc
            if perform_teardown_first:
                args_list_teardown = ['teardown'] + base_args
                job_name_td = f"Teardown_{tag}"
                try:
                    trigger_td = DateTrigger(run_date=actual_teardown_run_time_utc, timezone=pytz.utc)
                    job_td = scheduler.add_job(run_labbuild_task, trigger=trigger_td, args=[args_list_teardown], name=job_name_td, misfire_grace_time=1800, replace_existing=False)
                    teardown_job_id = job_td.id
                    scheduled_op_details_for_interim.append({"operation": "teardown", "job_id": job_td.id, "name": job_name_td, "run_time_utc": actual_teardown_run_time_utc.isoformat(), "args": args_list_teardown})
                    scheduled_ops_count += 1
                except Exception as e_sched_td:
                    logger.error(f"Failed to schedule TEARDOWN job {job_name_td}: {e_sched_td}", exc_info=True)
                    failed_ops_count += 1
            
            args_list_setup = ['setup'] + base_args
            
            if item_to_build.get('start_date'): args_list_setup.extend(['--start-date', item_to_build['start_date']])
            if item_to_build.get('end_date'): args_list_setup.extend(['--end-date', item_to_build['end_date']])
            if item_to_build.get('sf_trainer_name'): args_list_setup.extend(['--trainer-name', item_to_build['sf_trainer_name']])
            
            apm_user = item_to_build.get('apm_username') if is_student_build else item_to_build.get('trainer_apm_username')
            apm_pass = item_to_build.get('apm_password') if is_student_build else item_to_build.get('trainer_apm_password')

            if apm_user: args_list_setup.extend(['--username', apm_user])
            if apm_pass: args_list_setup.extend(['--password', apm_pass])
            
            job_name_setup = f"Setup_{tag}"
            actual_setup_run_time_utc = actual_teardown_run_time_utc + setup_delay_after_teardown if perform_teardown_first else current_operation_block_start_time_utc

            try:
                trigger_setup = DateTrigger(run_date=actual_setup_run_time_utc, timezone=pytz.utc)
                job_setup = scheduler.add_job(run_labbuild_task, trigger=trigger_setup, args=[args_list_setup], name=job_name_setup, misfire_grace_time=7200, replace_existing=False)
                scheduled_op_details_for_interim.append({"operation": "setup", "job_id": job_setup.id, "name": job_name_setup, "run_time_utc": actual_setup_run_time_utc.isoformat(), "args": args_list_setup, "depends_on_job_id": teardown_job_id})
                scheduled_ops_count += 1
            except Exception as e_sched_setup:
                logger.error(f"Failed to schedule SETUP job {job_name_setup}: {e_sched_setup}", exc_info=True)
                failed_ops_count += 1

    final_batch_status = "builds_scheduled_with_errors"
    if scheduled_ops_count > 0 and failed_ops_count == 0:
        final_batch_status = "builds_fully_scheduled"
    elif scheduled_ops_count == 0 and failed_ops_count > 0:
        final_batch_status = "build_scheduling_all_failed"
    elif scheduled_ops_count == 0 and failed_ops_count == 0:
         final_batch_status = "no_builds_to_schedule"
    
    if interim_alloc_collection is not None and batch_review_id:
        try:
            update_filter = {"batch_review_id": batch_review_id, "status": {"$in": ["student_confirmed", "trainer_confirmed", "trainer_disabled_by_rule", "trainer_skipped_by_user"]}}
            update_doc = {"$set": {"status": final_batch_status, "scheduled_operations": scheduled_op_details_for_interim, "scheduled_at_utc": datetime.datetime.now(pytz.utc)}}
            interim_alloc_collection.update_many(update_filter, update_doc)
        except PyMongoError as e_upd_interim:
            logger.error(f"Error updating final interim status for batch '{batch_review_id}': {e_upd_interim}", exc_info=True)

    flash(f"Attempted to schedule operations. Scheduled: {scheduled_ops_count}. Failures: {failed_ops_count}.", 'success' if failed_ops_count == 0 and scheduled_ops_count > 0 else 'warning' if scheduled_ops_count > 0 else 'danger')
    return redirect(url_for('main.index'))

@bp.route('/view-pending-allocation', methods=['GET'])
def view_upcoming_allocation():
    """
    Finds the most recent pending build batch from the interim collection
    and takes the user directly to the final review page for it.
    This is triggered by the "View Pending Allocation" button.
    """
    logger.info("Request received to view latest pending allocation.")

    if interim_alloc_collection is None:
        flash("Database service is unavailable. Cannot check for pending allocations.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))

    try:
        # Find the most recently created document in the interim collection.
        # This will belong to the last batch that was started.
        latest_doc = interim_alloc_collection.find_one(
            sort=[("created_at", DESCENDING)]
        )

        if not latest_doc or 'batch_review_id' not in latest_doc:
            flash("No pending build allocation found to review. Please select courses to start a new one.", "info")
            return redirect(url_for('main.view_upcoming_courses'))
        
        latest_batch_id = latest_doc['batch_review_id']
        logger.info(f"Found latest pending batch ID: {latest_batch_id}. Preparing final review page.")
        
        # Call the helper function that contains the logic to render the final page
        return _prepare_and_render_final_review(latest_batch_id, regenerate_apm=False)

    except PyMongoError as e:
        logger.error(f"Database error while trying to find pending allocation: {e}", exc_info=True)
        flash("A database error occurred while checking for pending allocations.", "danger")
        return redirect(url_for('main.view_upcoming_courses'))


# ==============================================================================
# SECTION: Helper Functions
# ==============================================================================

def _prepare_and_render_final_review(batch_review_id: str, regenerate_apm: bool = True):
    """
    Internal helper to fetch, process, and render the final review page.
    This is called by both the multi-step build process and the direct
    "view pending allocation" route.

    :param batch_review_id: The ID of the batch to review.
    :param regenerate_apm: If True, it will generate new APM data and update the DB.
                           If False, it will just fetch and display existing data.
    """
    logger.info(f"Preparing final review page for Batch ID: {batch_review_id} (Regenerate APM: {regenerate_apm})")
    current_theme = request.cookies.get('theme', 'light')

    # --- Nested Helper: Fetch all finalized docs for the batch ---
    def _fetch_all_plan_data(batch_id):
        finalized_statuses = ["student_confirmed", "trainer_confirmed", 
                              "trainer_skipped_by_user", "trainer_disabled_by_rule",
                              "builds_fully_scheduled"]
        try:
            if interim_alloc_collection is None:
                return [], "Database service unavailable."
            cursor = interim_alloc_collection.find(
                {"batch_review_id": batch_id, "status": {"$in": finalized_statuses}}
            ).sort([("sf_start_date", ASCENDING), ("sf_course_code", ASCENDING)])
            return list(cursor), None
        except PyMongoError as e:
            logger.error(f"Error fetching final plan items for batch '{batch_id}': {e}", exc_info=True)
            return [], "Error fetching review data."

    # --- Nested Helper: Update DB with APM credentials ---
    def _update_db_with_apm(batch_id, apm_creds_map):
        if not apm_creds_map:
            return True, "No APM credentials to update."
        update_ops = []
        for apm_code, creds in apm_creds_map.items():
            if apm_code.endswith("-TP"):
                sf_code = apm_code[:-3]
                payload = {"trainer_apm_username": creds.get("username"), "trainer_apm_password": creds.get("password")}
                update_ops.append(UpdateOne({"batch_review_id": batch_id, "sf_course_code": sf_code}, {"$set": payload}))
            else:
                payload = {"student_apm_username": creds.get("username"), "student_apm_password": creds.get("password")}
                update_ops.append(UpdateOne({"batch_review_id": batch_id, "sf_course_code": apm_code}, {"$set": payload}))
        try:
            if update_ops and interim_alloc_collection is not None:
                interim_alloc_collection.bulk_write(update_ops)
            return True, None
        except Exception as e:
            logger.error(f"Error updating interim DB with APM credentials: {e}", exc_info=True)
            return False, "Database error while saving APM credentials."

    # --- Nested Helper: Process final plan for display ---
    def _process_plan_for_display(final_plan_docs):
        """
        STEP 6: Prepare Data for Rendering.
        This version ensures the correct APM credentials are assigned to the trainer item.
        """
        processed_items: List[Dict] = []
        logger.info(f"[_process_plan_for_display] Processing {len(final_plan_docs)} final documents for display.")

        for doc in final_plan_docs:
            sf_code = doc.get("sf_course_code")
            vendor = doc.get("vendor")
            logger.debug(f"[_process_plan_for_display] Processing doc for SF Code: {sf_code}")

            # --- Student Part (This part is correct) ---
            student_item = {
                "type": "Student Build",
                "sf_course_code": sf_code,
                "original_sf_course_code": sf_code,
                "labbuild_course": doc.get("final_labbuild_course"),
                "sf_course_type": doc.get("sf_course_type"),
                "vendor": vendor,
                "start_date": doc.get("sf_start_date"),
                "end_date": doc.get("sf_end_date"),
                "location": doc.get("location", "Virtual"),
                "assignments": doc.get("assignments", []),
                "status_note": doc.get("student_assignment_warning"),
                "sf_trainer_name": doc.get("sf_trainer_name"),
                "f5_class_number": doc.get("f5_class_number"),
                "sf_pax_count": doc.get("sf_pax_count"),
                "effective_pods_req_student": doc.get("effective_pods_req"),
                "memory_gb_one_pod": doc.get("memory_gb_one_pod"),
                "apm_username": doc.get("student_apm_username"),
                "apm_password": doc.get("student_apm_password"),
            }
            processed_items.append(student_item)
            logger.debug(f"[_process_plan_for_display] Added student item. APM User: {student_item.get('apm_username')}")

            # --- Trainer Part ---
            if doc.get("status") in ["trainer_confirmed", "trainer_skipped_by_user", "trainer_disabled_by_rule"]:
                is_skipped = not doc.get("trainer_assignment")
                trainer_sfc_display = sf_code + ("-TP (Skipped)" if is_skipped else "-TP")
                
                # --- HIGHLIGHTED FIX: Use trainer_apm_* fields for the generic keys ---
                trainer_item = {
                    "type": "Trainer Build",
                    "sf_course_code": trainer_sfc_display,
                    "original_sf_course_code": sf_code,
                    "labbuild_course": doc.get("trainer_labbuild_course") or "N/A",
                    "vendor": vendor,
                    "start_date": doc.get("sf_start_date"),
                    "end_date": doc.get("sf_end_date"),
                    "location": doc.get("location", "Virtual"),
                    "assignments": doc.get("trainer_assignment") or [],
                    "status_note": doc.get("trainer_assignment_warning"),
                    "sf_trainer_name": doc.get("sf_trainer_name"),
                    "f5_class_number": doc.get("f5_class_number"),
                    
                    # This is the corrected block
                    "apm_username": doc.get("trainer_apm_username"),
                    "apm_password": doc.get("trainer_apm_password"),
                    
                    # These fields are still useful for context if needed elsewhere, but not for display
                    "student_apm_username": doc.get("student_apm_username"),
                    "trainer_apm_username": doc.get("trainer_apm_username"),
                    "trainer_apm_password": doc.get("trainer_apm_password")
                }
                # --- END OF FIX ---
                processed_items.append(trainer_item)
                logger.debug(f"[_process_plan_for_display] Added trainer item. APM User: {trainer_item.get('apm_username')}")

        return processed_items

    # --- Nested Helper: Sanitize data for JSON embedding ---
    def _sanitize_for_json(data):
        if isinstance(data, list): return [_sanitize_for_json(i) for i in data]
        if isinstance(data, dict): return {str(k): _sanitize_for_json(v) for k,v in data.items()}
        if isinstance(data, ObjectId): return str(data)
        if isinstance(data, datetime.datetime): return data.astimezone(pytz.utc).isoformat().replace("+00:00", "Z")
        if isinstance(data, datetime.date): return data.isoformat()
        return data

    # --- Main execution flow of the helper ---
    final_plan_docs, error = _fetch_all_plan_data(batch_review_id)
    if error:
        flash(error, "danger")
        return redirect(url_for('main.index'))
    if not final_plan_docs:
        flash(f"No data found for review batch '{batch_review_id}'. It may be expired or invalid.", "warning")
        return redirect(url_for('main.view_upcoming_courses'))
    
    apm_commands = []
    
    if regenerate_apm:
        logger.info(f"Regenerating APM data for batch '{batch_review_id}'...")
        
        # Step 1: Fetch current APM state and extended tags
        current_apm_entries = {}
        extended_apm_codes = set()
        apm_errors = []
        try:
            apm_list_url = os.getenv("APM_LIST_URL", "http://connect.rededucation.com:1212/list")
            response = requests.get(apm_list_url, timeout=15)
            response.raise_for_status()
            current_apm_entries = response.json()
        except Exception as e:
            apm_errors.append(f"Could not fetch current APM state: {e}")
        
        if alloc_collection is not None:
            try:
                for doc in alloc_collection.find({"extend": "true"}, {"tag": 1, "_id": 0}):
                    if doc.get("tag"): extended_apm_codes.add(doc.get("tag"))
            except PyMongoError:
                apm_errors.append("Could not fetch extended allocation tags.")

        # Step 2: Call the helper with all required arguments
        apm_commands, apm_creds_map, gen_errors = generate_apm_helper(
            final_plan_docs, 
            current_apm_entries, 
            extended_apm_codes
        )
        apm_errors.extend(gen_errors) # Combine any errors

        if apm_errors:
            flash("Note: Errors occurred during APM data generation: " + " | ".join(apm_errors), "info")
        
        # Step 3: Update DB with new credentials
        success, error = _update_db_with_apm(batch_review_id, apm_creds_map)
        if not success:
            flash(error, "danger")
        
        # Re-fetch to get the newly saved APM credentials
        final_plan_docs, _ = _fetch_all_plan_data(batch_review_id)
    else:
        # This "view-only" logic remains the same
        logger.info(f"Skipping APM data regeneration for batch '{batch_review_id}'. Displaying existing data.")
        try:
            # We still need to fetch the data to generate a preview of commands
            current_apm_entries = {}
            extended_apm_codes = set()
            try:
                apm_list_url = os.getenv("APM_LIST_URL", "http://connect.rededucation.com:1212/list")
                response = requests.get(apm_list_url, timeout=15); response.raise_for_status()
                current_apm_entries = response.json()
            except Exception: pass # Ignore errors for preview
            if alloc_collection is not None:
                for doc in alloc_collection.find({"extend": "true"}, {"tag": 1, "_id": 0}):
                    if doc.get("tag"): extended_apm_codes.add(doc.get("tag"))

            apm_commands, _, _ = generate_apm_helper(final_plan_docs, current_apm_entries, extended_apm_codes)
        except Exception as e:
            logger.warning(f"Could not generate APM command preview for view-only mode: {e}")
            apm_commands = ["# Could not generate command preview."]
    
    processed_plan_items = _process_plan_for_display(final_plan_docs)
    
    sanitized_all_items = _sanitize_for_json(processed_plan_items)
    sanitized_buildable_items = [
        item for item in sanitized_all_items
        if item.get("type") in ["Student Build", "Trainer Build"] and item.get("assignments")
    ]
    
    return render_template(
        'final_review_schedule.html',
        all_items_for_review=sanitized_all_items,
        buildable_items_json=json.dumps(sanitized_buildable_items),
        all_review_items_json=json.dumps(sanitized_all_items),
        apm_commands_for_preview=apm_commands,
        batch_review_id=batch_review_id,
        current_theme=current_theme,
    )