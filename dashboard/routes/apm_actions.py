# dashboard/routes/apm_actions.py

import logging
import json
import re
import os
import requests
from flask import Blueprint, request, jsonify
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Any
from pymongo.errors import PyMongoError
from ..extensions import alloc_collection, interim_alloc_collection
from .build_planner_actions import _create_contiguous_ranges, _generate_random_password

bp = Blueprint('apm_actions', __name__, url_prefix='/apm')
logger = logging.getLogger('dashboard.routes.apm_actions')


def _generate_apm_data_for_plan(
        local_final_build_plan_items: List[Dict], # Pass the already fetched interim items
        local_current_apm_entries: Dict[str, Dict[str, Any]],
        local_extended_apm_course_codes: Set[str]
    ) -> Tuple[Dict[str, Dict[str, str]], List[str], List[str]]:
        """
        Generates APM usernames and passwords for the items in the build plan.
        Returns:
            - apm_credentials_map (Dict[apm_auth_code, {"username": str, "password": str}])
            - all_generated_apm_commands (List[str])
            - apm_generation_errors (List[str])
        """
        logger.info(f"--- (Helper) APM Data Generation Started for {len(local_final_build_plan_items)} plan items ---")
        apm_commands_delete_local: List[str] = []
        apm_commands_add_update_local: List[str] = []
        errors_local: List[str] = []
        
        # This will store the final state including allocated usernames and passwords
        # Key: apm_auth_course_code, Value: { vpn_auth_courses, vpn_auth_range, ..., username, password }
        desired_apm_state_with_users: Dict[str, Dict[str, Any]] = {}

        # Step 3a: Build intermediate desired state (details without username yet)
        desired_apm_config_intermediate: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "all_pod_numbers": set(), "trainer_name_for_desc": "N/A", "course_type_for_desc": "N/A",
            "labbuild_course_for_version": "N/A", "vendor_short": "xx",
            "is_maestro_overall_for_version": False, "maestro_main_lb_for_version": None
        })

        for item_doc in local_final_build_plan_items:
            original_sf_code = item_doc.get("original_sf_course_code", item_doc.get("sf_course_code"))
            if not original_sf_code: continue

            # Determine if this is for Student APM entry or Trainer APM entry
            # A single interim 'doc' can lead to two APM entries (one for student, one for trainer)
            
            # Process Student Part from 'item_doc'
            if item_doc.get("status") in ["student_confirmed", "trainer_confirmed"]: # trainer_confirmed implies student part is also relevant
                student_assignments = item_doc.get("student_assignments_final", [])
                if student_assignments:
                    apm_auth_code_key_student = original_sf_code
                    student_entry_details = desired_apm_config_intermediate[apm_auth_code_key_student]
                    if student_entry_details["trainer_name_for_desc"] == "N/A": # Initialize if first time for this key
                        student_entry_details["trainer_name_for_desc"] = item_doc.get("sf_trainer_name", "N/A")
                        student_entry_details["course_type_for_desc"] = item_doc.get("sf_course_type", "Course")
                        student_entry_details["vendor_short"] = (item_doc.get("vendor", "xx") or "xx").lower()
                        maestro_config = item_doc.get("maestro_split_config_details_rule")
                        if isinstance(maestro_config, dict) and item_doc.get("vendor") == "cp":
                            student_entry_details["is_maestro_overall_for_version"] = True
                            student_entry_details["maestro_main_lb_for_version"] = maestro_config.get("main_course", item_doc.get("final_labbuild_course_student", "maestro"))
                        else:
                            student_entry_details["labbuild_course_for_version"] = item_doc.get("final_labbuild_course_student", item_doc.get("labbuild_course", "N/A"))
                    for asgn in student_assignments:
                        s, e = asgn.get("start_pod"), asgn.get("end_pod")
                        if s is not None and e is not None: 
                            try: [student_entry_details["all_pod_numbers"].add(p) for p in range(int(s),int(e)+1)]; 
                            except:pass
            
            # Process Trainer Part from 'item_doc'
            if item_doc.get("status") == "trainer_confirmed": # Only if trainer pod is to be built
                trainer_assignments = item_doc.get("trainer_assignment_final", [])
                if trainer_assignments:
                    apm_auth_code_key_trainer = f"{original_sf_code}-TP"
                    trainer_entry_details = desired_apm_config_intermediate[apm_auth_code_key_trainer]
                    if trainer_entry_details["trainer_name_for_desc"] == "N/A":
                        trainer_entry_details["trainer_name_for_desc"] = "Trainer Pods"
                        trainer_entry_details["course_type_for_desc"] = item_doc.get("sf_course_type", "Trainer Setup")
                        trainer_entry_details["labbuild_course_for_version"] = item_doc.get("trainer_labbuild_course", "N/A")
                        trainer_entry_details["vendor_short"] = (item_doc.get("vendor", "xx") or "xx").lower()
                    for asgn in trainer_assignments:
                        s, e = asgn.get("start_pod"), asgn.get("end_pod")
                        if s is not None and e is not None: 
                            try: [trainer_entry_details["all_pod_numbers"].add(p) for p in range(int(s),int(e)+1)]; 
                            except:pass
        
        # Step 3b: Finalize intermediate state (convert pod sets to ranges, etc.)
        for apm_auth_code, details in desired_apm_config_intermediate.items():
            if not details["all_pod_numbers"]: continue
            vpn_range = _create_contiguous_ranges(list(details["all_pod_numbers"]))
            if not vpn_range: continue
            desc = f"{details['trainer_name_for_desc']} - {details['course_type_for_desc']}"
            ver = details["labbuild_course_for_version"]
            if details["is_maestro_overall_for_version"] and details["maestro_main_lb_for_version"]: ver = details["maestro_main_lb_for_version"]
            
            desired_apm_state_with_users[apm_auth_code] = {
                "vpn_auth_courses": desc[:250], "vpn_auth_range": vpn_range,
                "vpn_auth_version": ver, "vpn_auth_expiry_date": "8",
                "password": _generate_random_password(), "vendor_short": details["vendor_short"]
                # Username to be added next
            }
        logger.info(f"(Helper) Prepared {len(desired_apm_state_with_users)} desired APM entries structure.")

        # Step 4 & 5: Username Allocation, Generate Commands
        # ... (This is the same complex logic as in your last working `generate_apm_commands_route`
        #      that iterates `current_apm_entries` and `desired_apm_state_with_users` (which was named desired_apm_state_from_plan before)
        #      to generate `apm_commands_delete_local`, `apm_commands_add_update_local`,
        #      and populates `details["username"]` in `desired_apm_state_with_users`)
        apm_course_code_to_username_to_keep: Dict[str, str] = {} 
        for username, existing_details in local_current_apm_entries.items(): # Use local_
            apm_cc = existing_details.get("vpn_auth_course_code")
            if not apm_cc: apm_commands_delete_local.append(f"course2 del {username}"); continue
            if apm_cc in local_extended_apm_course_codes: # Use local_
                if apm_cc not in apm_course_code_to_username_to_keep: apm_course_code_to_username_to_keep[apm_cc] = username
                if apm_cc in desired_apm_state_with_users: desired_apm_state_with_users[apm_cc]["username"] = username; desired_apm_state_with_users[apm_cc]["is_update"] = True
            elif apm_cc in desired_apm_state_with_users:
                if apm_cc not in apm_course_code_to_username_to_keep: apm_course_code_to_username_to_keep[apm_cc] = username
                new_d = desired_apm_state_with_users[apm_cc]
                if existing_details.get("vpn_auth_range") != new_d["vpn_auth_range"]: apm_commands_add_update_local.append(f'course2 range {username} "{new_d["vpn_auth_range"]}"')
                if existing_details.get("vpn_auth_version") != new_d["vpn_auth_version"]: apm_commands_add_update_local.append(f'course2 version {username} "{new_d["vpn_auth_version"]}"')
                if existing_details.get("vpn_auth_courses") != new_d["vpn_auth_courses"]: apm_commands_add_update_local.append(f'course2 description {username} "{new_d["vpn_auth_courses"]}"')
                if str(existing_details.get("vpn_auth_expiry_date")) != str(new_d["vpn_auth_expiry_date"]): apm_commands_add_update_local.append(f'course2 expiry_date {username} "{new_d["vpn_auth_expiry_date"]}"')
                apm_commands_add_update_local.append(f'course2 password {username} "{new_d["password"]}"')
                desired_apm_state_with_users[apm_cc]["username"] = username; desired_apm_state_with_users[apm_cc]["is_update"] = True
            else: apm_commands_delete_local.append(f"course2 del {username}")
        
        used_x_nums: Dict[str,Set[int]] = defaultdict(set)
        for _, uname_kept in apm_course_code_to_username_to_keep.items():
            m = re.match(r"lab([a-z0-9]+)-(\d+)",uname_kept.lower());
            if m: used_x_nums[m.group(1)].add(int(m.group(2)))

        for apm_auth_c, details_final in desired_apm_state_with_users.items():
            if details_final.get("is_update"): continue
            v_short = details_final["vendor_short"]; uname=None; x=1
            while True:
                if x not in used_x_nums.get(v_short,set()): uname=f"lab{v_short}-{x}"; used_x_nums[v_short].add(x); break
                x+=1
            details_final["username"] = uname # Store assigned username
            apm_commands_add_update_local.append(f'course2 add {uname} "{details_final["password"]}" "{details_final["vpn_auth_range"]}" "{details_final["vpn_auth_version"]}" "{details_final["vpn_auth_courses"]}" "{details_final["vpn_auth_expiry_date"]}" "{apm_auth_c}"')

        all_generated_commands = apm_commands_delete_local + apm_commands_add_update_local
        logger.info(f"(Helper) APM Logic Finished. Generated {len(all_generated_commands)} commands. Errors: {errors_local}")
        
        # Return the map of apm_auth_code to {"username": ..., "password": ...}
        apm_credentials_map_result: Dict[str, Dict[str, str]] = {}
        for apm_auth_code, details_with_user in desired_apm_state_with_users.items():
            if details_with_user.get("username") and details_with_user.get("password"):
                apm_credentials_map_result[apm_auth_code] = {
                    "username": details_with_user["username"],
                    "password": details_with_user["password"]
                }
        
        return all_generated_commands, apm_credentials_map_result, errors_local

@bp.route('/generate-commands', methods=['POST'])
def generate_apm_commands_route():
    batch_review_id_req = request.json.get('batch_review_id') # For context/logging
    logger.info(f"--- APM Command Generation Route Called (Batch ID context: {batch_review_id_req}) ---")

    # Fetch necessary current data for the APM logic helper
    current_apm_entries_route: Dict[str, Dict[str, Any]] = {}
    try:
        apm_list_url_route = os.getenv("APM_LIST_URL", "http://connect.rededucation.com:1212/list")
        response_route = requests.get(apm_list_url_route, timeout=15); response_route.raise_for_status()
        current_apm_entries_route = response_route.json()
    except Exception as e_route: 
        logger.error(f"APM Route: Error fetching current APM entries: {e_route}")
        return jsonify({"commands": ["# ERROR: Could not fetch current APM entries from server."], "message": f"Error fetching current APM: {e_route}", "error": True}), 500

    extended_apm_codes_route: Set[str] = set()
    try: # Fetch extended codes
        if alloc_collection is not None:
            cursor_route = alloc_collection.find({"extend": "true"}, {"tag": 1, "courses.apm_vpn_auth_course_code": 1, "_id": 0})
            for doc_route in cursor_route:
                found_route = False;
                for ca_route in doc_route.get("courses",[]):
                    if ca_route.get("apm_vpn_auth_course_code"): extended_apm_codes_route.add(ca_route["apm_vpn_auth_course_code"]); found_route=True
                if not found_route and doc_route.get("tag"): extended_apm_codes_route.add(doc_route.get("tag"))
    except PyMongoError as e_ext_route: logger.error(f"APM Route: Error fetching extended: {e_ext_route}")

    plan_items_for_route_apm: List[Dict] = []
    if interim_alloc_collection is not None:
        try:
            # Fetch all confirmed items, as batch_id might be lost if user reloads final review page
            # Or, if JS always sends batch_id, use it:
            query_filter_route = {"status": {"$in": ["student_confirmed", "trainer_confirmed"]}}
            if batch_review_id_req: # If JS sends it, use it to scope.
                 query_filter_route["batch_review_id"] = batch_review_id_req
            plan_cursor_route = interim_alloc_collection.find(query_filter_route)
            plan_items_for_route_apm = list(plan_cursor_route)
        except PyMongoError as e_plan_route:
            logger.error(f"APM Route: Error fetching plan items: {e_plan_route}")
            return jsonify({"commands": ["# ERROR: Could not fetch build plan."], "message": f"DB error: {e_plan_route}", "error": True}), 500
    
    # Call the refactored APM logic
    all_commands, _, apm_errors = _generate_apm_data_for_plan(
        plan_items_for_route_apm,
        current_apm_entries_route,
        extended_apm_codes_route
    )
    
    num_actual_adds = sum(1 for cmd in all_commands if " add " in cmd)
    num_actual_deletes = sum(1 for cmd in all_commands if " del " in cmd)
    num_actual_updates = len(all_commands) - num_actual_adds - num_actual_deletes
    final_message = f"Generated {len(all_commands)} APM commands ({num_actual_deletes} deletions, {num_actual_adds} adds, {num_actual_updates} field updates)."

    if apm_errors:
        final_message = "Errors occurred during APM generation: " + " | ".join(apm_errors) + ". " + final_message
        return jsonify({"commands": ["# Errors occurred. Review messages and generated commands.", *apm_errors, *all_commands], "message": final_message, "error": True}), 200
    if not all_commands: return jsonify({"commands": ["# No APM changes identified."], "message": "No APM changes identified."})
    
    logger.info(f"APM Route: Command Generation Successful. {final_message}")
    return jsonify({"commands": all_commands, "message": final_message})