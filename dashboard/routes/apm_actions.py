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
from ..utils import _create_contiguous_ranges, _generate_random_password

bp = Blueprint('apm_actions', __name__, url_prefix='/apm')
logger = logging.getLogger('dashboard.routes.apm_actions')


def _generate_apm_data_for_plan(
    final_plan_items: List[Dict],
    current_apm_entries: Dict[str, Dict[str, Any]],
    extended_apm_codes: Set[str]
) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, str]], List[str]]:
    """
    Generates APM commands and credentials based on the final build plan.
    Compares the desired state with the current state to generate add, update, and delete commands.
    Includes special logic for Nutanix range expansion and APM versioning.
    """
    logger.info(f"--- APM Data Generation Started for {len(final_plan_items)} plan items ---")
    
    # --- MODIFICATION: Store commands grouped by code ---
    commands_by_code = defaultdict(list)
    # --- END MODIFICATION ---

    apm_errors: List[str] = []
    apm_credentials_map: Dict[str, Dict[str, str]] = {}

    def _determine_apm_version(vendor: str, lb_course_name: str, sf_course_code: str) -> str:
        """
        Determines the correct version string for an APM entry based on specific rules.
        """
        vendor_lower = (vendor or "").lower()
        lb_course_lower = (lb_course_name or "").lower()
        sf_code_lower = (sf_course_code or "").lower()

        if vendor_lower == 'av':
            return "aura"
        
        if vendor_lower == 'cp':
            if "maestro" in lb_course_lower or "maestro" in sf_code_lower:
                return "maestro"
            if "ccse" in lb_course_lower or "ccse" in sf_code_lower:
                return "CCSE-R81.20"
            # Add other Checkpoint rules here if needed
        
        if vendor_lower == 'f5':
            # if "all" in lb_course_lower or "big-ip" in lb_course_lower:
                return "bigip16.1.4"
            # Add other F5 rules here if needed

        if vendor_lower == 'nu':
            return "nutanix-4"

        # Fallback to the LabBuild course name if no specific rule matches
        return lb_course_name or "unknown-version"

    def get_expanded_pods(assignments, vendor):
        expanded_pods = set()
        for asgn in assignments:
            s, e = asgn.get("start_pod"), asgn.get("end_pod")
            if s is not None and e is not None:
                for pod_num in range(int(s), int(e) + 1):
                    if vendor == 'nu':
                        start_logical = (pod_num - 1) * 4 + 1
                        end_logical = pod_num * 4
                        for logical_pod in range(start_logical, end_logical + 1):
                            expanded_pods.add(logical_pod)
                    else:
                        expanded_pods.add(pod_num)
        return expanded_pods
    
    # --- Step 1: Build the desired state from the build plan ---
    desired_apm_state = defaultdict(lambda: {"all_pod_numbers": set()})
    
    for item in final_plan_items:
        # --- REVISED LOGIC to correctly distinguish doc types ---
        is_trainer_group_doc = item.get("sf_trainer_name") == "Trainer Pods"
        
        if is_trainer_group_doc:
            # --- This is a consolidated trainer pod document ---
            key_tp = item.get("sf_course_code") # This is the tag, e.g., "CCSA-R81.20-Trainer Pod"
            if not key_tp: continue

            if "details" not in desired_apm_state[key_tp]:
                
                # --- THIS IS THE FIX: Define related_types before using it ---
                related_types = item.get("related_student_course_types", ["Trainer Setup"])
                filtered_types = [t for t in related_types if t]
                final_course_type = ", ".join(filtered_types) if filtered_types else "Trainer Pod Group"
                # --- END OF FIX ---

                desired_apm_state[key_tp]["details"] = {
                    "trainer": "Trainer Pods",
                    "type": final_course_type, # Use the corrected string
                    "lb_course_name": item.get("final_labbuild_course", "N/A"),
                    "sf_course_code": key_tp,
                    "vendor": item.get("vendor", "xx").lower()
                }
            
            # Add all pods from the trainer assignments
            trainer_pods = get_expanded_pods(item.get("assignments", []), item.get("vendor"))
            desired_apm_state[key_tp]["all_pod_numbers"].update(trainer_pods)

        else:
            # --- This is a student document ---
            sf_code = item.get("original_sf_course_code") or item.get("sf_course_code")
            vendor = (item.get("vendor", "xx") or "xx").lower()
            if not sf_code: continue

            if item.get("assignments"):
                if "details" not in desired_apm_state[sf_code]:
                    desired_apm_state[sf_code]["details"] = {
                        "trainer": item.get("sf_trainer_name", "N/A"),
                        "type": item.get("sf_course_type", "Course"),
                        "lb_course_name": item.get("final_labbuild_course", item.get("labbuild_course", "N/A")),
                        "sf_course_code": sf_code,
                        "vendor": vendor
                    }
                student_pods = get_expanded_pods(item.get("assignments", []), vendor)
                desired_apm_state[sf_code]["all_pod_numbers"].update(student_pods)

    # --- Step 2: Finalize the desired state with passwords, ranges, and correct version ---
    final_desired_state: Dict[str, Dict[str, Any]] = {}
    for code, data in desired_apm_state.items():
        if not data["all_pod_numbers"]: continue
        
        # Call the version helper with the correct source data
        apm_version = _determine_apm_version(
            vendor=data["details"]["vendor"],
            lb_course_name=data["details"]["lb_course_name"],
            sf_course_code=data["details"]["sf_course_code"]
        )
        
        final_desired_state[code] = {
            "vpn_auth_courses": f"{data['details']['trainer']} - {data['details']['type']}"[:250],
            "vpn_auth_range": _create_contiguous_ranges(list(data["all_pod_numbers"])),
            "version": apm_version,
            "password": _generate_random_password(),
            "vendor_short": data["details"]["vendor"]
        }
    
    # --- Step 3: Compare current and desired states to generate commands ---
    apm_code_to_user: Dict[str, str] = {}
    
    for username, existing_details in current_apm_entries.items():
        code = existing_details.get("vpn_auth_course_code")
        if not code:
            # --- MODIFICATION: Group orphaned deletes under a generic key ---
            commands_by_code["__orphaned_deletes__"].append(f"course2 del {username}")
            continue
        
        if code in final_desired_state:
            apm_code_to_user[code] = username
            new = final_desired_state[code]
            # --- MODIFICATION: Append commands to the dictionary ---
            if str(existing_details.get("vpn_auth_range")) != str(new["vpn_auth_range"]): 
                commands_by_code[code].append(f'course2 range {username} "{new["vpn_auth_range"]}"')
            if str(existing_details.get("vpn_auth_version")) != str(new["version"]): 
                commands_by_code[code].append(f'course2 version {username} "{new["version"]}"')
            if str(existing_details.get("vpn_auth_courses")) != str(new["vpn_auth_courses"]): 
                commands_by_code[code].append(f'course2 description {username} "{new["vpn_auth_courses"]}"')
            commands_by_code[code].append(f'course2 password {username} "{new["password"]}"')
        elif code not in extended_apm_codes:
            # --- MODIFICATION: Append delete command to the dictionary ---
            commands_by_code[code].append(f"course2 del {username}")

    used_x_nums = defaultdict(set)
    for uname in current_apm_entries.keys():
        m = re.match(r"lab([a-z0-9]+)-(\d+)", uname.lower())
        if m: used_x_nums[m.group(1)].add(int(m.group(2)))

    for code, details in final_desired_state.items():
        if code not in apm_code_to_user:
            vendor_short, x = details["vendor_short"], 1
            while True:
                if x not in used_x_nums[vendor_short]:
                    new_user = f"lab{vendor_short}-{x}"
                    used_x_nums[vendor_short].add(x)
                    break
                x += 1
            # --- MODIFICATION: Append add command to the dictionary ---
            commands_by_code[code].append(f'course2 add {new_user} "{details["password"]}" "{details["vpn_auth_range"]}" "{details["version"]}" "{details["vpn_auth_courses"]}" "8" "{code}"')
            apm_code_to_user[code] = new_user
            
    # --- Step 4: Populate the final credentials map ---
    for code, details in final_desired_state.items():
        username = apm_code_to_user.get(code)
        if username:
            apm_credentials_map[code] = {"username": username, "password": details["password"]}
    
    # --- MODIFICATION: Prepare final return values ---
    final_commands_by_code = dict(commands_by_code)
    total_commands = sum(len(cmds) for cmds in final_commands_by_code.values())
    logger.info(f"APM Logic: Generated {total_commands} commands. Errors: {apm_errors}")
    
    return final_commands_by_code, apm_credentials_map, apm_errors

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
    
    # --- MODIFICATION: Handle new return signature ---
    commands_by_code, _, apm_errors = _generate_apm_data_for_plan(
        plan_items_for_route_apm,
        current_apm_entries_route,
        extended_apm_codes_route
    )
    
    # Flatten the dictionary of lists into a single list for the JSON response
    all_commands = [cmd for cmd_list in commands_by_code.values() for cmd in cmd_list]
    # --- END MODIFICATION ---
    
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