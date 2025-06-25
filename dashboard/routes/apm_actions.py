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
) -> Tuple[List[str], Dict[str, Dict[str, str]], List[str]]:
    """
    Generates APM commands and credentials based on the final build plan.
    Compares the desired state with the current state to generate add, update, and delete commands.
    """
    logger.info(f"--- APM Data Generation Started for {len(final_plan_items)} plan items ---")
    apm_commands_delete: List[str] = []
    apm_commands_add_update: List[str] = []
    apm_errors: List[str] = []
    apm_credentials_map: Dict[str, Dict[str, str]] = {}

    # --- Step 1: Build the desired state from the build plan ---
    desired_apm_state = defaultdict(lambda: {"all_pod_numbers": set()})
    for item in final_plan_items:
        sf_code = item.get("original_sf_course_code") or item.get("sf_course_code")
        if not sf_code: continue
        
        # Process student pods
        if item.get("assignments"):
            if "details" not in desired_apm_state[sf_code]:
                desired_apm_state[sf_code]["details"] = {
                    "trainer": item.get("sf_trainer_name", "N/A"),
                    "type": item.get("sf_course_type", "Course"),
                    "version": item.get("final_labbuild_course", item.get("labbuild_course", "N/A")),
                    "vendor": (item.get("vendor", "xx") or "xx").lower()
                }
            for asgn in item.get("assignments", []):
                s, e = asgn.get("start_pod"), asgn.get("end_pod")
                if s is not None and e is not None:
                    [desired_apm_state[sf_code]["all_pod_numbers"].add(p) for p in range(int(s), int(e) + 1)]
        
        # Process trainer pods
        if item.get("trainer_assignment"):
            key_tp = f"{sf_code}-TP"
            if "details" not in desired_apm_state[key_tp]:
                desired_apm_state[key_tp]["details"] = {
                    "trainer": "Trainer Pods",
                    "type": item.get("sf_course_type", "Trainer Setup"),
                    "version": item.get("trainer_labbuild_course", "N/A"),
                    "vendor": (item.get("vendor", "xx") or "xx").lower()
                }
            for asgn_tp in item["trainer_assignment"]:
                s, e = asgn_tp.get("start_pod"), asgn_tp.get("end_pod")
                if s is not None and e is not None:
                    [desired_apm_state[key_tp]["all_pod_numbers"].add(p) for p in range(int(s), int(e) + 1)]

    # --- Step 2: Finalize the desired state with passwords and ranges ---
    final_desired_state: Dict[str, Dict[str, Any]] = {}
    for code, data in desired_apm_state.items():
        if not data["all_pod_numbers"]: continue
        final_desired_state[code] = {
            "vpn_auth_courses": f"{data['details']['trainer']} - {data['details']['type']}"[:250],
            "vpn_auth_range": _create_contiguous_ranges(list(data["all_pod_numbers"])),
            "version": data["details"]["version"],
            "password": _generate_random_password(),
            "vendor_short": data["details"]["vendor"]
        }

    # --- Step 3: Compare current and desired states to generate commands ---
    apm_code_to_user: Dict[str, str] = {} # Map of course codes that have an existing user

    # First pass: find updates and deletions
    for username, existing_details in current_apm_entries.items():
        code = existing_details.get("vpn_auth_course_code")
        if not code:
            apm_commands_delete.append(f"course2 del {username}")
            continue
        
        # If the code is for a course that is in the new plan, it's an update.
        if code in final_desired_state:
            apm_code_to_user[code] = username
            new = final_desired_state[code]
            if str(existing_details.get("vpn_auth_range")) != str(new["vpn_auth_range"]):
                apm_commands_add_update.append(f'course2 range {username} "{new["vpn_auth_range"]}"')
            if str(existing_details.get("vpn_auth_version")) != str(new["version"]):
                apm_commands_add_update.append(f'course2 version {username} "{new["version"]}"')
            if str(existing_details.get("vpn_auth_courses")) != str(new["vpn_auth_courses"]):
                apm_commands_add_update.append(f'course2 description {username} "{new["vpn_auth_courses"]}"')
            # Always update the password for a fresh run
            apm_commands_add_update.append(f'course2 password {username} "{new["password"]}"')
        
        # If the code is not in the new plan AND not marked for extension, delete it.
        elif code not in extended_apm_codes:
            apm_commands_delete.append(f"course2 del {username}")

    # --- HIGHLIGHTED FIX: Second pass to find new courses that need to be added ---
    used_usernames = set(current_apm_entries.keys())
    used_x_nums = defaultdict(set)
    for uname in used_usernames:
        m = re.match(r"lab([a-z0-9]+)-(\d+)", uname.lower())
        if m:
            used_x_nums[m.group(1)].add(int(m.group(2)))

    for code, details in final_desired_state.items():
        # If this course code was not found in the first pass, it's a new entry.
        if code not in apm_code_to_user:
            vendor, x = details["vendor_short"], 1
            while True:
                if x not in used_x_nums[vendor]:
                    new_user = f"lab{vendor}-{x}"
                    used_x_nums[vendor].add(x)
                    break
                x += 1
            
            # Generate the full 'add' command
            apm_commands_add_update.append(
                f'course2 add {new_user} "{details["password"]}" "{details["vpn_auth_range"]}" "{details["version"]}" "{details["vpn_auth_courses"]}" "8" "{code}"'
            )
            # Store the newly generated user for the credentials map
            apm_code_to_user[code] = new_user
    # --- END FIX ---
            
    # --- Step 4: Populate the final credentials map ---
    for code, details in final_desired_state.items():
        username = apm_code_to_user.get(code)
        if username:
            apm_credentials_map[code] = {
                "username": username,
                "password": details["password"]
            }

    all_generated_commands = apm_commands_delete + apm_commands_add_update
    logger.info(f"APM Logic: Generated {len(all_generated_commands)} commands. Errors: {apm_errors}")
    
    return all_generated_commands, apm_credentials_map, apm_errors

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