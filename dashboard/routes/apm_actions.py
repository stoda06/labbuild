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
from ..utils import _create_contiguous_ranges, _generate_deterministic_password

bp = Blueprint('apm_actions', __name__, url_prefix='/apm')
logger = logging.getLogger('dashboard.routes.apm_actions')


def _generate_apm_data_for_plan(
    final_plan_items: List[Dict],
    current_apm_entries: Dict[str, Dict[str, Any]],
    extended_apm_codes: Set[str]
) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, str]], List[str]]:
    """
    Generates APM commands and credentials based on the final build plan.
    It now handles separate US and AU APM servers based on host assignments.
    """
    logger.info(f"--- APM Data Generation Started for {len(final_plan_items)} plan items ---")
    
    commands_by_code = defaultdict(list)
    apm_errors: List[str] = []
    apm_credentials_map: Dict[str, Dict[str, str]] = {}

    US_APM_HOSTS = {"hotshot", "trypticon"}

    def _determine_apm_version(vendor: str, lb_course_name: str, sf_course_code: str) -> str:
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
        
        if vendor_lower == 'f5':
            return "bigip16.1.4"

        if vendor_lower == 'nu':
            return "nutanix-4"

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

    desired_apm_state = defaultdict(lambda: {"all_pod_numbers": set(), "assigned_hosts": set()})
    
    for item in final_plan_items:
        # --- MODIFICATION: Add logging to inspect the item ---
        logger.debug(f"Processing item for APM state: {item}")
        doc_id_for_debug = item.get("_id")
        if not doc_id_for_debug:
            logger.warning(f"Item with SF code '{item.get('sf_course_code')}' is MISSING '_id'. Password will be random.")
        # --- END MODIFICATION ---

        is_trainer_group_doc = item.get("sf_trainer_name") == "Trainer Pods"
        key = item.get("sf_course_code") if is_trainer_group_doc else (item.get("original_sf_course_code") or item.get("sf_course_code"))
        if not key: continue
        
        vendor = (item.get("vendor", "xx") or "xx").lower()
        assignments = item.get("assignments", [])
        if not assignments: continue

        if "details" not in desired_apm_state[key]:
            related_types = item.get("related_student_course_types", ["Trainer Setup"]) if is_trainer_group_doc else [item.get("sf_course_type", "Course")]
            filtered_types = [t for t in related_types if t]
            final_course_type = ", ".join(filtered_types) if filtered_types else "Course Group"
            desired_apm_state[key]["details"] = {
                "trainer": "Trainer Pods" if is_trainer_group_doc else item.get("sf_trainer_name", "N/A"),
                "type": final_course_type,
                "lb_course_name": item.get("final_labbuild_course", "N/A"),
                "sf_course_code": key,
                "vendor": vendor,
                "doc_id": str(item.get("_id"))
            }
        desired_apm_state[key]["all_pod_numbers"].update(get_expanded_pods(assignments, vendor))
        desired_apm_state[key]["assigned_hosts"].update({a.get("host") for a in assignments if a.get("host")})

    final_desired_state: Dict[str, Dict[str, Any]] = {}
    for code, data in desired_apm_state.items():
        if not data["all_pod_numbers"]: continue
        
        is_us_course = any(host.lower() in US_APM_HOSTS for host in data["assigned_hosts"])
        target_location = 'us' if is_us_course else 'au'
        
        apm_version = _determine_apm_version(
            vendor=data["details"]["vendor"],
            lb_course_name=data["details"]["lb_course_name"],
            sf_course_code=data["details"]["sf_course_code"]
        )
        
        doc_id = data["details"].get("doc_id")
        
        final_desired_state[code] = {
            "vpn_auth_courses": f"{data['details']['trainer']} - {data['details']['type']}"[:250],
            "vpn_auth_range": _create_contiguous_ranges(list(data["all_pod_numbers"])),
            "version": apm_version,
            "password": _generate_deterministic_password(doc_id),
            "vendor_short": data["details"]["vendor"],
            "target_location": target_location,
            "command_prefix": "course2 -u" if target_location == 'us' else "course2"
        }

    apm_code_to_user_map: Dict[str, Dict[str, str]] = defaultdict(dict)
    
    for username, existing_details in current_apm_entries.items():
        code = existing_details.get("vpn_auth_course_code")
        source = existing_details.get("source", "au")
        cmd_prefix_del = "course2 -u" if source == 'us' else "course2"

        if not code:
            commands_by_code["__orphaned_deletes__"].append(f"{cmd_prefix_del} del {username}")
            continue
        
        apm_code_to_user_map[source][code] = username

        if code in final_desired_state and final_desired_state[code]["target_location"] != source:
            if code not in extended_apm_codes: commands_by_code[code].append(f"{cmd_prefix_del} del {username}")
        elif code not in final_desired_state:
            if code not in extended_apm_codes: commands_by_code[code].append(f"{cmd_prefix_del} del {username}")

    used_x_nums_by_loc = {'au': defaultdict(set), 'us': defaultdict(set)}
    for uname, udetails in current_apm_entries.items():
        loc = udetails.get('source', 'au')
        m = re.match(r"lab([a-z0-9]+)-(\d+)", uname.lower())
        if m: used_x_nums_by_loc[loc][m.group(1)].add(int(m.group(2)))

    for code, new_details in final_desired_state.items():
        target_loc, cmd_prefix = new_details["target_location"], new_details["command_prefix"]
        username = apm_code_to_user_map[target_loc].get(code)
        
        if username: 
            existing = current_apm_entries[username]
            if str(existing.get("vpn_auth_range")) != str(new_details["vpn_auth_range"]): commands_by_code[code].append(f'{cmd_prefix} range {username} "{new_details["vpn_auth_range"]}"')
            if str(existing.get("vpn_auth_version")) != str(new_details["version"]): commands_by_code[code].append(f'{cmd_prefix} version {username} "{new_details["version"]}"')
            if str(existing.get("vpn_auth_courses")) != str(new_details["vpn_auth_courses"]): commands_by_code[code].append(f'{cmd_prefix} description {username} "{new_details["vpn_auth_courses"]}"')
            commands_by_code[code].append(f'{cmd_prefix} password {username} "{new_details["password"]}"')
        else: 
            vendor_short, x = new_details["vendor_short"], 1
            while True:
                if x not in used_x_nums_by_loc[target_loc][vendor_short]:
                    new_user = f"lab{vendor_short}-{x}"; used_x_nums_by_loc[target_loc][vendor_short].add(x); break
                x += 1
            commands_by_code[code].append(f'{cmd_prefix} add {new_user} "{new_details["password"]}" "{new_details["vpn_auth_range"]}" "{new_details["version"]}" "{new_details["vpn_auth_courses"]}" "8" "{code}"')
            apm_code_to_user_map[target_loc][code] = new_user
            
    for code, details in final_desired_state.items():
        username = apm_code_to_user_map[details["target_location"]].get(code)
        if username: apm_credentials_map[code] = {"username": username, "password": details["password"]}
            
    final_commands_by_code = dict(commands_by_code)
    total_commands = sum(len(cmds) for cmds in final_commands_by_code.values())
    logger.info(f"APM Logic: Generated {total_commands} commands. Errors: {apm_errors}")
    
    return final_commands_by_code, apm_credentials_map, apm_errors


@bp.route('/generate-commands', methods=['POST'])
def generate_apm_commands_route():
    batch_review_id_req = request.json.get('batch_review_id')
    logger.info(f"--- APM Command Generation Route Called (Batch ID context: {batch_review_id_req}) ---")

    # --- MODIFICATION: Fetch from both AU and US APM servers ---
    current_apm_entries_route: Dict[str, Dict[str, Any]] = {}
    apm_errors_route: List[str] = []

    # Define APM sources
    apm_sources = {
        'au': os.getenv("APM_LIST_URL", "http://connect.rededucation.com:1212/list"),
        'us': os.getenv("APM_LIST_URL_US", "http://connect.rededucation.com:1212/list?us=True")
    }

    for source_key, url in apm_sources.items():
        try:
            response_route = requests.get(url, timeout=15)
            response_route.raise_for_status()
            apm_data = response_route.json()
            
            # Add a 'source' key to each entry to track its origin
            for username, details in apm_data.items():
                if username in current_apm_entries_route:
                    logger.warning(f"APM username conflict: '{username}' exists on multiple APM servers. Prioritizing first-seen entry.")
                else:
                    details['source'] = source_key # 'au' or 'us'
                    current_apm_entries_route[username] = details
        except Exception as e_route:
            error_msg = f"Could not fetch APM entries from {source_key.upper()} server ({url}): {e_route}"
            logger.error(f"APM Route: {error_msg}")
            apm_errors_route.append(error_msg)

    if apm_errors_route and not current_apm_entries_route:
        return jsonify({"commands": ["# ERROR: Could not fetch any APM entries."], "message": " | ".join(apm_errors_route), "error": True}), 500
    # --- END MODIFICATION ---

    extended_apm_codes_route: Set[str] = set()
    try:
        if alloc_collection is not None:
            cursor_route = alloc_collection.find({"extend": "true"}, {"tag": 1, "_id": 0})
            for doc_route in cursor_route:
                if doc_route.get("tag"): extended_apm_codes_route.add(doc_route.get("tag"))
    except PyMongoError as e_ext_route: logger.error(f"APM Route: Error fetching extended tags: {e_ext_route}")

    plan_items_for_route_apm: List[Dict] = []
    if interim_alloc_collection is not None:
        try:
            query_filter_route = {"status": {"$in": ["student_confirmed", "trainer_confirmed"]}}
            if batch_review_id_req:
                 query_filter_route["batch_review_id"] = batch_review_id_req
            plan_cursor_route = interim_alloc_collection.find(query_filter_route)
            plan_items_for_route_apm = list(plan_cursor_route)
        except PyMongoError as e_plan_route:
            logger.error(f"APM Route: Error fetching plan items: {e_plan_route}")
            return jsonify({"commands": ["# ERROR: Could not fetch build plan."], "message": f"DB error: {e_plan_route}", "error": True}), 500
    
    commands_by_code, _, apm_errors = _generate_apm_data_for_plan(
        plan_items_for_route_apm,
        current_apm_entries_route,
        extended_apm_codes_route
    )
    
    apm_errors.extend(apm_errors_route)
    
    all_commands = [cmd for cmd_list in commands_by_code.values() for cmd in cmd_list]
    
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