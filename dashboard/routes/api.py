# dashboard/routes/api.py

import logging
import re
import threading
from flask import Blueprint, request, jsonify
from typing import List, Dict, Optional, Any
from collections import defaultdict

# Import extensions and utils from dashboard package
from ..extensions import db, course_config_collection
from ..extensions import (
    build_rules_collection,
    alloc_collection,
    host_collection,
    course_config_collection,
    # interim_alloc_collection # Not directly used by this API endpoint unless you want to log proposals
)
from ..utils import build_args_from_dict # Helper to build args from JSON
from ..tasks import run_labbuild_task # Task runner
from ..utils import get_hosts_available_memory_parallel # For host capacity
from ..salesforce_utils import get_upcoming_courses_data 
from ..routes.actions import _find_all_matching_rules, _get_memory_for_course
from constants import SUBSEQUENT_POD_MEMORY_FACTOR
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from bson import ObjectId

# Define Blueprint
bp = Blueprint('api', __name__, url_prefix='/api') # Prefix all routes with /api
logger = logging.getLogger('dashboard.routes.api')

@bp.route('/courses') # Route becomes /api/courses
def api_courses():
    """Returns course name suggestions based on query and optional vendor."""
    # Logic moved from original app.py
    query = request.args.get('q', '').strip()
    vendor = request.args.get('vendor', '').strip()
    suggestions = []
    if not query or db is None or course_config_collection is None:
        return jsonify(suggestions)
    try:
        mongo_filter = {'course_name': {'$regex': f'^{re.escape(query)}', '$options': 'i'}}
        if vendor: mongo_filter['vendor_shortcode'] = {'$regex': f'^{re.escape(vendor)}$', '$options': 'i'}
        cursor = course_config_collection.find(mongo_filter, {'course_name': 1, '_id': 0}).limit(15)
        suggestions = [doc['course_name'] for doc in cursor if 'course_name' in doc]
    except Exception as e:
        logger.error(f"Error fetching course suggestions: {e}", exc_info=True)
    return jsonify(suggestions)


@bp.route('/v1/labbuild', methods=['POST']) # Route becomes /api/v1/labbuild
def api_run_labbuild():
    """API endpoint to trigger labbuild commands asynchronously."""
    # Logic moved from original app.py
    # TODO: Add Authentication/Authorization
    if not request.is_json: return jsonify({"error": "Request must be JSON"}), 415
    data = request.get_json();
    if not data: return jsonify({"error": "No JSON data received"}), 400
    logger.info(f"API request received: {data}")

    # Build and validate arguments using the dict helper
    args_list, error_msg = build_args_from_dict(data)

    if error_msg: logger.error(f"API validation failed: {error_msg}"); return jsonify({"error": f"Invalid input: {error_msg}"}), 400
    if not args_list: logger.error("API argument building failed silently."); return jsonify({"error": "Failed to process arguments."}), 500

    # Execute asynchronously
    try:
        thread = threading.Thread(target=run_labbuild_task, args=(args_list,), daemon=True); thread.start()
        logger.info(f"Submitted API task: {' '.join(args_list)}")
        # TODO: Generate and return a unique run_id here for tracking
        # run_id = ...
        return jsonify({ "status": "submitted", "message": "LabBuild command submitted.", "submitted_command": args_list, #"run_id": run_id
                         }), 202
    except Exception as e: logger.error(f"Failed to start API task thread: {e}", exc_info=True); return jsonify({"error": "Failed to start background task."}), 500

@bp.route('/v1/propose-build-assignments', methods=['GET'])
def api_propose_build_assignments():
    logger.info("API /v1/propose-build-assignments called.")
    # --- Authentication Placeholder ---

    build_rules: List[Dict] = []
    all_available_host_names: List[str] = []
    initial_host_capacities_gb: Dict[str, Optional[float]] = {}
    db_locked_pods: Dict[str, set] = defaultdict(set)
    db_reusable_candidates_by_vendor_host: Dict[str, Dict[str, set]] = \
        defaultdict(lambda: defaultdict(set))
    released_memory_by_host: Dict[str, float] = defaultdict(float)
    host_memory_start_of_api_call: Dict[str, float] = {}

    try:
        if build_rules_collection is not None:
            build_rules = list(
                build_rules_collection.find().sort("priority", ASCENDING)
            )

        if alloc_collection is not None:
            alloc_collection.update_many(
                {"extend": {"$exists": False}}, {"$set": {"extend": "false"}}
            )
            db_allocs_cursor = alloc_collection.find({}, {
                "courses.vendor": 1, "courses.course_name": 1,
                "courses.pod_details.host": 1,
                "courses.pod_details.pod_number": 1,
                "courses.pod_details.class_number": 1,
                "courses.pod_details.pods.pod_number": 1,
                "extend": 1, "_id": 0
            })
            _counted_for_release_mem_tracker_api = set()
            for tag_doc_db in db_allocs_cursor:
                is_tag_locked = str(tag_doc_db.get("extend", "false")).lower() == "true"
                for course_alloc_db in tag_doc_db.get("courses", []):
                    vendor = course_alloc_db.get("vendor", "").lower()
                    lb_course = course_alloc_db.get("course_name")
                    if not vendor or not lb_course: continue
                    mem_pod = _get_memory_for_course(lb_course)
                    for pd_db in course_alloc_db.get("pod_details", []):
                        host = pd_db.get("host")
                        ids_in_detail: List[int] = []
                        pod_num = pd_db.get("pod_number")
                        if pod_num is not None: 
                            try: ids_in_detail.append(int(pod_num)); 
                            except: pass
                        if vendor == 'f5':
                            class_num = pd_db.get("class_number")
                            if class_num is not None: 
                                try: ids_in_detail.append(int(class_num)); 
                                except: pass
                            for np_item in pd_db.get("pods", []):
                                np_n_val = np_item.get("pod_number")
                                if np_n_val is not None: 
                                    try: ids_in_detail.append(int(np_n_val)); 
                                    except: pass
                        for item_id in set(ids_in_detail):
                            if is_tag_locked: db_locked_pods[vendor].add(item_id)
                            elif host and mem_pod > 0:
                                db_reusable_candidates_by_vendor_host[vendor][host].add(item_id)
                                mem_tracker_key = f"{vendor}_{host}_{item_id}"
                                if mem_tracker_key not in _counted_for_release_mem_tracker_api:
                                    released_memory_by_host[host] += mem_pod
                                    _counted_for_release_mem_tracker_api.add(mem_tracker_key)
            # if hasattr(intermediate_build_review, '_counted_for_release'): # Cleanup if it was set on function object
            #      delattr(intermediate_build_review, '_counted_for_release')


        if host_collection is not None:
            hosts_docs = list(host_collection.find({"include_for_build": "true"}))
            all_available_host_names = sorted([h['host_name'] for h in hosts_docs if 'host_name' in h])
            if hosts_docs:
                initial_host_capacities_gb = get_hosts_available_memory_parallel(hosts_docs)
            for h_doc in hosts_docs:
                h_name = h_doc.get("host_name")
                vc_cap = initial_host_capacities_gb.get(h_name)
                if h_name and vc_cap is not None:
                    rel_mem = released_memory_by_host.get(h_name, 0.0)
                    host_memory_start_of_api_call[h_name] = round(vc_cap + rel_mem, 2)
        
        current_assignment_capacities_gb = host_memory_start_of_api_call.copy()
        pods_assigned_in_this_api_call: Dict[str, set] = defaultdict(set)

        logger.debug(f"API Call Initial State: DB Locked: {dict(db_locked_pods)}, "
                     f"DB Reusables: { {v: dict(h) for v, h in db_reusable_candidates_by_vendor_host.items()} }, "
                     f"Host Mem Start: {host_memory_start_of_api_call}")

    except PyMongoError as e_data_fetch:
        logger.error(f"API: DB error during data fetch: {e_data_fetch}", exc_info=True)
        return jsonify({"error": "DB error during data preparation"}), 500
    except Exception as e_prep:
        logger.error(f"API: Unexpected error during data prep: {e_prep}", exc_info=True)
        return jsonify({"error": "Server error during data preparation"}), 500

    try:
        course_configs_list_api: List[Dict] = []
        if course_config_collection is not None:
            course_configs_list_api = list(course_config_collection.find(
                {}, {"course_name": 1, "vendor_shortcode": 1, "_id": 0}
            ))
        upcoming_courses_raw = get_upcoming_courses_data(
            build_rules, course_configs_list_api, all_available_host_names
        )
        if upcoming_courses_raw is None:
            return jsonify({"error": "Failed to get upcoming course data"}), 500
    except Exception as e_sf:
        logger.error(f"API: Error getting upcoming courses: {e_sf}", exc_info=True)
        return jsonify({"error": "Server error fetching upcoming courses"}), 500

    filter_vendor_api = request.args.get('vendor', type=str)
    filter_sf_code_api = request.args.get('course_code', type=str)
    filtered_upcoming_courses = upcoming_courses_raw
    if filter_vendor_api or filter_sf_code_api:
        filtered_upcoming_courses = [
            course_sf for course_sf in upcoming_courses_raw
            if (not filter_vendor_api or course_sf.get('vendor', '').lower() == filter_vendor_api.lower()) and \
               (not filter_sf_code_api or course_sf.get('Course Code', '') == filter_sf_code_api)
        ]
    logger.info(f"API: Processing {len(filtered_upcoming_courses)} courses after filters.")

    api_results: List[Dict] = []
    for course_data in filtered_upcoming_courses: # Renamed loop variable
        # Extract all necessary fields from course_data first
        sf_code = course_data.get('Course Code', f'SF_API_UNKN_{str(ObjectId())[:4]}')
        # 'preselect_labbuild_course' comes from apply_build_rules_to_courses
        current_lb_course = course_data.get('preselect_labbuild_course')
        vendor = course_data.get('vendor', '').lower() # From apply_build_rules_to_courses
        # These keys also come from apply_build_rules_to_courses, which got them from process_salesforce_data
        current_sf_type = course_data.get('sf_course_type', 'N/A')
        current_sf_pax = course_data.get('sf_pax', 0)
        current_trainer_name = course_data.get('Trainer', 'N/A')
        current_start_date = course_data.get('sf_start_date', 'N/A')
        current_end_date = course_data.get('sf_end_date', 'N/A')
        # 'Pods Req.' from DataFrame is the initial requirement before specific rules might alter it further here
        current_sf_pods_req = max(1, int(course_data.get('Pods Req.', 1)))


        assigned_map_student_api = defaultdict(list)
        assignment_warn_msg_api: Optional[str] = None
        
        eff_actions_api: Dict[str, Any] = {}
        rules = _find_all_matching_rules(build_rules, vendor, sf_code, current_sf_type)
        action_keys = ["set_labbuild_course", "host_priority", "allow_spillover", 
                       "set_max_pods", "start_pod_number"]
        for r_item in rules:
            acts = r_item.get("actions", {});
            for k_item in action_keys:
                if k_item not in eff_actions_api and k_item in acts: eff_actions_api[k_item] = acts[k_item]
        
        if "set_labbuild_course" in eff_actions_api:
            current_lb_course = eff_actions_api["set_labbuild_course"]
        if not current_lb_course:
            assignment_warn_msg_api = "No LabBuild Course determined by API."
        
        mem_per_pod_api = 0.0
        if not assignment_warn_msg_api and current_lb_course:
            mem_per_pod_api = _get_memory_for_course(current_lb_course)
            if mem_per_pod_api <= 0:
                assignment_warn_msg_api = ((assignment_warn_msg_api + ". ") if assignment_warn_msg_api else "") + \
                                          f"Memory for LB '{current_lb_course}' is 0."
        
        eff_pods_req_api = current_sf_pods_req
        if "set_max_pods" in eff_actions_api and eff_actions_api["set_max_pods"] is not None:
            try: eff_pods_req_api = min(current_sf_pods_req, int(eff_actions_api["set_max_pods"]))
            except: pass

        if not assignment_warn_msg_api and mem_per_pod_api > 0 and eff_pods_req_api > 0:
            hosts_to_try_api: List[str] = []
            priority_hosts_api = eff_actions_api.get("host_priority", [])
            if isinstance(priority_hosts_api, list):
                hosts_to_try_api.extend([h for h in priority_hosts_api if h in current_assignment_capacities_gb])
            if eff_actions_api.get("allow_spillover", True):
                hosts_to_try_api.extend(sorted([
                    h_n for h_n in current_assignment_capacities_gb if h_n not in hosts_to_try_api
                ]))
            if not hosts_to_try_api and all_available_host_names:
                 hosts_to_try_api.append(all_available_host_names[0])
                 assignment_warn_msg_api = (assignment_warn_msg_api + ". " if assignment_warn_msg_api else "") + "No hosts by rule/cap; using default. VERIFY."
            elif not hosts_to_try_api:
                 assignment_warn_msg_api = (assignment_warn_msg_api + ". " if assignment_warn_msg_api else "") + "No hosts available."

            pods_left_api = eff_pods_req_api
            if hosts_to_try_api:
                start_pod_cfg_api = 1
                try: sp_val = eff_actions_api.get("start_pod_number"); start_pod_cfg_api = max(1,int(sp_val)) if sp_val is not None else 1
                except: pass
                
                temp_host_caps_course_api = current_assignment_capacities_gb.copy()

                for host_target_api in hosts_to_try_api:
                    if pods_left_api <= 0: break
                    # current_host_temp_cap_api was defined inside the loop. This is the fix.
                    current_host_temp_cap_api_iter = temp_host_caps_course_api.get(host_target_api, 0.0)
                    
                    # Reusable pods
                    db_reusables_api = sorted(list(db_reusable_candidates_by_vendor_host.get(vendor, {}).get(host_target_api, set())))
                    for r_pod_api in db_reusables_api:
                        if pods_left_api <= 0: break
                        if r_pod_api in pods_assigned_in_this_api_call.get(vendor, set()): continue
                        is_first = not assigned_map_student_api.get(host_target_api)
                        mem_nd = mem_per_pod_api if is_first else (mem_per_pod_api * SUBSEQUENT_POD_MEMORY_FACTOR)
                        if current_host_temp_cap_api_iter >= mem_nd:
                            assigned_map_student_api[host_target_api].append(r_pod_api)
                            pods_assigned_in_this_api_call[vendor].add(r_pod_api)
                            current_host_temp_cap_api_iter -= mem_nd; pods_left_api -= 1
                    
                    # New pods
                    if pods_left_api > 0:
                        cand_new_pod = start_pod_cfg_api
                        while pods_left_api > 0:
                            while cand_new_pod in db_locked_pods.get(vendor, set()) or \
                                  cand_new_pod in pods_assigned_in_this_api_call.get(vendor, set()):
                                cand_new_pod += 1
                            is_first = not assigned_map_student_api.get(host_target_api)
                            mem_nd = mem_per_pod_api if is_first else (mem_per_pod_api * SUBSEQUENT_POD_MEMORY_FACTOR)
                            if current_host_temp_cap_api_iter >= mem_nd:
                                assigned_map_student_api[host_target_api].append(cand_new_pod)
                                pods_assigned_in_this_api_call[vendor].add(cand_new_pod)
                                current_host_temp_cap_api_iter -= mem_nd; pods_left_api -= 1
                                cand_new_pod += 1
                            else: assignment_warn_msg_api = (assignment_warn_msg_api + ". " if assignment_warn_msg_api else "") + f"Host {host_target_api} full."; break
                    temp_host_caps_course_api[host_target_api] = current_host_temp_cap_api_iter # Update temp cap for this host for next iter if course is split

            if assigned_map_student_api:
                for h_asgn, p_list_asgn in assigned_map_student_api.items():
                    if p_list_asgn:
                        num_on_h = len(p_list_asgn)
                        mem_cons = (mem_per_pod_api + (num_on_h - 1) * mem_per_pod_api * SUBSEQUENT_POD_MEMORY_FACTOR) if num_on_h > 0 else 0
                        current_assignment_capacities_gb[h_asgn] = max(0, round(current_assignment_capacities_gb.get(h_asgn, 0.0) - mem_cons, 2))
            
            total_asgn = sum(len(p) for p in assigned_map_student_api.values())
            if total_asgn < eff_pods_req_api:
                assignment_warn_msg_api = (assignment_warn_msg_api + ". " if assignment_warn_msg_api else "") + \
                                   f"API Proposed: Assigned {total_asgn}/{eff_pods_req_api} pods."
        
        course_api_result = {
            "sf_course_code": sf_code,
            "final_labbuild_course": current_lb_course, # Use the final determined LB course
            "vendor": vendor,
            "trainer": current_trainer_name,       # Use current_trainer_name
            "sf_start_date": current_start_date,   # Use current_start_date
            "sf_end_date": current_end_date,       # Use current_end_date
            "sf_course_type": current_sf_type,     # Use current_sf_type
            "sf_pax": current_sf_pax,              # Use current_sf_pax
            "effective_pods_req_student": eff_pods_req_api,
            "memory_gb_one_student_pod": mem_per_pod_api,
            "proposed_assignments": [],
            "assignment_warning": assignment_warn_msg_api
        }
        if assigned_map_student_api:
            for h_asgn_res, p_list_res in assigned_map_student_api.items():
                if p_list_res:
                    sorted_p = sorted(list(set(p_list_res)))
                    if not sorted_p: continue
                    s, e_val = sorted_p[0], sorted_p[0]
                    for i in range(1, len(sorted_p)):
                        if sorted_p[i] == e_val + 1: e_val = sorted_p[i]
                        else: course_api_result["proposed_assignments"].append({"host":h_asgn_res, "start_pod":s, "end_pod":e_val}); s=e_val=sorted_p[i]
                    course_api_result["proposed_assignments"].append({"host":h_asgn_res, "start_pod":s, "end_pod":e_val})
        elif not assignment_warn_msg_api and eff_pods_req_api > 0:
            course_api_result["assignment_warning"] = (assignment_warn_msg_api + ". " if assignment_warn_msg_api else "") + "No pods proposed by API."
        
        api_results.append(course_api_result)

    logger.info(f"API: Proposed assignments with all details for {len(api_results)} courses.")
    logger.debug(f"API Call FINAL State: Pods Assigned This Call: {dict(pods_assigned_in_this_api_call)}, "
                 f"Final Assignment Capacities: {dict(current_assignment_capacities_gb)}")
    return jsonify(api_results), 200