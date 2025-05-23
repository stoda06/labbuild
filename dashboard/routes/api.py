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
    """
    API endpoint to get proposed host and pod assignments for upcoming courses.
    Runs student pod auto-assignment logic based on current DB state.
    """
    logger.info("API /v1/propose-build-assignments called.")
    # Placeholder for Authentication/Authorization
    # ...

    build_rules: List[Dict] = []
    all_available_host_names: List[str] = []
    initial_host_capacities_gb: Dict[str, Optional[float]] = {}
    # For this API call, these represent the current state read from DB
    db_globally_taken_pods_by_vendor: Dict[str, set] = defaultdict(set)
    db_reusable_pods_by_vendor_host: Dict[str, Dict[str, set]] = \
        defaultdict(lambda: defaultdict(set))
    db_released_memory_by_host: Dict[str, float] = defaultdict(float)
    # Effective host capacity considering reusables before this API call's simulation
    host_memory_start_of_api_call: Dict[str, float] = {}

    try:
        # --- 1. Fetch Build Rules ---
        if build_rules_collection is not None:
            build_rules = list(
                build_rules_collection.find().sort("priority", ASCENDING)
            )
        else:
            logger.warning("API: Build rules collection unavailable.")

        # --- 2. Fetch Existing Allocations (for taken and reusable pods) ---
        if alloc_collection is not None:
            # Populate db_globally_taken_pods_by_vendor
            all_db_allocs_cursor = alloc_collection.find({}, {"courses.vendor": 1, "courses.pod_details": 1})
            for tag_doc_db in all_db_allocs_cursor:
                for course_alloc_db in tag_doc_db.get("courses", []):
                    vendor_db = course_alloc_db.get("vendor", "").lower()
                    if not vendor_db: continue
                    for pd_db in course_alloc_db.get("pod_details", []):
                        pod_num = pd_db.get("pod_number")
                        if pod_num is not None: 
                            try: db_globally_taken_pods_by_vendor[vendor_db].add(int(pod_num)); 
                            except: pass
                        if vendor_db == 'f5':
                            class_num = pd_db.get("class_number")
                            if class_num is not None: 
                                try: db_globally_taken_pods_by_vendor[vendor_db].add(int(class_num)); 
                                except: pass
                            for np in pd_db.get("pods",[]): n_pod = np.get("pod_number");
                            if n_pod is not None: 
                                try: db_globally_taken_pods_by_vendor[vendor_db].add(int(n_pod)); 
                                except: pass
            
            # Populate db_reusable_pods_by_vendor_host and db_released_memory_by_host
            alloc_collection.update_many({"extend": {"$exists": False}}, {"$set": {"extend": "false"}})
            # --- CORRECTED PROJECTION for reusable_cursor ---
            reusable_projection = {
                "courses.vendor": 1,
                "courses.course_name": 1,
                "courses.pod_details.host": 1,
                "courses.pod_details.pod_number": 1,
                "courses.pod_details.class_number": 1,
                "courses.pod_details.pods.pod_number": 1,
                "_id": 0 # Usually good to exclude _id unless needed
            }
            reusable_cursor = alloc_collection.find(
                {"extend": "false"}, reusable_projection
            )
            # --- END CORRECTION ---
            for tag_doc_reusable in reusable_cursor:
                for course_data_r in tag_doc_reusable.get("courses", []):
                    vendor_r, lb_course_r = course_data_r.get("vendor","").lower(), course_data_r.get("course_name")
                    if not vendor_r or not lb_course_r: continue
                    mem_r_pod = _get_memory_for_course(lb_course_r)
                    if mem_r_pod <=0: continue
                    for pd_r in course_data_r.get("pod_details", []):
                        host_r = pd_r.get("host");
                        if not host_r: continue
                        pod_r_num = pd_r.get("pod_number")
                        if pod_r_num is not None: 
                            try: db_reusable_pods_by_vendor_host[vendor_r][host_r].add(int(pod_r_num)); db_released_memory_by_host[host_r] += mem_r_pod; 
                            except:pass
                        if vendor_r == 'f5': # F5 specific reuse
                            class_r_num = pd_r.get("class_number")
                            if class_r_num is not None: 
                                try: db_reusable_pods_by_vendor_host[vendor_r][host_r].add(int(class_r_num)); db_released_memory_by_host[host_r] += mem_r_pod; 
                                except: pass
                            for np_r_item in pd_r.get("pods",[]): np_r_val = np_r_item.get("pod_number");
                            if np_r_val is not None: 
                                try: db_reusable_pods_by_vendor_host[vendor_r][host_r].add(int(np_r_val)); db_released_memory_by_host[host_r] += (mem_r_pod * SUBSEQUENT_POD_MEMORY_FACTOR); 
                                except: pass
        else:
            logger.warning("API: alloc_collection is None. Cannot determine taken/reusable pods.")


        # --- 3. Fetch Host Capacities ---
        if host_collection is not None:
            hosts_to_check_docs = list(host_collection.find({"include_for_build": "true"}))
            all_available_host_names = sorted([h['host_name'] for h in hosts_to_check_docs if 'host_name' in h])
            if hosts_to_check_docs:
                initial_host_capacities_gb = get_hosts_available_memory_parallel(hosts_to_check_docs)
            for host_doc in hosts_to_check_docs:
                host_name = host_doc.get("host_name")
                vcenter_cap = initial_host_capacities_gb.get(host_name)
                if host_name and vcenter_cap is not None:
                    releasable = db_released_memory_by_host.get(host_name, 0.0)
                    host_memory_start_of_api_call[host_name] = round(vcenter_cap + releasable, 2)
        else:
            logger.warning("API: host_collection is None. Cannot determine host capacities.")
        
        # This is what the assignment logic will deplete for this API call
        current_assignment_capacities_gb = host_memory_start_of_api_call.copy()
        # Tracks pods assigned during this API call's simulation
        pods_assigned_in_this_api_call: Dict[str, set] = defaultdict(set)

        logger.debug(f"API Call Initial State:")
        logger.debug(f"  DB Globally Taken Pods: {dict(db_globally_taken_pods_by_vendor)}")
        logger.debug(f"  DB Reusable Pods by Host: { {v: dict(h) for v, h in db_reusable_pods_by_vendor_host.items()} }")
        logger.debug(f"  Host Memory at start of API call (incl. DB reusables): {host_memory_start_of_api_call}")


    except PyMongoError as e_data_fetch:
        logger.error(f"API: DB error during initial data fetch: {e_data_fetch}", exc_info=True)
        return jsonify({"error": "DB error during data preparation"}), 500
    except Exception as e_prep:
        logger.error(f"API: Unexpected error during data prep: {e_prep}", exc_info=True)
        return jsonify({"error": "Server error during data preparation"}), 500

    # --- 4. Fetch Upcoming Salesforce Courses ---
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

    # --- 5. Optional Filtering ---
    filter_vendor_api = request.args.get('vendor', type=str)
    filter_sf_code_api = request.args.get('course_code', type=str)
    filtered_upcoming_courses = []
    if filter_vendor_api or filter_sf_code_api:
        for course_sf_in in upcoming_courses_raw:
            match_v = (not filter_vendor_api or
                       course_sf_in.get('vendor', '').lower() == filter_vendor_api.lower())
            match_c = (not filter_sf_code_api or
                       course_sf_in.get('Course Code', '') == filter_sf_code_api)
            if match_v and match_c:
                filtered_upcoming_courses.append(course_sf_in)
    else:
        filtered_upcoming_courses = upcoming_courses_raw
    logger.info(f"API: Processing {len(filtered_upcoming_courses)} courses after filters.")


    # --- 6. Perform Student Pod Auto-Assignment for each filtered course ---
    api_results: List[Dict] = []
    for course_input_data_api in filtered_upcoming_courses:
        sf_code_api = course_input_data_api.get('Course Code', f'SF_API_UNKNOWN_{str(ObjectId())[:5]}')
        final_lb_course_api = course_input_data_api.get('preselect_labbuild_course')
        vendor_api = course_input_data_api.get('vendor', '').lower()
        sf_type_api = course_input_data_api.get('Course Type', '')
        sf_pods_req_orig_api = max(1, int(course_input_data_api.get('Pods Req.', 1)))
        trainer_name_api = course_input_data_api.get('Trainer', 'N/A')
        start_date_api = course_input_data_api.get('sf_start_date', 'N/A') # From apply_build_rules
        end_date_api = course_input_data_api.get('sf_end_date', 'N/A')     # From apply_build_rules
        sf_course_type_api = course_input_data_api.get('sf_course_type', 'N/A')
        sf_pax_count_api = course_input_data_api.get('sf_pax', 0)


        assigned_map_student_api = defaultdict(list)
        student_warn_msg_api: Optional[str] = None
        
        eff_actions_api: Dict[str, Any] = {}
        rules_api = _find_all_matching_rules(build_rules, vendor_api, sf_code_api, sf_type_api)
        action_keys_api = ["set_labbuild_course", "host_priority", "allow_spillover", 
                           "set_max_pods", "start_pod_number"]
        for r_api in rules_api:
            acts_api = r_api.get("actions", {})
            for k_api in action_keys_api:
                if k_api not in eff_actions_api and k_api in acts_api:
                    eff_actions_api[k_api] = acts_api[k_api]
        
        if "set_labbuild_course" in eff_actions_api:
            final_lb_course_api = eff_actions_api["set_labbuild_course"]
        if not final_lb_course_api:
            student_warn_msg_api = "No LabBuild Course determined."
        
        mem_per_pod_api = 0.0
        if not student_warn_msg_api and final_lb_course_api:
            mem_per_pod_api = _get_memory_for_course(final_lb_course_api)
            if mem_per_pod_api <= 0:
                student_warn_msg_api = (student_warn_msg_api + ". " if student_warn_msg_api else "") + \
                                       f"Memory for LB course '{final_lb_course_api}' is 0."
        
        eff_pods_req_api = sf_pods_req_orig_api
        if "set_max_pods" in eff_actions_api and eff_actions_api["set_max_pods"] is not None:
            try: eff_pods_req_api = min(sf_pods_req_orig_api, int(eff_actions_api["set_max_pods"]))
            except: pass

        # --- Actual Auto-Assignment Logic for STUDENT pods within API call scope ---
        if not student_warn_msg_api and mem_per_pod_api > 0 and eff_pods_req_api > 0:
            logger.info(f"API Assign: SF '{sf_code_api}', LB '{final_lb_course_api}', "
                        f"Req: {eff_pods_req_api}, Mem/Pod: {mem_per_pod_api:.2f}GB")

            hosts_to_try_api: List[str] = []
            priority_hosts_rule_api = eff_actions_api.get("host_priority", [])
            if isinstance(priority_hosts_rule_api, list):
                hosts_to_try_api.extend([h for h in priority_hosts_rule_api if h in current_assignment_capacities_gb])
            if eff_actions_api.get("allow_spillover", True):
                hosts_to_try_api.extend(sorted([
                    h_n for h_n in current_assignment_capacities_gb if h_n not in hosts_to_try_api
                ]))
            if not hosts_to_try_api and all_available_host_names:
                 hosts_to_try_api.append(all_available_host_names[0]) # Fallback
                 student_warn_msg_api = (student_warn_msg_api + ". " if student_warn_msg_api else "") + "No hosts by rule/cap; using default. VERIFY."
            elif not hosts_to_try_api:
                 student_warn_msg_api = (student_warn_msg_api + ". " if student_warn_msg_api else "") + "No hosts available."
            
            logger.debug(f"  API Assign: Hosts to try for '{sf_code_api}': {hosts_to_try_api}")
            pods_remaining_api = eff_pods_req_api

            if hosts_to_try_api:
                start_pod_num_cfg_api = 1
                try: sp_val_api = eff_actions_api.get("start_pod_number"); start_pod_num_cfg_api = max(1,int(sp_val_api)) if sp_val_api is not None else 1
                except: pass
                
                temp_host_caps_course_api = current_assignment_capacities_gb.copy()

                for host_idx_api, host_target_api in enumerate(hosts_to_try_api):
                    if pods_remaining_api <= 0: break
                    current_host_temp_c_api = temp_host_caps_course_api.get(host_target_api, 0.0)
                    logger.debug(f"  API Assign: Host {host_idx_api+1}: '{host_target_api}' (TempCap: {current_host_temp_c_api:.2f}GB)")

                    available_reusables_api = sorted(list(
                        db_reusable_pods_by_vendor_host.get(vendor_api, {}).get(host_target_api, set())
                    ))
                    for r_pod_api in available_reusables_api:
                        if pods_remaining_api <= 0: break
                        if r_pod_api in pods_assigned_in_this_api_call.get(vendor_api, set()):
                            continue # Already assigned in this API call to another course

                        is_first = not assigned_map_student_api.get(host_target_api)
                        mem_nd_api = mem_per_pod_api if is_first else (mem_per_pod_api * SUBSEQUENT_POD_MEMORY_FACTOR)
                        if current_host_temp_c_api >= mem_nd_api:
                            assigned_map_student_api[host_target_api].append(r_pod_api)
                            pods_assigned_in_this_api_call[vendor_api].add(r_pod_api)
                            current_host_temp_c_api -= mem_nd_api
                            pods_remaining_api -= 1
                    
                    if pods_remaining_api > 0:
                        cand_new_pod_api = start_pod_num_cfg_api
                        while pods_remaining_api > 0:
                            while cand_new_pod_api in db_globally_taken_pods_by_vendor.get(vendor_api, set()) or \
                                  cand_new_pod_api in pods_assigned_in_this_api_call.get(vendor_api, set()):
                                cand_new_pod_api += 1
                            is_first = not assigned_map_student_api.get(host_target_api)
                            mem_nd_api = mem_per_pod_api if is_first else (mem_per_pod_api * SUBSEQUENT_POD_MEMORY_FACTOR)
                            if current_host_temp_c_api >= mem_nd_api:
                                assigned_map_student_api[host_target_api].append(cand_new_pod_api)
                                pods_assigned_in_this_api_call[vendor_api].add(cand_new_pod_api)
                                current_host_temp_c_api -= mem_nd_api
                                pods_remaining_api -= 1
                                cand_new_pod_api += 1
                            else: student_warn_msg_api = (student_warn_msg_api + ". " if student_warn_msg_api else "") + f"Host {host_target_api} full."; break
                    temp_host_caps_course_api[host_target_api] = current_host_temp_c_api

                if assigned_map_student_api:
                    for h_asgn, p_list_asgn in assigned_map_student_api.items():
                        if p_list_asgn:
                            num_on_h = len(p_list_asgn)
                            mem_cons = (mem_per_pod_api + (num_on_h - 1) * mem_per_pod_api * SUBSEQUENT_POD_MEMORY_FACTOR)
                            current_assignment_capacities_gb[h_asgn] = max(0, round(current_assignment_capacities_gb.get(h_asgn, 0.0) - mem_cons, 2))
            
            total_asgn_api = sum(len(p) for p in assigned_map_student_api.values())
            if total_asgn_api < eff_pods_req_api:
                student_warn_msg_api = (student_warn_msg_api + ". " if student_warn_msg_api else "") + \
                                   f"API Proposed: Assigned {total_asgn_api}/{eff_pods_req_api} pods."
            elif not assigned_map_student_api and eff_pods_req_api > 0:
                 student_warn_msg_api = (student_warn_msg_api + ". " if student_warn_msg_api else "") + "No pods auto-assigned by API."

        course_api_result = {
            "sf_course_code": sf_code_api,
            "final_labbuild_course": final_lb_course_api,
            "vendor": vendor_api,
            "trainer": trainer_name_api,
            "sf_start_date": start_date_api,
            "sf_end_date": end_date_api,
            "sf_course_type": sf_course_type_api,
            "sf_pax": sf_pax_count_api,
            "effective_pods_req_student": eff_pods_req_api,
            "memory_gb_one_student_pod": mem_per_pod_api,
            "proposed_assignments": [],
            "assignment_warning": student_warn_msg_api
        }
        if assigned_map_student_api:
            for h_asgn_res, p_list_res in assigned_map_student_api.items():
                if p_list_res:
                    sorted_p_res = sorted(list(set(p_list_res)))
                    if sorted_p_res:
                        r_s, r_e = sorted_p_res[0], sorted_p_res[0]
                        for i_p_res in range(1, len(sorted_p_res)):
                            if sorted_p_res[i_p_res] == r_e + 1: r_e = sorted_p_res[i_p_res]
                            else: course_api_result["proposed_assignments"].append({"host":h_asgn_res, "start_pod":r_s, "end_pod":r_e}); r_s=r_e=sorted_p_res[i_p_res]
                        course_api_result["proposed_assignments"].append({"host":h_asgn_res, "start_pod":r_s, "end_pod":r_e})
        elif not student_warn_msg_api and eff_pods_req_api > 0: # Only add this if no other warning and pods were expected
            course_api_result["assignment_warning"] = "No pods proposed by API (0 required or no suitable hosts)."
        
        api_results.append(course_api_result)

    logger.info(f"API: Proposed assignments with all details for {len(api_results)} courses.")
    return jsonify(api_results), 200