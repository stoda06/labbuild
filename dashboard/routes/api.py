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
    This runs the student pod auto-assignment logic.

    Query Parameters (Optional):
        - vendor (str): Filter upcoming courses by a specific vendor.
        - course_code (str): Filter for a specific Salesforce course code.
    """
    logger.info("API /v1/propose-build-assignments called.")
    # Placeholder for Authentication/Authorization
    # if not is_authenticated(request):
    #     return jsonify({"error": "Unauthorized"}), 401

    # --- 1. Fetch and Prepare Initial Data ---
    build_rules: List[Dict] = []
    all_available_host_names: List[str] = []
    initial_host_capacities_gb: Dict[str, Optional[float]] = {}
    globally_taken_pods_by_vendor: Dict[str, set] = defaultdict(set)
    reusable_pods_by_vendor_host: Dict[str, Dict[str, set]] = \
        defaultdict(lambda: defaultdict(set))
    released_memory_by_host: Dict[str, float] = defaultdict(float)
    host_memory_before_batch_assignment: Dict[str, float] = {}

    try:
        if build_rules_collection is not None:
            build_rules = list(
                build_rules_collection.find().sort("priority", ASCENDING)
            )

        if alloc_collection is not None:
            all_db_allocs_cursor = alloc_collection.find(
                {},
                {"courses.vendor": 1, "courses.pod_details": 1, "_id": 0}
            )
            for tag_doc_db in all_db_allocs_cursor:
                for course_alloc_db in tag_doc_db.get("courses", []):
                    vendor_db = course_alloc_db.get("vendor", "").lower()
                    if not vendor_db:
                        continue
                    for pd_db in course_alloc_db.get("pod_details", []):
                        pod_num = pd_db.get("pod_number")
                        if pod_num is not None:
                            try:
                                globally_taken_pods_by_vendor[vendor_db].add(int(pod_num))
                            except (ValueError, TypeError):
                                pass
                        if vendor_db == 'f5':
                            class_num = pd_db.get("class_number")
                            if class_num is not None:
                                try:
                                    globally_taken_pods_by_vendor[vendor_db].add(int(class_num))
                                except (ValueError, TypeError):
                                    pass
                            for np in pd_db.get("pods", []):
                                n_pod = np.get("pod_number")
                                if n_pod is not None:
                                    try:
                                        globally_taken_pods_by_vendor[vendor_db].add(int(n_pod))
                                    except (ValueError, TypeError):
                                        pass
            
            alloc_collection.update_many(
                {"extend": {"$exists": False}}, {"$set": {"extend": "false"}}
            )
            reusable_cursor = alloc_collection.find(
                {"extend": "false"},
                {"courses.vendor": 1, "courses.course_name": 1,
                 "courses.pod_details.host": 1,
                 "courses.pod_details.pod_number": 1,
                 "courses.pod_details.class_number": 1,
                 "courses.pod_details.pods.pod_number": 1, "_id": 0}
            )
            for tag_doc_reusable in reusable_cursor:
                for course_data_reusable in tag_doc_reusable.get("courses", []):
                    vendor_r = course_data_reusable.get("vendor", "").lower()
                    lb_course_r = course_data_reusable.get("course_name")
                    if not vendor_r or not lb_course_r: continue
                    mem_per_r_pod = _get_memory_for_course(lb_course_r)
                    if mem_per_r_pod <=0: continue
                    for pd_r in course_data_reusable.get("pod_details", []):
                        host_r = pd_r.get("host")
                        if not host_r: continue
                        pod_num_r = pd_r.get("pod_number")
                        if pod_num_r is not None:
                            try: reusable_pods_by_vendor_host[vendor_r][host_r].add(int(pod_num_r)); released_memory_by_host[host_r] += mem_per_r_pod
                            except: pass
                        if vendor_r == 'f5': # F5 specific reuse
                            class_r = pd_r.get("class_number")
                            if class_r is not None: 
                                try: reusable_pods_by_vendor_host[vendor_r][host_r].add(int(class_r)); released_memory_by_host[host_r] += mem_per_r_pod; 
                                except: pass
                            for np_r in pd_r.get("pods",[]): np_val_r = np_r.get("pod_number");
                            if np_val_r is not None: 
                                try: reusable_pods_by_vendor_host[vendor_r][host_r].add(int(np_val_r)); released_memory_by_host[host_r] += (mem_per_r_pod * SUBSEQUENT_POD_MEMORY_FACTOR); 
                                except: pass


        if host_collection is not None:
            hosts_to_check_docs = list(
                host_collection.find({"include_for_build": "true"})
            )
            all_available_host_names = sorted([
                h['host_name'] for h in hosts_to_check_docs if 'host_name' in h
            ])
            if hosts_to_check_docs:
                initial_host_capacities_gb = \
                    get_hosts_available_memory_parallel(hosts_to_check_docs)
            for host_doc in hosts_to_check_docs:
                host_name = host_doc.get("host_name")
                vcenter_cap = initial_host_capacities_gb.get(host_name)
                if host_name and vcenter_cap is not None:
                    releasable = released_memory_by_host.get(host_name, 0.0)
                    host_memory_before_batch_assignment[host_name] = \
                        round(vcenter_cap + releasable, 2)
        
        current_assignment_capacities_gb = \
            host_memory_before_batch_assignment.copy()

    except PyMongoError as e_data_fetch:
        logger.error(f"API: DB error during initial data fetch: {e_data_fetch}", exc_info=True)
        return jsonify({"error": "DB error during data preparation"}), 500
    except Exception as e_prep:
        logger.error(f"API: Unexpected error during data prep: {e_prep}", exc_info=True)
        return jsonify({"error": "Server error during data preparation"}), 500

    # --- 2. Fetch Upcoming Salesforce Courses ---
    try:
        course_configs_list_api: List[Dict] = []
        if course_config_collection is not None:
            course_configs_list_api = list(course_config_collection.find(
                {}, {"course_name": 1, "vendor_shortcode": 1, "_id": 0}
            ))

        upcoming_courses_raw = get_upcoming_courses_data(
            build_rules, course_configs_list_api, all_available_host_names
        )
        if upcoming_courses_raw: # Add check
            logger.debug(f"API: First raw upcoming course data: {upcoming_courses_raw[0]}")
        else:
            logger.debug("API: upcoming_courses_raw is empty or None.")

        if upcoming_courses_raw is None:
            return jsonify({"error": "Failed to get upcoming course data"}), 500
    except Exception as e_sf:
        logger.error(f"API: Error getting upcoming courses: {e_sf}", exc_info=True)
        return jsonify({"error": "Server error fetching upcoming courses"}), 500

    # --- Optional Filtering ---
    filter_vendor_api = request.args.get('vendor', type=str)
    filter_sf_code_api = request.args.get('course_code', type=str)
    filtered_upcoming_courses = []
    if filter_vendor_api or filter_sf_code_api:
        for course_sf in upcoming_courses_raw:
            match_v = (not filter_vendor_api or
                       course_sf.get('vendor', '').lower() == filter_vendor_api.lower())
            match_c = (not filter_sf_code_api or
                       course_sf.get('Course Code', '') == filter_sf_code_api)
            if match_v and match_c:
                filtered_upcoming_courses.append(course_sf)
    else:
        filtered_upcoming_courses = upcoming_courses_raw
    logger.info(f"API: Processing {len(filtered_upcoming_courses)} courses after filters.")

    # --- 3. Perform Student Pod Auto-Assignment ---
    api_results: List[Dict] = []
    for course_in_data in filtered_upcoming_courses:
        sf_code = course_in_data.get(
            'Course Code', f'SF_API_UNKNOWN_{str(ObjectId())[:5]}'
        )
        final_lb_course = course_in_data.get('preselect_labbuild_course')
        vendor = course_in_data.get('vendor', '').lower()
        sf_type = course_in_data.get('Course Type', '')
        sf_pods_req = max(1, int(course_in_data.get('Pods Req.', 1)))
        trainer_name = course_in_data.get('Trainer', 'N/A') # Get from the data
        start_date = course_in_data.get('sf_start_date', 'N/A')
        end_date = course_in_data.get('sf_end_date', 'N/A')


        assigned_map_student = defaultdict(list)
        student_warn_msg: Optional[str] = None
        
        eff_actions: Dict[str, Any] = {}
        rules = _find_all_matching_rules(build_rules, vendor, sf_code, sf_type)
        action_keys = ["set_labbuild_course", "host_priority",
                       "allow_spillover", "set_max_pods", "start_pod_number"]
        for r in rules:
            a = r.get("actions", {})
            for k in action_keys:
                if k not in eff_actions and k in a: eff_actions[k] = a[k]
        
        if "set_labbuild_course" in eff_actions:
            final_lb_course = eff_actions["set_labbuild_course"]
        if not final_lb_course:
            student_warn_msg = "No LabBuild Course determined."
        
        mem_per_pod = 0.0
        if not student_warn_msg:
            mem_per_pod = _get_memory_for_course(final_lb_course) # type: ignore
            if mem_per_pod <= 0:
                student_warn_msg = (student_warn_msg + ". " if student_warn_msg else "") + \
                                   f"Memory for LB course '{final_lb_course}' is 0."
        
        eff_pods_req = sf_pods_req
        if "set_max_pods" in eff_actions and eff_actions["set_max_pods"] is not None:
            try: eff_pods_req = min(sf_pods_req, int(eff_actions["set_max_pods"]))
            except: pass

        # --- Student Pod Auto-Assignment Logic Block ---
        if not student_warn_msg:
            hosts_to_try: List[str] = []
            priority_hosts = eff_actions.get("host_priority", [])
            if isinstance(priority_hosts, list):
                hosts_to_try.extend([h for h in priority_hosts if h in current_assignment_capacities_gb])
            if eff_actions.get("allow_spillover", True):
                hosts_to_try.extend(sorted([
                    h_n for h_n in current_assignment_capacities_gb
                    if h_n not in hosts_to_try
                ]))
            if not hosts_to_try and all_available_host_names:
                hosts_to_try.append(all_available_host_names[0])
                student_warn_msg = (student_warn_msg + ". " if student_warn_msg else "") + \
                                   "No hosts by rule/capacity; using default. VERIFY."

            pods_left_to_assign = eff_pods_req
            if hosts_to_try:
                start_pod_num_cfg = 1
                try: sp_cfg = eff_actions.get("start_pod_number"); start_pod_num_cfg = max(1,int(sp_cfg)) if sp_cfg is not None else 1
                except: pass
                
                temp_caps_course = current_assignment_capacities_gb.copy()

                for host_target in hosts_to_try:
                    if pods_left_to_assign <= 0: break
                    current_host_temp_cap = temp_caps_course.get(host_target, 0.0)
                    
                    reusable_here = sorted(list(
                        reusable_pods_by_vendor_host.get(vendor, {}).get(host_target, set()) -
                        globally_taken_pods_by_vendor.get(vendor, set())
                    ))
                    for r_pod_num in reusable_here:
                        if pods_left_to_assign <= 0: break
                        is_first = not assigned_map_student[host_target]
                        mem_nd = mem_per_pod if is_first else (mem_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR)
                        if current_host_temp_cap >= mem_nd:
                            assigned_map_student[host_target].append(r_pod_num)
                            globally_taken_pods_by_vendor[vendor].add(r_pod_num)
                            current_host_temp_cap -= mem_nd
                            pods_left_to_assign -= 1
                    
                    if pods_left_to_assign > 0:
                        cand_new_pod_num = start_pod_num_cfg
                        while pods_left_to_assign > 0:
                            while cand_new_pod_num in globally_taken_pods_by_vendor.get(vendor, set()):
                                cand_new_pod_num += 1
                            is_first = not assigned_map_student[host_target]
                            mem_nd = mem_per_pod if is_first else (mem_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR)
                            if current_host_temp_cap >= mem_nd:
                                assigned_map_student[host_target].append(cand_new_pod_num)
                                globally_taken_pods_by_vendor[vendor].add(cand_new_pod_num)
                                current_host_temp_cap -= mem_nd
                                pods_left_to_assign -= 1
                                cand_new_pod_num += 1
                            else:
                                student_warn_msg = (student_warn_msg + ". " if student_warn_msg else "") + f"Host {host_target} full."
                                break 
                
                # Update main current_assignment_capacities_gb
                for h_asgn, p_list_asgn in assigned_map_student.items():
                    if p_list_asgn:
                        n_on_h = len(p_list_asgn)
                        mem_cons = (mem_per_pod + (n_on_h - 1) * mem_per_pod * SUBSEQUENT_POD_MEMORY_FACTOR)
                        current_assignment_capacities_gb[h_asgn] = max(0, round(current_assignment_capacities_gb.get(h_asgn, 0.0) - mem_cons, 2))
            
            total_asgn = sum(len(p) for p in assigned_map_student.values())
            if total_asgn < eff_pods_req:
                student_warn_msg = (student_warn_msg + ". " if student_warn_msg else "") + \
                                   f"API Assigned: {total_asgn}/{eff_pods_req} pods."
        # --- End Student Pod Auto-Assignment Logic Block ---

        course_api_result = {
            "sf_course_code": sf_code,
            "final_labbuild_course": final_lb_course,
            "vendor": vendor,
            "trainer": trainer_name,
            "sf_start_date": start_date, # <<< ADDED START DATE
            "sf_end_date": end_date,     # <<< ADDED END DATE
            "effective_pods_req_student": eff_pods_req,
            "memory_gb_one_student_pod": mem_per_pod,
            "proposed_assignments": [],
            "assignment_warning": student_warn_msg
        }
        if assigned_map_student:
            for h_asgn_res, p_list_res in assigned_map_student.items():
                if p_list_res:
                    sorted_p_res = sorted(list(set(p_list_res)))
                    if sorted_p_res:
                        r_s, r_e = sorted_p_res[0], sorted_p_res[0]
                        for i_p_res in range(1, len(sorted_p_res)):
                            if sorted_p_res[i_p_res] == r_e + 1: r_e = sorted_p_res[i_p_res]
                            else: course_api_result["proposed_assignments"].append({"host":h_asgn_res, "start_pod":r_s, "end_pod":r_e}); r_s=r_e=sorted_p_res[i_p_res]
                        course_api_result["proposed_assignments"].append({"host":h_asgn_res, "start_pod":r_s, "end_pod":r_e})
        elif not student_warn_msg: # No assignments but no error means 0 pods needed or no suitable hosts
            course_api_result["assignment_warning"] = (student_warn_msg + ". " if student_warn_msg else "") + "No pods proposed (0 required or no suitable hosts)."
        
        api_results.append(course_api_result)

    logger.info(f"API: Proposed assignments for {len(api_results)} courses.")
    return jsonify(api_results), 200