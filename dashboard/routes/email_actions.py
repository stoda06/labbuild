# dashboard/routes/email_actions.py

import logging
import json
import itertools
from flask import Blueprint, request, jsonify
from collections import defaultdict
from typing import List, Dict, Any
import datetime
from ..extensions import host_collection, trainer_email_collection
from ..email_utils import send_allocation_email
from pymongo.errors import PyMongoError

bp = Blueprint('email_actions', __name__, url_prefix='/email')
logger = logging.getLogger('dashboard.routes.email_actions')


def _generate_email_previews(all_review_items: List[Dict]) -> List[Dict[str, Any]]:
    """
    Server-side logic to generate DATA for trainer email previews.
    It consolidates data, creating multi-line strings for split-host allocations,
    and ensures all numeric values are correctly calculated and passed.
    """
    email_data_list = []
    
    student_items = [
        item for item in all_review_items 
        if item.get("type") == "Student Build" and item.get("sf_trainer_name") and item.get("assignments")
    ]
    if not student_items:
        return []

    try:
        all_hosts_set = {asgn.get("host") for item in student_items for asgn in item.get("assignments", []) if asgn.get("host")}
        hosts_info_map = {h["host_name"]: h for h in host_collection.find({"host_name": {"$in": list(all_hosts_set)}})}
    except PyMongoError as e:
        logger.error(f"Failed to fetch host details for email previews: {e}")
        hosts_info_map = {}

    emails_grouped = defaultdict(list)
    for item in student_items:
        key = f"{item.get('sf_trainer_name', 'N/A')}|{item.get('original_sf_course_code', 'N/A')}"
        emails_grouped[key].append(item)

    for key, items in emails_grouped.items():
        trainer_name, sf_code = key.split('|')
        first_item = items[0]
        
        # --- Group assignments by host ---
        assignments_by_host = defaultdict(list)
        for item in items:
            for asgn in item.get("assignments", []):
                host = asgn.get("host")
                if host:
                    assignments_by_host[host].append(asgn)

        # --- Generate multi-line strings for display ---
        host_lines = []
        vcenter_lines = []
        pod_range_lines = []
        total_pods_for_course = set() # Use a set to handle potential overlaps
        total_ram_for_course = 0.0

        # ** THIS IS THE KEY CORRECTION BLOCK **
        for host, host_assignments in sorted(assignments_by_host.items()):
            host_pod_numbers = {p for asgn in host_assignments for p in range(int(asgn.get('start_pod')), int(asgn.get('end_pod')) + 1) if asgn.get('start_pod') is not None}
            total_pods_for_course.update(host_pod_numbers)
            
            pod_numbers_sorted = sorted(list(host_pod_numbers))
            groups = [list(g) for k, g in itertools.groupby(enumerate(pod_numbers_sorted), lambda x: x[0]-x[1])]
            host_start_end_pod_str = ", ".join([f"{r[0][1]}-{r[-1][1]}" if len(r) > 1 else str(r[0][1]) for r in groups])

            host_info = hosts_info_map.get(host, {})
            num_pods_on_host = len(host_pod_numbers)
            
            # Use the memory from the first item as it's consistent for the course
            memory_per_pod = float(first_item.get('memory_gb_one_pod', 0.0))
            ram_for_this_host = memory_per_pod * num_pods_on_host
            
            # This was missing in the previous version, causing the error.
            # Now we correctly add this host's RAM to the grand total for the course.
            total_ram_for_course += ram_for_this_host 

            host_lines.append(f"{host} ({host_start_end_pod_str})")
            vcenter_lines.append(host_info.get("vcenter", "N/A"))
            pod_range_lines.append(host_start_end_pod_str)
        # ** END OF CORRECTION BLOCK **
            
        virtual_host_display = "\n".join(host_lines)
        vcenter_display = "\n".join(vcenter_lines)
        pod_range_display = "\n".join(pod_range_lines)

        start_date_str, end_date_str = first_item.get("start_date"), first_item.get("end_date")
        date_range_display, end_day_abbr = "N/A", "N/A"
        try:
            start_dt = datetime.datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            end_dt = datetime.datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            start_day, end_day = start_dt.strftime("%a"), end_dt.strftime("%a")
            date_range_display = f"{start_day}-{end_day}" if start_day != end_day else start_day
            end_day_abbr = end_day
        except (ValueError, TypeError): pass

        email_data_list.append({
            "key": key,
            "trainer_name": trainer_name,
            "sf_course_code": sf_code,
            "email_subject": f"Lab Allocation for {sf_code}",
            "payload_items": items,
            "template_data": {
                "original_sf_course_code": sf_code,
                "date_range_display": date_range_display,
                "end_day_abbr": end_day_abbr,
                "primary_location": "Virtual",
                "sf_course_type": first_item.get('sf_course_type', 'N/A'),
                "start_end_pod_str": pod_range_display,
                "username": first_item.get("apm_username", "N/A"),
                "password": first_item.get("apm_password", "UseProvidedPassword"),
                "effective_pods_req": len(total_pods_for_course), # Use the size of the final set
                "final_labbuild_course": first_item.get('labbuild_course', 'N/A'),
                "virtual_host_display": virtual_host_display,
                "primary_vcenter": vcenter_display,
                "total_ram_for_course": total_ram_for_course # Pass the calculated total RAM
            }
        })
    return email_data_list

@bp.route('/prepare-previews', methods=['POST'])
def prepare_email_previews():
    """
    Server-side route to generate all email previews.
    Takes the full review data and returns structured HTML for the modal.
    """
    all_review_items = request.json.get('all_review_items')
    if not isinstance(all_review_items, list):
        return jsonify({"error": "Invalid data format."}), 400
    
    email_previews = _generate_email_previews(all_review_items)
    
    return jsonify({"previews": email_previews})

@bp.route('/send-trainer-email', methods=['POST'])
def send_trainer_email():
    data = request.json
    trainer_name = data.get('trainer_name')
    course_items_for_email = data.get('course_item_to_email')
    edited_subject = data.get('edited_subject')
    edited_html_body = data.get('edited_html_body')

    if not all([trainer_name, isinstance(course_items_for_email, list), course_items_for_email, edited_subject, edited_html_body]):
        return jsonify({"status": "error", "message": "Missing required data for sending email."}), 400

    trainer_email_doc = trainer_email_collection.find_one({"trainer_name": trainer_name, "active": True})
    if not trainer_email_doc or not trainer_email_doc.get("email_address"):
        return jsonify({"status": "error", "message": f"Active email address not found for trainer '{trainer_name}'."}), 404
    to_email_address = trainer_email_doc.get("email_address")
    
    # We pass course_items_for_email only for the plain-text fallback generation.
    success, message = send_allocation_email(
        to_address=to_email_address,
        trainer_name=trainer_name,
        subject=edited_subject,
        course_allocations_data=course_items_for_email, # For plain-text fallback
        html_body_override=edited_html_body, # The user's preview is now the source of truth
        is_test=False
    )

    if success:
        return jsonify({"status": "success", "message": message})
    else:
        return jsonify({"status": "error", "message": f"Email sending failed: {message}"}), 500

@bp.route('/send-test-email', methods=['POST'])
def send_test_email():
    data = request.json
    trainer_name = data.get('trainer_name')
    course_items = data.get('course_item_to_email')
    edited_subject = data.get('edited_subject')
    edited_html_body = data.get('edited_html_body')

    if not all([trainer_name, isinstance(course_items, list), course_items, edited_subject, edited_html_body]):
        return jsonify({"status": "error", "message": "Missing required data for sending test email."}), 400
    
    success, message = send_allocation_email(
        to_address="placeholder@example.com",
        trainer_name=trainer_name,
        subject=edited_subject,
        course_allocations_data=course_items, # For plain-text fallback
        html_body_override=edited_html_body,
        is_test=True
    )

    if success:
        return jsonify({"status": "success", "message": message})
    else:
        return jsonify({"status": "error", "message": f"Test email failed: {message}"}), 500