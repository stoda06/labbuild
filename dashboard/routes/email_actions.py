# In dashboard/routes/email_actions.py

import logging
import json
from flask import Blueprint, request, jsonify
from ..extensions import trainer_email_collection
from ..email_utils import send_allocation_email
from pymongo.errors import PyMongoError

bp = Blueprint('email_actions', __name__)
logger = logging.getLogger('dashboard.routes.email_actions')


@bp.route('/send-trainer-email', methods=['POST'])
def send_trainer_email():
    """
    Handles the AJAX request to send a real allocation email to a specific trainer.
    """
    data = request.json
    trainer_name = data.get('trainer_name')
    course_items_for_email = data.get('course_item_to_email')
    edited_subject = data.get('edited_subject')
    edited_html_body = data.get('edited_html_body')

    if not all([trainer_name, isinstance(course_items_for_email, list), edited_subject, edited_html_body]):
        return jsonify({"status": "error", "message": "Missing required data for sending email."}), 400

    try:
        if trainer_email_collection is None:
            raise PyMongoError("Database collection for trainer emails is unavailable.")

        # Find the active email address for the given trainer.
        trainer_email_doc = trainer_email_collection.find_one({"trainer_name": trainer_name, "active": True})
        if not trainer_email_doc or not trainer_email_doc.get("email_address"):
            return jsonify({"status": "error", "message": f"Active email address not found for trainer '{trainer_name}'."}), 404
        
        to_email_address = trainer_email_doc.get("email_address")
        
        # Call the underlying email utility function.
        success, message = send_allocation_email(
            to_address=to_email_address,
            trainer_name=trainer_name,
            subject=edited_subject,
            course_allocations_data=course_items_for_email,
            html_body_override=edited_html_body,
            is_test=False  # This is a REAL email
        )

        if success:
            return jsonify({"status": "success", "message": message})
        else:
            return jsonify({"status": "error", "message": f"Email sending failed: {message}"}), 500

    except PyMongoError as e:
        logger.error(f"Database error while sending email to '{trainer_name}': {e}")
        return jsonify({"status": "error", "message": "Database error occurred."}), 500
    except Exception as e:
        logger.error(f"Unexpected error while sending email to '{trainer_name}': {e}", exc_info=True)
        return jsonify({"status": "error", "message": "An unexpected server error occurred."}), 500


@bp.route('/send-test-email', methods=['POST'])
def send_test_email():
    """
    Handles the AJAX request to send a TEST allocation email.
    """
    data = request.json
    trainer_name = data.get('trainer_name')
    course_items = data.get('course_item_to_email')
    edited_subject = data.get('edited_subject')
    edited_html_body = data.get('edited_html_body')

    if not all([trainer_name, isinstance(course_items, list), edited_subject, edited_html_body]):
        return jsonify({"status": "error", "message": "Missing required data for sending test email."}), 400

    try:
        # Call the underlying email utility function with the is_test flag set to True.
        # The to_address is ignored when is_test is True, as the function will use the
        # SMTP_TEST_RECIPIENT from the .env file.
        success, message = send_allocation_email(
            to_address="placeholder@example.com", # This is ignored but required by the function signature
            trainer_name=trainer_name,
            subject=edited_subject,
            course_allocations_data=course_items,
            html_body_override=edited_html_body,
            is_test=True  # This sends the email to the test recipient
        )

        if success:
            return jsonify({"status": "success", "message": message})
        else:
            return jsonify({"status": "error", "message": f"Test email failed: {message}"}), 500
    
    except Exception as e:
        logger.error(f"Unexpected error while sending test email for trainer '{trainer_name}': {e}", exc_info=True)
        return jsonify({"status": "error", "message": "An unexpected server error occurred."}), 500