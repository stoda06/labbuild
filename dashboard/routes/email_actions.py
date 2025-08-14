# In dashboard/routes/email_actions.py

import logging # MODIFICATION: Import logging
import json
from flask import Blueprint, request, jsonify
from ..extensions import trainer_email_collection
from ..email_utils import send_allocation_email
from pymongo.errors import PyMongoError

bp = Blueprint('email_actions', __name__)
logger = logging.getLogger('dashboard.routes.email_actions') # MODIFICATION: Get logger


@bp.route('/send-trainer-email', methods=['POST'])
def send_trainer_email():
    """
    Handles the AJAX request to send a real allocation email to a specific trainer.
    """
    data = request.json
    trainer_name = data.get('trainer_name')
    
    # highlight-start
    # MODIFICATION: Added detailed logging at the start of the route.
    logger.info(f"Received request to send REAL email for trainer: '{trainer_name}'")
    logger.debug(f"Full payload received: {data}")
    # highlight-end

    course_items_for_email = data.get('course_item_to_email')
    edited_subject = data.get('edited_subject')
    edited_html_body = data.get('edited_html_body')

    if not all([trainer_name, isinstance(course_items_for_email, list), edited_subject, edited_html_body]):
        logger.error("Request failed validation: Missing required data.") # MODIFICATION: Log validation failure
        return jsonify({"status": "error", "message": "Missing required data for sending email."}), 400

    try:
        if trainer_email_collection is None:
            raise PyMongoError("Database collection for trainer emails is unavailable.")

        logger.info(f"Searching for active email for trainer '{trainer_name}'...") # MODIFICATION: Log DB step
        trainer_email_doc = trainer_email_collection.find_one({"trainer_name": trainer_name, "active": True})
        if not trainer_email_doc or not trainer_email_doc.get("email_address"):
            logger.warning(f"Active email address not found for trainer '{trainer_name}'.") # MODIFICATION: Log not found case
            return jsonify({"status": "error", "message": f"Active email address not found for trainer '{trainer_name}'."}), 404
        
        to_email_address = trainer_email_doc.get("email_address")
        logger.info(f"Found email '{to_email_address}'. Proceeding to call email utility.") # MODIFICATION: Log before calling utility
        
        success, message = send_allocation_email(
            to_address=to_email_address,
            trainer_name=trainer_name,
            subject=edited_subject,
            course_allocations_data=course_items_for_email,
            html_body_override=edited_html_body,
            is_test=False
        )

        # highlight-start
        # MODIFICATION: Log the result returned from the email utility.
        logger.info(f"send_allocation_email utility returned: success={success}, message='{message}'")
        # highlight-end

        if success:
            logger.info("Preparing successful JSON response.") # MODIFICATION: Log before sending response
            return jsonify({"status": "success", "message": message})
        else:
            logger.error(f"Email utility failed. Preparing error JSON response.") # MODIFICATION: Log before sending response
            return jsonify({"status": "error", "message": f"Email sending failed: {message}"}), 500

    except PyMongoError as e:
        logger.error(f"Database error while sending email to '{trainer_name}': {e}")
        return jsonify({"status": "error", "message": "Database error occurred."}), 500
    except Exception as e:
        logger.critical(f"Unexpected exception in send_trainer_email route: {e}", exc_info=True) # MODIFICATION: Use critical level
        return jsonify({"status": "error", "message": "An unexpected server error occurred."}), 500


@bp.route('/send-test-email', methods=['POST'])
def send_test_email():
    """
    Handles the AJAX request to send a TEST allocation email.
    """
    data = request.json
    trainer_name = data.get('trainer_name')
    # highlight-start
    # MODIFICATION: Added detailed logging for the test email route as well.
    logger.info(f"Received request to send TEST email for trainer: '{trainer_name}'")
    logger.debug(f"Full payload received: {data}")
    # highlight-end

    course_items = data.get('course_item_to_email')
    edited_subject = data.get('edited_subject')
    edited_html_body = data.get('edited_html_body')

    if not all([trainer_name, isinstance(course_items, list), edited_subject, edited_html_body]):
        logger.error("Test email request failed validation: Missing required data.")
        return jsonify({"status": "error", "message": "Missing required data for sending test email."}), 400

    try:
        logger.info("Proceeding to call email utility for a TEST email.") # MODIFICATION: Log before utility call
        success, message = send_allocation_email(
            to_address="placeholder@example.com",
            trainer_name=trainer_name,
            subject=edited_subject,
            course_allocations_data=course_items,
            html_body_override=edited_html_body,
            is_test=True
        )

        logger.info(f"send_allocation_email (test) utility returned: success={success}, message='{message}'") # MODIFICATION: Log result

        if success:
            logger.info("Preparing successful JSON response for test email.") # MODIFICATION: Log before response
            return jsonify({"status": "success", "message": message})
        else:
            logger.error("Test email utility failed. Preparing error JSON response.") # MODIFICATION: Log before response
            return jsonify({"status": "error", "message": f"Test email failed: {message}"}), 500
    
    except Exception as e:
        logger.critical(f"Unexpected exception in send_test_email route: {e}", exc_info=True) # MODIFICATION: Use critical level
        return jsonify({"status": "error", "message": "An unexpected server error occurred."}), 500