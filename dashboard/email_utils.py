# dashboard/email_utils.py

import os
import smtplib
import logging
import json # Not strictly needed if html_body_override is used, but good for context
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr # For pretty From header
from jinja2 import Template # Still used for the default template and plain text part
from typing import List, Dict, Optional, Any # For type hinting

logger = logging.getLogger('dashboard.email_utils') # Use a consistent logger name

# Default HTML template structure for PodAllocation emails.
# This template iterates over a list of 'courses', where each 'course' is a dictionary
# containing details for one row in the email table.
DEFAULT_POD_ALLOCATION_HTML_TEMPLATE = """"""
def send_allocation_email(
    to_address: str,
    trainer_name: str,
    subject: str,
    course_allocations_data: List[Dict[str, Any]],
    html_body_override: Optional[str] = None,
    is_test: bool = False
) -> tuple[bool, str]:
    """
    Constructs and sends the pod allocation email.
    It now relies EXCLUSIVELY on html_body_override for the HTML part.
    """
    smtp_host = os.getenv('SMTP_HOST')
    smtp_port_str = os.getenv('SMTP_PORT', '25')
    smtp_sender_email = os.getenv('SMTP_SENDER_EMAIL', 'support@rededucation.com')
    smtp_sender_name = os.getenv('SMTP_SENDER_DISPLAY_NAME', 'LabBuild Notifications')
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')
    smtp_use_tls = os.getenv('SMTP_USE_TLS', 'False').lower() == 'true'

    final_to_address = to_address
    final_subject = subject
    cc_address = "it@rededucation.com"

    if is_test:
        test_recipient = os.getenv('SMTP_TEST_RECIPIENT')
        if not test_recipient:
            msg = "Cannot send test email: SMTP_TEST_RECIPIENT is not set in the .env file."
            logger.error(msg)
            return False, msg
        
        final_to_address = test_recipient
        final_subject = f"[TEST] {subject}"
        cc_address = None
    
    # --- *** THIS IS THE KEY CORRECTION *** ---
    # The function now REQUIRES the HTML body to be provided from the caller.
    if not html_body_override:
        err_msg = "Internal server error: html_body_override was not provided to send_allocation_email."
        logger.error(err_msg)
        return False, err_msg
        
    html_body_to_send = html_body_override
    # --- *** END OF CORRECTION *** ---

    msg_obj = MIMEMultipart('alternative')
    msg_obj['Subject'] = final_subject
    msg_obj['From'] = formataddr((smtp_sender_name, smtp_sender_email))
    msg_obj['To'] = final_to_address
    
    recipients_list = [final_to_address]
    if cc_address:
        msg_obj['Cc'] = cc_address
        recipients_list.append(cc_address)

    # --- Plain-text Fallback (Generated from consolidated data) ---
    plain_text_body = f"Dear {trainer_name},\n\nThis is an automated notification. Please view this email in an HTML-compatible client to see your lab allocation details."
    
    part_plain = MIMEText(plain_text_body, 'plain', 'utf-8')
    part_html = MIMEText(html_body_to_send, 'html', 'utf-8')

    msg_obj.attach(part_plain)
    msg_obj.attach(part_html)
    
    try:
        smtp_port = int(smtp_port_str)
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            if smtp_use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            
            server.sendmail(smtp_sender_email, recipients_list, msg_obj.as_string())
        
        log_message = "Test email sent" if is_test else "Email sent"
        logger.info(f"{log_message} successfully to {final_to_address} for subject: {final_subject}")
        return True, f"{log_message} successfully."
    except Exception as e:
        logger.error(f"General error sending email to {final_to_address}: {e}", exc_info=True)
        return False, f"Error sending email: {e}"