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
DEFAULT_POD_ALLOCATION_HTML_TEMPLATE = """
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <title>{{ subject }}</title>
    <style>
        body {{ font-family: Arial, Helvetica, sans-serif; font-size: 10pt; color: #333333; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 15px; border: 1px solid #cccccc; }}
        th, td {{ border: 1px solid #dddddd; text-align: left; padding: 8px; vertical-align: top; white-space: nowrap; }}
        th {{ background-color: #f0f0f0; font-weight: bold; color: #333333; }}
        p {{ margin-bottom: 10px; line-height: 1.5; }}
        .footer {{ font-size: 9pt; color: #777777; margin-top: 20px; }}
        .text-center {{ text-align: center; }}
        .text-right {{ text-align: right; }}
    </style>
</head>
<body class="email-preview-body">
    <p>Dear {{ trainer_name }},</p>
    
    {% for course in courses %}
    <p>Here are the details for your course allocation ({{ course.original_sf_course_code | default('N/A') }}):</p>
    <table class="email-preview-table">
        <thead>
            <tr>
                <th>Course Code</th><th>Date</th><th>Last Day</th><th>Location</th>
                <th>Course Name</th><th>Start/End Pod</th><th>Username</th><th>Password</th>
                <th>Students</th><th>Vendor Pods</th><th>Version</th>
                <th>Virtual Host</th><th>vCenter</th><th>RAM (GB)</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>{{ course.original_sf_course_code | default('N/A') }}</td>
                <td>{{ course.date_range_display | default('N/A') }}</td>
                <td>{{ course.end_day_abbr | default('N/A') }}</td>
                <td>{{ course.primary_location | default('Virtual') }}</td>
                <td>{{ course.sf_course_type | default('N/A') }}</td>
                <td>{{ course.start_end_pod_str | default('N/A') }}</td>
                <td>{{ course.username | default('N/A') }}</td>
                <td>{{ course.password | default('N/A') }}</td>
                <td class="text-center">{{ course.effective_pods_req | default(0) }}</td>
                <td class="text-center">{{ course.effective_pods_req | default(0) }}</td>
                <td>{{ course.final_labbuild_course | default('N/A') }}</td>
                <td>{{ course.virtual_host_display | default('N/A') }}</td>
                <td>{{ course.primary_vcenter | default('N/A') }}</td>
                <td class="text-right">{{ "%.1f"|format(course.memory_gb_one_pod | default(0.0) | float) }}</td>
            </tr>
        </tbody>
    </table>
    {% endfor %}

    <p>Best regards,<br>Your Training Team</p>
    <p class="footer">This is an automated notification. Please do not reply directly to this email.</p>
</body>
</html>
"""

def send_allocation_email(
    to_address: str,
    trainer_name: str,
    subject: str,
    # `course_allocations_data` is the structured data for the email body parts.
    # For a single course email, this list will contain one dictionary.
    # For the default template, this list is iterated.
    course_allocations_data: List[Dict[str, Any]],
    # `html_body_override` allows passing pre-rendered/edited HTML from the frontend.
    html_body_override: Optional[str] = None
) -> tuple[bool, str]:
    """
    Constructs and sends the pod allocation email to a trainer.
    If `html_body_override` is provided, it's used for the HTML part.
    Otherwise, `DEFAULT_POD_ALLOCATION_HTML_TEMPLATE` is rendered with `course_allocations_data`.
    A plain text part is always generated from `course_allocations_data`.
    """
    smtp_host = os.getenv('SMTP_HOST')
    smtp_port_str = os.getenv('SMTP_PORT', '25')
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')
    smtp_sender_email = os.getenv('SMTP_SENDER_EMAIL', 'support@rededucation.com')
    smtp_sender_name = os.getenv('SMTP_SENDER_DISPLAY_NAME', 'LabBuild Notifications') # Optional display name
    smtp_use_tls_str = os.getenv('SMTP_USE_TLS', 'False') # Default to False if port is 25 or for internal relay

    if not smtp_host or not smtp_port_str or not smtp_sender_email:
        msg = "SMTP configuration (HOST, PORT, SENDER_EMAIL) is incomplete. Cannot send email."
        logger.error(msg)
        return False, msg
    
    try:
        smtp_port = int(smtp_port_str)
        smtp_use_tls = smtp_use_tls_str.lower() == 'true'
    except ValueError:
        msg = "SMTP_PORT must be an integer."
        logger.error(msg)
        return False, msg

    # If TLS is explicitly true, user/pass become essential for login.
    if smtp_use_tls and (not smtp_user or not smtp_password):
        msg = "SMTP_USER and SMTP_PASSWORD are required when SMTP_USE_TLS is true."
        logger.error(msg)
        return False, msg

    # Prepare variables for Jinja2 template (used for default HTML and always for plain text part)
    template_vars = {
        "trainer_name": trainer_name,
        "courses": course_allocations_data, # This list of dicts is iterated in the template
        "subject": subject # Make subject available to template if needed for title
    }

    html_body_to_send: str
    if html_body_override:
        html_body_to_send = html_body_override
        logger.info(f"Using provided HTML body override for email to {to_address} for subject: {subject}")
    else:
        try:
            html_body_to_send = Template(DEFAULT_POD_ALLOCATION_HTML_TEMPLATE).render(**template_vars)
        except Exception as e_render:
            err_msg = f"Error rendering default email template: {e_render}"
            logger.error(err_msg, exc_info=True)
            return False, err_msg

    msg_obj = MIMEMultipart('alternative')
    msg_obj['Subject'] = subject
    msg_obj['From'] = formataddr((smtp_sender_name, smtp_sender_email)) # Use display name and email
    msg_obj['To'] = to_address
    msg_obj['Cc'] = "it@rededucation.com" # Hardcoded CC from your example

    # --- Plain-text Fallback (always generated from structured data) ---
    plain_text_body = f"Dear {trainer_name},\n\nHere are the details for your course allocation(s):\n\n"
    headers_plain = [
        "Course Code", "Date", "Last Day", "Location", "Course Name", "Start/End Pod",
        "Username", "Password", "Students", "Vendor Pods", "Version",
        "Virtual Host", "vCenter", "RAM (GB)"
    ]
    plain_text_body += "\t".join(headers_plain) + "\n"

    for course_data_item in course_allocations_data: # Iterate through the list of course dicts
        # Ensure all keys exist or provide defaults to avoid KeyErrors
        row_values_plain = [
            str(course_data_item.get("original_sf_course_code", 'N/A')),
            str(course_data_item.get("date_range_display", 'N/A')),
            str(course_data_item.get("end_day_abbr", 'N/A')),
            str(course_data_item.get("primary_location", 'N/A')),
            str(course_data_item.get("labbuild_course", 'N/A')),
            str(course_data_item.get("start_end_pod_str", 'N/A')),
            str(course_data_item.get("username", 'N/A')),
            str(course_data_item.get("password", 'UseProvidedPassword')),
            str(course_data_item.get("student_pax", 'N/A')),
            str(course_data_item.get("vendor_pods_count", 0)),
            str(course_data_item.get("labbuild_course", 'N/A')), # Version in template is LabBuild Course
            str(course_data_item.get("primary_host_name", 'N/A')) +
                (f" ({course_data_item.get('start_end_pod_str')})" if course_data_item.get("vendor_pods_count", 0) > 0 else ""),
            str(course_data_item.get("primary_vcenter", 'N/A')),
            f"{float(course_data_item.get('memory_gb_one_pod', 0.0)):.1f}"
        ]
        plain_text_body += "\t".join(row_values_plain) + "\n"
    plain_text_body += "\nBest regards,\nYour Training Team\n\nThis is an automated notification. Please do not reply directly to this email."
    
    part_plain = MIMEText(plain_text_body, 'plain', 'utf-8') # Specify utf-8 encoding
    part_html = MIMEText(html_body_to_send, 'html', 'utf-8') # Specify utf-8 encoding

    msg_obj.attach(part_plain)
    msg_obj.attach(part_html)
    
    try:
        logger.info(f"Attempting to send email to {to_address} via {smtp_host}:{smtp_port} (TLS: {smtp_use_tls})")
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server: # Added timeout
            if smtp_use_tls:
                server.starttls() # For port 587 or if port 25 supports STARTTLS
            
            # Attempt login only if SMTP_USER and SMTP_PASSWORD are set
            if smtp_user and smtp_password:
                logger.info(f"Attempting SMTP login with user: {smtp_user}")
                server.login(smtp_user, smtp_password)
            else:
                logger.info("SMTP_USER and/or SMTP_PASSWORD not set. Proceeding without explicit login.")

            # Send to primary recipient and CC
            recipients_list = [to_address, "it@rededucation.com"]
            server.sendmail(smtp_sender_email, recipients_list, msg_obj.as_string())
        logger.info(f"Email successfully sent to {to_address} (and CC'd to it@rededucation.com) for subject: {subject}")
        return True, "Email sent successfully."
    except smtplib.SMTPAuthenticationError as e:
        err_msg = f"SMTP Authentication Error: {e}. Check SMTP_USER/SMTP_PASSWORD. Ensure they are correct for {smtp_host}."
        logger.error(err_msg, exc_info=True)
        return False, err_msg
    except smtplib.SMTPConnectError as e:
        err_msg = f"SMTP Connection Error: {e}. Check SMTP_HOST/SMTP_PORT ({smtp_host}:{smtp_port})."
        logger.error(err_msg, exc_info=True)
        return False, err_msg
    except smtplib.SMTPServerDisconnected:
        err_msg = "SMTP server disconnected unexpectedly. Please check server status and network."
        logger.error(err_msg, exc_info=True)
        return False, err_msg
    except smtplib.SMTPException as e: # Catch other SMTP specific errors
        err_msg = f"SMTP related error: {e}"
        logger.error(err_msg, exc_info=True)
        return False, err_msg
    except Exception as e: # Catch any other general exceptions
        err_msg = f"General error sending email: {e}"
        logger.error(err_msg, exc_info=True)
        return False, err_msg