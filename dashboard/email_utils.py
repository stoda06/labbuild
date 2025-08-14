# In dashboard/email_utils.py

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from typing import List, Dict, Optional, Any

logger = logging.getLogger('dashboard.email_utils')

# highlight-start
# --- MODIFICATION: Encapsulate SMTP logic in a dedicated class ---
class EmailSender:
    """
    Handles the configuration and sending of emails via SMTP.
    This class is designed to be used in background tasks.
    """
    def __init__(self):
        """Loads SMTP configuration from environment variables."""
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port_str = os.getenv('SMTP_PORT', '25')
        self.smtp_sender_email = os.getenv('SMTP_SENDER_EMAIL', 'support@rededucation.com')
        self.smtp_sender_name = os.getenv('SMTP_SENDER_DISPLAY_NAME', 'LabBuild Notifications')
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.smtp_use_tls = os.getenv('SMTP_USE_TLS', 'False').lower() == 'true'
        self.test_recipient = os.getenv('SMTP_TEST_RECIPIENT')

    def send(self, to_address: str, subject: str, html_body: str, plain_text_body: str, cc_address: Optional[str] = None) -> None:
        """
        Connects to the SMTP server and sends the composed email.
        Raises exceptions on failure, which will be caught and logged by the background task.
        """
        if not self.smtp_host:
            raise ValueError("SMTP_HOST is not configured in environment variables.")

        msg_obj = MIMEMultipart('alternative')
        msg_obj['Subject'] = subject
        msg_obj['From'] = formataddr((self.smtp_sender_name, self.smtp_sender_email))
        msg_obj['To'] = to_address

        recipients_list = [to_address]
        if cc_address:
            msg_obj['Cc'] = cc_address
            recipients_list.append(cc_address)

        part_plain = MIMEText(plain_text_body, 'plain', 'utf-8')
        part_html = MIMEText(html_body, 'html', 'utf-8')
        msg_obj.attach(part_plain)
        msg_obj.attach(part_html)

        logger.info(f"Attempting to connect to SMTP server at {self.smtp_host}:{self.smtp_port_str}...")
        with smtplib.SMTP(self.smtp_host, int(self.smtp_port_str), timeout=30) as server:
            logger.info("SMTP connection established.")
            if self.smtp_use_tls:
                logger.info("Starting TLS...")
                server.starttls()
            if self.smtp_user and self.smtp_password:
                logger.info("Logging in with SMTP credentials...")
                server.login(self.smtp_user, self.smtp_password)
            
            logger.info(f"Sending email to recipients: {recipients_list}")
            server.sendmail(self.smtp_sender_email, recipients_list, msg_obj.as_string())
        logger.info(f"Successfully sent email with subject '{subject}' to {to_address}.")

# --- MODIFICATION: The original function now acts as a simpler coordinator ---
def send_allocation_email(
    to_address: str,
    trainer_name: str,
    subject: str,
    course_allocations_data: List[Dict[str, Any]], # This is kept for potential future use in the plain-text part
    html_body_override: Optional[str] = None,
    is_test: bool = False
) -> tuple[bool, str]:
    """
    Coordinates the email sending process using the EmailSender class.
    """
    try:
        sender = EmailSender() # Instantiates the class with config from .env

        final_to_address = to_address
        final_subject = subject
        cc_address = "it@rededucation.com"

        if is_test:
            if not sender.test_recipient:
                msg = "Cannot send test email: SMTP_TEST_RECIPIENT is not set in the .env file."
                logger.error(msg)
                return False, msg
            
            final_to_address = sender.test_recipient
            final_subject = f"[TEST] {subject}"
            cc_address = None # Do not CC on test emails
        
        if not html_body_override:
            return False, "Internal server error: HTML body override was not provided."

        plain_text_body = f"Dear {trainer_name},\n\nThis is an automated notification. Please view this email in an HTML-compatible client to see your lab allocation details."

        sender.send(
            to_address=final_to_address,
            subject=final_subject,
            html_body=html_body_override,
            plain_text_body=plain_text_body,
            cc_address=cc_address
        )

        log_message = "Test email sent" if is_test else "Email sent"
        logger.info(f"{log_message} successfully to {final_to_address} for subject: {final_subject}")
        return True, f"{log_message} successfully."

    except (smtplib.SMTPException, ConnectionRefusedError, ValueError) as e:
        logger.error(f"Caught specific exception while sending email: {e}", exc_info=True)
        return False, f"Email server error: {e}"
    except Exception as e:
        logger.critical(f"Caught UNEXPECTED exception during email send process: {e}", exc_info=True)
        return False, f"An unexpected error occurred: {e}"
# highlight-end