# vcenter_utils.py
"""vCenter utility functions."""

import logging
import os
from typing import Optional, Dict, Any

from dotenv import load_dotenv

from managers.vcenter import VCenter # Import the VCenter class

# Load environment variables for vCenter credentials
load_dotenv() # Assumes .env is in project root relative to labbuild.py
logger = logging.getLogger('labbuild.vcenter')

VC_USER = os.getenv("VC_USER")
VC_PASS = os.getenv("VC_PASS")
VC_DISABLE_SSL_VERIFY = os.getenv("VC_DISABLE_SSL_VERIFY", "False").lower() in ('true', '1', 't')

if VC_DISABLE_SSL_VERIFY:
    logger.warning(
        "VCENTER SSL CERTIFICATE VERIFICATION IS GLOBALLY DISABLED VIA VC_DISABLE_SSL_VERIFY ENV VAR. "
        "THIS IS INSECURE AND SHOULD ONLY BE USED IN TRUSTED DEVELOPMENT/LAB ENVIRONMENTS."
    )

def get_vcenter_instance(host_details: Dict[str, Any]) -> Optional[VCenter]:
    """Create and connect a VCenter service instance."""
    vc_host = host_details.get("vcenter")
    if not vc_host:
        logger.error(f"vCenter host address missing in host_details for host '{host_details.get('host_name', 'Unknown')}'.")
        return None
    if not VC_USER or not VC_PASS:
        logger.error("vCenter credentials missing from env vars (VC_USER, VC_PASS).")
        return None

    try:
        # Pass the global disable_ssl_verification flag to the VCenter constructor
        service_instance = VCenter(
            vc_host,
            VC_USER,
            VC_PASS,
            port=443, # Default port
            disable_ssl_verification=VC_DISABLE_SSL_VERIFY # Pass the flag
        )
        service_instance.connect() 
        
        if not service_instance.is_connected():
             # Error already logged by VCenter.connect()
             logger.error(f"get_vcenter_instance: Failed to establish connection object for {vc_host}")
             return None
        # logger.info(f"Successfully created VCenter instance for: {vc_host}") # VCenter.connect() logs success
        return service_instance
    except Exception as e:
        # VCenter.connect() should log its own errors.
        logger.error(f"get_vcenter_instance: Unexpected error creating VCenter instance for {vc_host}: {e}", exc_info=True)
        return None