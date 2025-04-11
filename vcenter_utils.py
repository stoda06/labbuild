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
        service_instance = VCenter(vc_host, VC_USER, VC_PASS, 443)
        service_instance.connect() # connect() method handles logging
        if not service_instance.is_connected():
             # Error already logged by connect() or is_connected() potentially
             return None
        # logger.info(f"Successfully connected to vCenter: {vc_host}") # connect() logs this
        return service_instance
    except Exception as e:
        # Error should have been logged by VCenter.connect()
        # logger.error(f"Error connecting to vCenter {vc_host}: {e}", exc_info=True)
        return None