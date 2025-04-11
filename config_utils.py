# config_utils.py
"""Configuration utility functions."""

import logging
import json
from typing import Optional, Dict, List, Any

import pymongo
from pymongo.errors import PyMongoError

# Import DB utils and constants
from db_utils import mongo_client # Use shared context manager
from constants import DB_NAME, COURSE_CONFIG_COLLECTION, HOST_COLLECTION

logger = logging.getLogger('labbuild.config')

def fetch_and_prepare_course_config(setup_name: str, pod: Optional[int] = None, f5_class: Optional[int] = None) -> Dict[str, Any]:
    """Retrieve and prepare the course configuration from MongoDB."""
    try:
        with mongo_client() as client:
            if not client: raise ConnectionError("Cannot fetch course config: MongoDB connection failed.")
            db = client[DB_NAME]; collection = db[COURSE_CONFIG_COLLECTION]
            logger.debug(f"Fetching course config: '{setup_name}'.")
            config = collection.find_one({"course_name": setup_name})
            if config is None: raise ValueError(f"Course config '{setup_name}' not found.")
            config.pop("_id", None); config_str = json.dumps(config)
            if pod is not None: config_str = config_str.replace("{X}", str(pod))
            if f5_class is not None: config_str = config_str.replace("{Y}", str(f5_class))
            logger.debug(f"Course config '{setup_name}' prepared."); return json.loads(config_str)
    except PyMongoError as e: logger.error(f"MongoDB error fetch course config '{setup_name}': {e}"); raise
    except Exception as e: logger.error(f"Error preparing course config '{setup_name}': {e}", exc_info=True); raise

def extract_components(course_config: Dict[str, Any]) -> List[str]:
    """Extract unique component names from the course configuration."""
    components = set()
    # Ensure robust checking for list and dict types
    for comp in course_config.get("components", []):
        if isinstance(comp, dict) and "component_name" in comp:
            components.add(comp["component_name"])
    for group in course_config.get("groups", []):
        if isinstance(group.get("component"), list):
            for comp in group.get("component", []):
                 if isinstance(comp, dict) and "component_name" in comp:
                     components.add(comp["component_name"])
    component_list = sorted(list(components))
    logger.debug(f"Extracted components: {component_list}"); return component_list

def get_host_by_name(hostname: str) -> Optional[Dict[str, Any]]:
    """Retrieve host details from the 'host' collection."""
    try:
        with mongo_client() as client:
            if not client: logger.error(f"Cannot get host details: DB connection failed."); return None
            db = client[DB_NAME]; collection = db[HOST_COLLECTION]
            logger.debug(f"Fetching details for host '{hostname}'.")
            host = collection.find_one({"host_name": hostname})
            if host: host.pop("_id", None); logger.debug(f"Found host details for '{hostname}'."); return host
            else: logger.error(f"Host '{hostname}' not found."); return None
    except PyMongoError as e: logger.error(f"MongoDB error fetching host '{hostname}': {e}"); return None
    except Exception as e: logger.error(f"Error fetching host '{hostname}': {e}", exc_info=True); return None