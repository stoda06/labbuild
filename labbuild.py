#!/usr/bin/env python3
"""
Lab Build Management Tool

This tool sets up, manages, and tears down lab environments for various vendors.
It uses MongoDB to store configurations and track pod allocations and relies on 
vendor-specific modules to execute the build and teardown processes.
"""

import argparse
import argcomplete
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, Future # Import Future
from contextlib import contextmanager
from urllib.parse import quote_plus
from typing import Optional, Dict, List, Any, Callable, Tuple, Generator

import pymongo
from pymongo.errors import PyMongoError # Import PyMongoError
from dotenv import load_dotenv
import uuid # Import uuid
import datetime # Import datetime
import threading

# Vendor-specific setup/teardown modules
import labs.setup.avaya as avaya
import labs.setup.checkpoint as checkpoint
import labs.setup.f5 as f5
import labs.setup.palo as palo
import labs.setup.pr as pr
import labs.setup.nu as nu

# Management and Utility modules
import labs.manage.vm_operations as vm_operations
from logger.log_config import setup_logger
from managers.vcenter import VCenter
from monitor.prtg import PRTGManager

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DB_NAME = "labbuild_db"
ALLOCATION_COLLECTION = "currentallocation"
COURSE_CONFIG_COLLECTION = "courseconfig"
HOST_COLLECTION = "host"
PRTG_COLLECTION = "prtg"
OPERATION_LOG_COLLECTION = "operation_logs" # New collection for operation logs

# -----------------------------------------------------------------------------
# Environment Setup: load environment variables and logging
# -----------------------------------------------------------------------------
load_dotenv()
logger = setup_logger()

# MongoDB credentials and URI (Ensure MONGO_USER and MONGO_PASSWORD are in .env)
MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF")) # Fallback for safety
MONGO_HOST = os.getenv("MONGO_HOST")
if not MONGO_HOST:
    logger.critical("MONGO_HOST environment variable not set.")
    sys.exit(1)
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"


# -----------------------------------------------------------------------------
# Operation Logger Class
# -----------------------------------------------------------------------------
class OperationLogger:
    """Logs structured operation and pod status to a single MongoDB document per run."""

    def __init__(self, command: str, args_dict: Dict[str, Any]):
        self.run_id = str(uuid.uuid4())
        self.command = command
        # Basic sanitization: remove sensitive args if needed
        safe_args = args_dict.copy()
        # We already removed 'func' before calling init
        # safe_args.pop('password', None) # Example: remove password if present
        # safe_args.pop('MONGO_PASSWORD', None) # Example
        self.args_dict = safe_args
        self.start_time = datetime.datetime.utcnow()
        self._pod_results: List[Dict[str, Any]] = [] # Buffer for pod statuses
        self._lock = threading.Lock() # Lock for thread-safe appends
        logger.info(f"Operation '{self.command}' starting with run_id: {self.run_id}")
        # Do NOT log to DB here

    def _get_collection(self):
        """Gets the MongoDB collection for operation logs."""
        client = None # Ensure client is defined
        try:
            client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command('ping') # Verify connection
            db = client[DB_NAME]
            return db[OPERATION_LOG_COLLECTION], client
        except PyMongoError as e:
            logger.error(f"[OperationLogger] MongoDB connection failed: {e}")
            if client: client.close()
            return None, None
        except Exception as e:
            logger.error(f"[OperationLogger] Error getting collection: {e}")
            if client: client.close()
            return None, None

    def _write_final_log(self, data: Dict[str, Any]):
        """Internal helper to write the final consolidated log document to MongoDB."""
        collection, client = self._get_collection()
        if collection is None:
            logger.error(f"Operation '{self.command}' (run_id: {self.run_id}) final log FAILED - DB connection issue.")
            return False # Silently fail if DB connection fails

        try:
            collection.insert_one(data)
            return True
        except PyMongoError as e:
            logger.error(f"[OperationLogger] Failed to write final log to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"[OperationLogger] Unexpected error writing final log: {e}")
            return False
        finally:
            if client:
                client.close()

    # Removed _log_operation_start method

    def log_pod_status(self, pod_id: Any, status: str, step: Optional[str] = None, error: Optional[str] = None, class_id: Optional[Any] = None):
        """Stores the success or failure status of a single pod/class internally."""
        pod_data = {
            "timestamp": datetime.datetime.utcnow(), # Log time of pod completion
            "identifier": str(pod_id),
            "status": status,
            "failed_step": step,
            "error_message": error,
        }
        if class_id is not None:
            pod_data["class_identifier"] = str(class_id)

        # Append to internal list using a lock for thread safety
        with self._lock:
            self._pod_results.append(pod_data)
        # Log to console/file logger immediately
        if status == "failed":
             logger.error(f"Pod/Class '{pod_id}' failed at step '{step}': {error}")
        # else:
        #     logger.info(f"Pod/Class '{pod_id}' completed successfully.") # Optional success log

    def finalize(self, overall_status: str, success_count: int, failure_count: int):
        """Constructs the final log document and writes it to MongoDB."""
        end_time = datetime.datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        # Construct the single consolidated document
        final_log_document = {
            "run_id": self.run_id,
            "command": self.command,
            "args": self.args_dict,
            "start_time": self.start_time,
            "end_time": end_time,
            "duration_seconds": round(duration, 2),
            "overall_status": overall_status,
            "summary": { # Nest summary counts
                 "success_count": success_count,
                 "failure_count": failure_count,
            },
            "pod_statuses": self._pod_results # Include the buffered pod results
        }

        if self._write_final_log(final_log_document):
            logger.info(f"Operation '{self.command}' (run_id: {self.run_id}) finished with status: {overall_status}. Log saved.")
        else:
            logger.error(f"Operation '{self.command}' (run_id: {self.run_id}) summary log to DB FAILED.")

# -----------------------------------------------------------------------------
# Database Access
# -----------------------------------------------------------------------------
@contextmanager
# CORRECTED TYPE HINT HERE using typing.Generator:
def mongo_client() -> Generator[pymongo.MongoClient, None, None]:
    """Context manager for MongoDB client. Yields the client."""
    client: Optional[pymongo.MongoClient] = None # Hint client variable type
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        # Verify connection
        client.admin.command('ping')
        logger.debug("Opened MongoDB connection.")
        yield client # Yield the client instance
    except pymongo.errors.ConnectionFailure as e:
        logger.critical(f"MongoDB connection failed: {e}")
        # Don't raise here, allow fallback or log handlers to manage
        # raise ConnectionError(f"MongoDB connection failed: {e}") from e
    except Exception as e:
        logger.error(f"Error during MongoDB client context management: {e}", exc_info=True)
        # raise # Re-raise unexpected errors
    finally:
        if client:
            client.close()
            logger.debug("Closed MongoDB connection.")


def update_database(data: Dict[str, Any]):
    """Update or insert an entry in the allocation collection."""
    try:
        with mongo_client() as client:
            if not client: # Handle connection failure from context manager
                 logger.error("Cannot update database: MongoDB connection failed.")
                 return

            db = client[DB_NAME]
            collection = db[ALLOCATION_COLLECTION]
            tag = data["tag"]
            course_name = data["course_name"]
            vendor = data.get("vendor")
            pod_details_list = data["pod_details"] # Renamed from pod_details for clarity

            logger.debug(f"Updating database for tag '{tag}', course '{course_name}'.")
            tag_entry = collection.find_one({"tag": tag})

            if tag_entry:
                # Find the course within the tag entry
                course_index = -1
                for i, course in enumerate(tag_entry.get("courses", [])):
                    if course.get("course_name") == course_name:
                        course_index = i
                        break

                if course_index != -1: # Course exists
                    existing_course = tag_entry["courses"][course_index]
                    existing_course["vendor"] = vendor or existing_course.get("vendor") # Update vendor if provided

                    # Update or add pods within the course
                    # Ensure keys are strings for reliable matching
                    existing_pods = {str(pod.get("pod_number", pod.get("class_number", "None"))): pod
                                     for pod in existing_course.get("pod_details", [])}


                    for new_pod_detail in pod_details_list:
                         pod_key = str(new_pod_detail.get("pod_number", new_pod_detail.get("class_number", "None")))
                         if pod_key in existing_pods:
                             existing_pods[pod_key].update(new_pod_detail)
                             logger.debug(f"Updated pod/class {pod_key} for course '{course_name}'.")
                         else:
                             # Ensure pod_details list exists before appending
                             existing_course.setdefault("pod_details", []).append(new_pod_detail)
                             logger.debug(f"Added new pod/class {pod_key} for course '{course_name}'.")
                             # Add the new pod to existing_pods dict as well for consistency before overwrite
                             existing_pods[pod_key] = new_pod_detail


                    # Overwrite pod_details with the potentially updated list
                    existing_course["pod_details"] = list(existing_pods.values())
                else: # Course does not exist, add it
                    tag_entry.setdefault("courses", []).append({
                        "course_name": course_name,
                        "vendor": vendor,
                        "pod_details": pod_details_list
                    })
                    logger.debug(f"Added new course '{course_name}' to tag '{tag}'.")

                # Update the entire tag entry
                collection.update_one({"tag": tag}, {"$set": {"courses": tag_entry["courses"]}})

            else: # Tag does not exist, insert new entry
                new_entry = {
                    "tag": tag,
                    "courses": [{
                        "course_name": course_name,
                        "vendor": vendor,
                        "pod_details": pod_details_list
                    }]
                }
                collection.insert_one(new_entry)
                logger.debug(f"Inserted new tag entry '{tag}'.")

            logger.info(f"Database updated for tag '{tag}' and course '{course_name}'.")

    except PyMongoError as e:
         logger.error(f"Database update error (PyMongoError): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Database update error: {e}", exc_info=True)


def delete_from_database(tag: str, course_name: Optional[str] = None, pod_number: Optional[int] = None, class_number: Optional[int] = None):
    """Delete an entry, course, or pod from the allocation collection."""
    try:
        with mongo_client() as client:
            if not client: # Handle connection failure
                 logger.error("Cannot delete from database: MongoDB connection failed.")
                 return

            db = client[DB_NAME]
            collection = db[ALLOCATION_COLLECTION]
            logger.debug(f"Attempting delete from DB: tag='{tag}', course='{course_name}', pod='{pod_number}', class='{class_number}'")

            tag_entry = collection.find_one({"tag": tag})
            if not tag_entry:
                logger.warning(f"Tag '{tag}' not found for deletion.")
                return

            # Case 1: Delete entire tag
            if not course_name and pod_number is None and class_number is None:
                collection.delete_one({"tag": tag})
                logger.info(f"Deleted entire tag '{tag}'.")
                return

            courses = tag_entry.get("courses", [])
            updated_courses = []
            modified = False

            for course in courses:
                if course.get("course_name") == course_name:
                    # Case 2: Delete entire course
                    if pod_number is None and class_number is None:
                        modified = True
                        logger.info(f"Deleting course '{course_name}' under tag '{tag}'.")
                        continue # Skip adding this course to updated_courses

                    # Case 3: Delete specific pod/class within a course
                    pods = course.get("pod_details", [])
                    updated_pods = []
                    for pod in pods:
                        # Determine if the current pod matches the deletion criteria
                        match = False
                        # Match F5 class first if class_number is given and pod_number is None
                        if class_number is not None and pod_number is None and pod.get("class_number") == class_number:
                            match = True
                        # Match specific pod (works for non-F5 and F5 pods within a class)
                        elif pod_number is not None and pod.get("pod_number") == pod_number:
                            # If F5, also ensure class matches if provided
                            if class_number is None or pod.get("class_number") == class_number:
                                match = True
                        # Handle F5 nested pods deletion
                        elif class_number is not None and pod_number is not None and pod.get("class_number") == class_number:
                             nested_pods = pod.get("pods", [])
                             updated_nested_pods = [np for np in nested_pods if np.get("pod_number") != pod_number]
                             if len(updated_nested_pods) < len(nested_pods):
                                 pod["pods"] = updated_nested_pods
                                 modified = True
                                 logger.info(f"Deleting nested pod '{pod_number}' from class '{class_number}' under course '{course_name}' tag '{tag}'.")
                                 # Keep the class entry with remaining pods
                                 updated_pods.append(pod)
                             else:
                                 updated_pods.append(pod) # No nested pod matched
                             continue # Skip default match check below


                        if not match:
                            updated_pods.append(pod)
                        else:
                            modified = True
                            # Refine log message
                            log_id = f"class {class_number}" if pod_number is None else f"pod {pod_number}"
                            if class_number is not None and pod_number is not None:
                                log_id += f" in class {class_number}"
                            logger.info(f"Deleting {log_id} from course '{course_name}' under tag '{tag}'.")


                    if updated_pods: # Keep course if it still has pods
                         course["pod_details"] = updated_pods
                         updated_courses.append(course)
                    elif "pods" in course and course["pods"]: # Keep F5 class if it has nested pods left
                         updated_courses.append(course)
                    else: # Course becomes empty, mark for removal
                        modified = True
                        logger.info(f"Course '{course_name}' became empty after pod/class deletion.")
                else:
                    updated_courses.append(course) # Keep other courses

            # Update or delete the tag entry
            if modified:
                if updated_courses:
                    collection.update_one({"tag": tag}, {"$set": {"courses": updated_courses}})
                    logger.info(f"Updated tag '{tag}' after deletion.")
                else:
                    collection.delete_one({"tag": tag})
                    logger.info(f"Deleted tag '{tag}' as it became empty.")
            else:
                 logger.warning(f"No matching course/pod/class found for deletion under tag '{tag}'.")

    except PyMongoError as e:
        logger.error(f"Error during database deletion (PyMongoError): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error during database deletion: {e}", exc_info=True)


def get_prtg_url(tag: str, course_name: str, pod_number: Optional[int] = None, class_number: Optional[int] = None) -> Optional[str]:
    """Retrieve the PRTG monitor URL."""
    try:
        with mongo_client() as client:
            if not client:
                logger.error("Cannot get PRTG URL: MongoDB connection failed.")
                return None

            db = client[DB_NAME]
            collection = db[ALLOCATION_COLLECTION]
            logger.debug(f"Searching PRTG URL: tag='{tag}', course='{course_name}', pod='{pod_number}', class='{class_number}'")
            tag_entry = collection.find_one({"tag": tag})
            if not tag_entry:
                logger.warning(f"Tag '{tag}' not found for PRTG URL search.")
                return None

            for course in tag_entry.get("courses", []):
                if course.get("course_name") == course_name:
                    for pod_detail in course.get("pod_details", []):
                        # F5 class level URL (pod_number is None)
                        if class_number is not None and pod_detail.get("class_number") == class_number and pod_number is None:
                             # Check if the URL is directly on the class entry
                             if "prtg_url" in pod_detail:
                                 logger.debug(f"Found PRTG URL for class '{class_number}'.")
                                 return pod_detail.get("prtg_url")
                             else: # Might be nested from older structure? Unlikely now.
                                 pass # No class-level URL found here

                        # Pod level URL (for non-F5)
                        elif pod_number is not None and class_number is None and pod_detail.get("pod_number") == pod_number:
                             logger.debug(f"Found PRTG URL for pod '{pod_number}'.")
                             return pod_detail.get("prtg_url")

                        # F5 pod level URL (check within nested pods)
                        elif class_number is not None and pod_number is not None and pod_detail.get("class_number") == class_number:
                            for nested_pod in pod_detail.get("pods", []):
                                if nested_pod.get("pod_number") == pod_number:
                                    logger.debug(f"Found PRTG URL for F5 pod '{pod_number}' within class '{class_number}'.")
                                    return nested_pod.get("prtg_url")

            logger.warning(f"No PRTG URL found for specified criteria: tag='{tag}', course='{course_name}', pod='{pod_number}', class='{class_number}'.")
            return None
    except PyMongoError as e:
        logger.error(f"Error retrieving PRTG URL (PyMongoError): {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error retrieving PRTG URL: {e}", exc_info=True)
        return None

# -----------------------------------------------------------------------------
# Configuration & Host Helpers
# -----------------------------------------------------------------------------
def fetch_and_prepare_course_config(setup_name: str, pod: Optional[int] = None, f5_class: Optional[int] = None) -> Dict[str, Any]:
    """Retrieve and prepare the course configuration from MongoDB."""
    try:
        with mongo_client() as client:
            if not client:
                 raise ConnectionError("Cannot fetch course config: MongoDB connection failed.")

            db = client[DB_NAME]
            collection = db[COURSE_CONFIG_COLLECTION]
            logger.debug(f"Fetching course configuration for '{setup_name}'.")
            config = collection.find_one({"course_name": setup_name})
            if config is None:
                logger.error(f"Course configuration '{setup_name}' not found.")
                raise ValueError(f"Course configuration '{setup_name}' not found.") # Raise error instead of sys.exit

            config.pop("_id", None)
            config_str = json.dumps(config)
            if pod is not None:
                config_str = config_str.replace("{X}", str(pod))
            if f5_class is not None:
                config_str = config_str.replace("{Y}", str(f5_class))

            logger.debug(f"Course configuration for '{setup_name}' prepared successfully.")
            return json.loads(config_str)
    except pymongo.errors.PyMongoError as e:
        logger.error(f"MongoDB error fetching course configuration '{setup_name}': {e}")
        raise  # Re-raise MongoDB errors
    except Exception as e:
        logger.error(f"Error preparing course configuration '{setup_name}': {e}", exc_info=True)
        raise # Re-raise other errors


def extract_components(course_config: Dict[str, Any]) -> List[str]:
    """Extract unique component names from the course configuration."""
    components = set()
    for comp in course_config.get("components", []):
        components.add(comp["component_name"])
    for group in course_config.get("groups", []):
        # Check if 'component' key exists and is iterable
        if isinstance(group.get("component"), list):
            for comp in group.get("component", []):
                 # Check if comp is a dict and has the key
                 if isinstance(comp, dict) and "component_name" in comp:
                     components.add(comp["component_name"])
    component_list = sorted(list(components))
    logger.debug(f"Extracted components: {component_list}")
    return component_list


def get_host_by_name(hostname: str) -> Optional[Dict[str, Any]]:
    """Retrieve host details from the 'host' collection."""
    try:
        with mongo_client() as client:
            if not client:
                 logger.error(f"Cannot get host details for '{hostname}': MongoDB connection failed.")
                 return None

            db = client[DB_NAME]
            collection = db[HOST_COLLECTION]
            logger.debug(f"Fetching details for host '{hostname}'.")
            host = collection.find_one({"host_name": hostname})
            if host:
                host.pop("_id", None)
                logger.debug(f"Found host details for '{hostname}'.")
                return host
            else:
                logger.error(f"Host '{hostname}' not found.")
                return None
    except pymongo.errors.PyMongoError as e:
        logger.error(f"MongoDB error fetching host details for '{hostname}': {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching host details for '{hostname}': {e}", exc_info=True)
        return None

# -----------------------------------------------------------------------------
# vCenter Helper
# -----------------------------------------------------------------------------
def get_vcenter_instance(host_details: Dict[str, Any]) -> Optional[VCenter]:
    """Create and connect a VCenter service instance."""
    vc_host = host_details.get("vcenter")
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")

    if not all([vc_host, vc_user, vc_password]):
        logger.error("vCenter credentials (host, user, pass) missing in host details or environment variables.")
        return None

    try:
        service_instance = VCenter(vc_host, vc_user, vc_password, 443)
        service_instance.connect()
        if not service_instance.is_connected():
             logger.error(f"Failed to connect to vCenter at {vc_host}")
             return None
        logger.info(f"Successfully connected to vCenter: {vc_host}")
        return service_instance
    except Exception as e:
        logger.error(f"Error connecting to vCenter {vc_host}: {e}", exc_info=True)
        return None


# -----------------------------------------------------------------------------
# Vendor Build/Teardown Function Maps
# -----------------------------------------------------------------------------

# Map vendor shortcode to the primary build function
VENDOR_SETUP_MAP: Dict[str, Callable] = {
    "cp": checkpoint.build_cp_pod,
    "pa": palo.build_1110_pod, # Default Palo build, specific logic inside vendor_setup
    "f5": f5.build_pod, # F5 handled specially in vendor_setup
    "av": avaya.build_aura_pod, # Default Avaya build, specific logic inside vendor_setup
    "pr": pr.build_pr_pod,
    "nu": nu.build_nu_pod,
}

# Map vendor shortcode to the primary teardown function
VENDOR_TEARDOWN_MAP: Dict[str, Callable] = {
    "cp": checkpoint.teardown_pod,
    "pa": palo.teardown_1110, # Default Palo teardown, specific logic inside vendor_teardown
    "f5": f5.teardown_class, # F5 handled specially in vendor_teardown
    "av": avaya.teardown_aura, # Default Avaya teardown, specific logic inside vendor_teardown
    "pr": pr.teardown_pr_pod,
    "nu": nu.teardown_nu_pod,
}

# Map vendor shortcode to the add_monitor function
VENDOR_MONITOR_MAP: Dict[str, Callable] = {
    "cp": checkpoint.add_monitor,
    "pa": palo.add_monitor,
    "f5": f5.add_monitor,
    "av": PRTGManager.add_monitor, # Default for Avaya
    "pr": PRTGManager.add_monitor, # Default for PR
    "nu": PRTGManager.add_monitor, # Default for Nu
    "ot": PRTGManager.add_monitor, # Default 'other'
}

# -----------------------------------------------------------------------------
# Monitor & Database Update Helper
# -----------------------------------------------------------------------------
def update_monitor_and_database(config: Dict[str, Any], args: argparse.Namespace, data: Dict[str, Any], extra_details: Optional[Dict[str, Any]] = None):
    """Add PRTG monitor and update database allocation."""
    vendor_shortcode = config.get("vendor_shortcode", "ot") # Default to 'other'
    add_monitor_func = VENDOR_MONITOR_MAP.get(vendor_shortcode, PRTGManager.add_monitor)

    prtg_url = None
    try:
        with mongo_client() as client:
            if not client:
                 logger.error("Cannot add monitor: MongoDB connection failed.")
                 # Decide how to handle this - skip monitor or fail?
                 # For now, we'll continue without a URL.
            else:
                # Special case for Checkpoint's add_monitor signature
                if vendor_shortcode == "cp":
                    prtg_url = add_monitor_func(config, client, getattr(args, 'prtg_server', None))
                else:
                    prtg_url = add_monitor_func(config, client)

        if prtg_url:
            logger.debug(f"PRTG monitor added/updated, URL: {prtg_url}")
        else:
            logger.warning("Failed to add/update PRTG monitor or skipped due to DB connection issue.")
            # Continue without URL if monitor addition failed or skipped

    except Exception as e:
        logger.error(f"Error adding PRTG monitor: {e}", exc_info=True)
        # Continue without URL if monitor addition failed

    # Prepare the record for the database
    record = {
        "host": args.host,
        "poweron": "True", # Assume powered on after successful build
        "prtg_url": prtg_url # Store URL even if None
    }
    if extra_details:
        record.update(extra_details)

    # F5 specific structure: class contains pods
    if vendor_shortcode == "f5":
        class_number = config.get("class_number", "unknown")
        # Check if it's a class build (no pod_number key) or pod build
        is_class_build = "pod_number" not in config

        # Find or create the class entry in data['pod_details']
        class_entry = None
        for entry in data["pod_details"]:
            if entry.get("class_number") == class_number:
                class_entry = entry
                break
        if not class_entry:
            class_entry = {"class_number": class_number, "pods": []}
            data["pod_details"].append(class_entry)

        if is_class_build:
            # Update class-level details
            class_entry.update(record)
            if "pods" not in class_entry: # Ensure 'pods' list exists
                 class_entry["pods"] = []
            logger.debug(f"Prepared DB record for F5 class {class_number}.")
        else:
            # This is a pod build, add/update pod details within the class entry
            pod_number = config.get("pod_number", "unknown")
            record["pod_number"] = pod_number # Add pod number to the record

            # Find or create the pod entry within the class's 'pods' list
            pod_entry = None
            for p in class_entry.get("pods", []):
                 # Ensure comparison is done correctly (e.g., int vs int or str vs str)
                 if p.get("pod_number") == pod_number:
                     pod_entry = p
                     break


            if pod_entry:
                pod_entry.update(record) # Update existing pod
            else:
                class_entry.setdefault("pods", []).append(record) # Add new pod

            # Ensure class-level info (like host) is present if class wasn't built explicitly first
            if "host" not in class_entry:
                 class_entry["host"] = args.host # Add host at class level if missing
            logger.debug(f"Prepared DB record for F5 pod {pod_number} in class {class_number}.")

    else: # Non-F5 vendors
        # Use pod_number if present, otherwise fall back to class_number
        record_key = "pod_number" if "pod_number" in config else "class_number"
        record[record_key] = config.get(record_key, "unknown")
        data["pod_details"].append(record)
        logger.debug(f"Prepared DB record for pod/class {record.get(record_key)}.")

    # Update the database with the accumulated data
    try:
        # Use a temporary copy for logging to avoid modifying the original data if logging fails
        log_data_copy = data.copy()
        # Make sure pod_details exists before trying to log it
        pod_details_to_log = data.get("pod_details", [])
        log_data_copy["pod_details"] = pod_details_to_log
        logger.info(f"Updating database with pod details: {log_data_copy['pod_details']}")
        update_database(data)
    except Exception as e:
        logger.error(f"Failed during final database update: {e}", exc_info=True)


# -----------------------------------------------------------------------------
# Utility: Wait for Futures
# -----------------------------------------------------------------------------
def wait_for_tasks(futures: List[Future], description: str = "tasks") -> List[Dict[str, Any]]:
    """Wait for futures, log errors, and return structured results."""
    results = []
    logger.info(f"Waiting for {len(futures)} {description} to complete...")
    done, not_done = wait(futures)

    for future in done:
        pod_num = getattr(future, "pod_number", None) # Use None if not present
        class_num = getattr(future, "class_number", None)
        # Prioritize pod_num as identifier if present
        identifier = str(pod_num) if pod_num is not None else str(class_num) if class_num is not None else "unknown"
        class_identifier = str(class_num) if class_num is not None else None # Store class separately if available

        try:
            # Assume the task function returns (success: bool, step: str|None, error: str|None)
            # Or raises an exception on failure.
            task_result = future.result()
            if isinstance(task_result, tuple) and len(task_result) == 3:
                success, step, error_msg = task_result
                results.append({
                    "identifier": identifier,
                    "class_identifier": class_identifier,
                    "status": "success" if success else "failed",
                    "failed_step": step,
                    "error_message": error_msg
                })
                logger.debug(f"Task completed (ID: {identifier}, Class: {class_identifier}). Result: {task_result}")
            else:
                 # Handle tasks that might return simpler results or None for success
                 results.append({
                     "identifier": identifier,
                     "class_identifier": class_identifier,
                     "status": "success", # Assume success if no specific failure tuple/exception
                     "failed_step": None,
                     "error_message": None
                 })
                 logger.debug(f"Task completed (ID: {identifier}, Class: {class_identifier}). Result: {task_result}")

        except Exception as e:
            logger.error(f"Task FAILED (ID: {identifier}, Class: {class_identifier}). Error: {e}", exc_info=True)
            results.append({
                "identifier": identifier,
                "class_identifier": class_identifier,
                "status": "failed",
                "failed_step": "task_exception",
                "error_message": str(e)
            })

    if not_done:
         logger.warning(f"{len(not_done)} {description} did not complete (timed out or cancelled).")
         for future in not_done:
             pod_num = getattr(future, "pod_number", None)
             class_num = getattr(future, "class_number", None)
             identifier = str(pod_num) if pod_num is not None else str(class_num) if class_num is not None else "unknown"
             class_identifier = str(class_num) if class_num is not None else None
             results.append({
                 "identifier": identifier,
                 "class_identifier": class_identifier,
                 "status": "failed",
                 "failed_step": "timeout_or_cancelled",
                 "error_message": "Task did not complete."
            })

    return results # Return the list of result dictionaries

# -----------------------------------------------------------------------------
# Unified Vendor Operations: Setup and Teardown
# -----------------------------------------------------------------------------
def vendor_setup(service_instance: VCenter, host_details: Dict[str, Any], args: argparse.Namespace, course_config: Dict[str, Any], selected_components: Optional[List[str]], operation_logger: OperationLogger) -> List[Dict[str, Any]]: # Add logger and return type
    """Unified vendor setup routine."""
    vendor_shortcode = course_config.get("vendor_shortcode")
    logger.info(f"Dispatching setup for vendor '{vendor_shortcode}'. Run ID: {operation_logger.run_id}")

    data_accumulator = {"tag": args.tag, "course_name": args.course, "vendor": vendor_shortcode, "pod_details": []}
    all_results = [] # To collect results from all tasks

    if vendor_shortcode == "f5":
        if not args.class_number:
             logger.error("F5 setup requires --class_number.")
             operation_logger.log_pod_status(pod_id=f"class-{args.class_number}", status="failed", step="missing_class_number", error="--class_number is required.")
             return [{"identifier": f"class-{args.class_number}", "class_identifier": args.class_number, "status": "failed", "failed_step": "missing_class_number", "error_message": "--class_number is required."}]


        # 1. Build F5 Class (Synchronous)
        class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number)
        class_config.update({
            "host_fqdn": host_details["fqdn"],
            "class_number": args.class_number,
            "class_name": f"f5-class{args.class_number}",
            "vendor_shortcode": vendor_shortcode # Ensure vendor code is present
        })
        logger.info(f"Building F5 class {args.class_number}...")
        class_result = f5.build_class(
            service_instance, class_config, rebuild=args.re_build, full=args.full, selected_components=selected_components
        )

        # Log F5 class result directly using OperationLogger
        class_success, class_step, class_error = class_result
        class_id_str = f"class-{args.class_number}" # Use a distinct ID for class
        operation_logger.log_pod_status(
            pod_id=class_id_str,
            status="success" if class_success else "failed",
            step=class_step,
            error=class_error,
            class_id=args.class_number # Identify it as a class operation
        )
        all_results.append({
            "identifier": class_id_str,
            "class_identifier": args.class_number,
            "status": "success" if class_success else "failed",
            "failed_step": class_step,
            "error_message": class_error
        })

        if class_success:
             # Update DB/Monitor only on success
             logger.info(f"Class {args.class_number} build successful. Updating monitor & DB.")
             try:
                 update_monitor_and_database(class_config, args, data_accumulator, {"class_number": args.class_number})
             except Exception as e:
                 logger.error(f"Error updating monitor/DB for Class {args.class_number}: {e}", exc_info=True)
                 # Optionally log this failure too
                 operation_logger.log_pod_status(pod_id=class_id_str, status="failed", step="update_monitor_db", error=str(e), class_id=args.class_number)
                 # Update the result status? This is tricky as the build was successful. Maybe add a separate event type?
        else:
             logger.warning(f"F5 class {args.class_number} build failed. Pods might have issues.")
             # Return early if class build fails? Or continue? Currently continues.

        # 2. Build F5 Pods (Synchronous loop)
        if class_success and args.start_pod is not None and args.end_pod is not None: # Only build pods if class was ok and range provided
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                logger.info(f"Building F5 pod {pod} for class {args.class_number}...")
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number)
                pod_config.update({
                    "host_fqdn": host_details["fqdn"],
                    "class_number": args.class_number,
                    "pod_number": pod, # Use integer pod number
                    "vendor_shortcode": vendor_shortcode # Ensure vendor code is present
                })
                pod_id_str = str(pod)
                try:
                    pod_result = f5.build_pod(
                        service_instance, pod_config, mem=args.memory, rebuild=args.re_build,
                        full=args.full, selected_components=selected_components
                    )
                    # Log pod result directly using OperationLogger
                    pod_success, pod_step, pod_error = pod_result
                    operation_logger.log_pod_status(
                        pod_id=pod_id_str,
                        status="success" if pod_success else "failed",
                        step=pod_step,
                        error=pod_error,
                        class_id=args.class_number
                    )
                    all_results.append({
                        "identifier": pod_id_str,
                        "class_identifier": args.class_number,
                        "status": "success" if pod_success else "failed",
                        "failed_step": pod_step,
                        "error_message": pod_error
                    })
                    if pod_success:
                        # Update DB/Monitor only on success
                        logger.info(f"Pod {pod} build successful. Updating monitor & DB.")
                        try:
                             update_monitor_and_database(pod_config, args, data_accumulator, {"class_number": args.class_number})
                        except Exception as e:
                             logger.error(f"Error updating monitor/DB for pod {pod}: {e}", exc_info=True)
                             operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="update_monitor_db", error=str(e), class_id=args.class_number)

                except Exception as e:
                    # Log exception result
                    logger.error(f"F5 build for pod {pod} failed with exception: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="build_pod_exception", error=str(e), class_id=args.class_number)
                    all_results.append({"identifier": pod_id_str, "class_identifier": args.class_number, "status": "failed", "failed_step": "build_pod_exception", "error_message": str(e)})
                    # Continue to next pod

    else: # Other Vendors (Async Pod Builds)
        build_func = VENDOR_SETUP_MAP.get(vendor_shortcode)
        if not build_func:
            logger.error(f"Unsupported vendor for setup: {vendor_shortcode}")
            operation_logger.log_pod_status(pod_id=f"vendor-{vendor_shortcode}", status="failed", step="unsupported_vendor", error=f"Vendor code '{vendor_shortcode}' not found in VENDOR_SETUP_MAP.")
            return [{"identifier": f"vendor-{vendor_shortcode}", "status": "failed", "failed_step": "unsupported_vendor", "error_message": f"Vendor '{vendor_shortcode}' not supported."}]

        futures = []
        pod_configs_map = {} # Store config for later lookup

        with ThreadPoolExecutor(max_workers=args.thread) as executor:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                logger.debug(f"Preparing config for pod {pod}...")
                pod_id_str = str(pod)
                try:
                    # Fetch config inside the loop for pod-specific replacements
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["pod_number"] = pod
                    pod_config["vendor_shortcode"] = vendor_shortcode # Ensure vendor code is present
                    course_name_lower = pod_config.get("course_name", "").lower()
                    pod_configs_map[pod_id_str] = pod_config # Store config

                    # Adjust build function based on course specifics (Palo, Avaya)
                    current_build_func = build_func # Start with default
                    if vendor_shortcode == "pa":
                        if "cortex" in course_name_lower: current_build_func = palo.build_cortex_pod
                        elif "1100-210" in course_name_lower: current_build_func = palo.build_1100_210_pod
                        elif "1110" in course_name_lower: current_build_func = palo.build_1110_pod
                        elif "1100-220" in course_name_lower: current_build_func = palo.build_1100_220_pod
                        else:
                            logger.error(f"Unsupported Palo course for setup: {course_name_lower}")
                            operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Unsupported Palo course: {course_name_lower}")
                            all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "unsupported_course", "error_message": f"Unsupported Palo course: {course_name_lower}"})
                            continue
                    elif vendor_shortcode == "av":
                        if "aura" in course_name_lower: current_build_func = avaya.build_aura_pod
                        elif "ipo" in course_name_lower: current_build_func = avaya.build_ipo_pod
                        elif "aep" in course_name_lower: current_build_func = avaya.build_aep_pod
                        else:
                             logger.error(f"Unsupported Avaya course for setup: {course_name_lower}")
                             operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Unsupported Avaya course: {course_name_lower}")
                             all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "unsupported_course", "error_message": f"Unsupported Avaya course: {course_name_lower}"})
                             continue

                    # Prepare arguments for the build function
                    build_args = {
                        "service_instance": service_instance,
                        "pod_config": pod_config,
                        "rebuild": args.re_build,
                        "full": args.full,
                        "selected_components": selected_components,
                    }
                    # Add/remove args based on vendor specifics if function signature differs
                    if vendor_shortcode == "cp": build_args["thread"] = args.thread
                    if vendor_shortcode == "av":
                        if "aura" in course_name_lower:
                            build_args = {"service_instance": service_instance, "pod_config": pod_config}
                        elif "aep" in course_name_lower:
                             # Ensure selected_components is a list or None
                             safe_selected_components = selected_components if isinstance(selected_components, list) else []
                             if "Student" not in safe_selected_components:
                                 common_config = pod_config.copy()
                                 common_config["type"] = "common"
                                 logger.info(f"Building AEP common components for pod {pod}...")
                                 # Run common part synchronously? Assume success/fail tuple return
                                 aep_common_result = avaya.build_aep_pod(service_instance, common_config, selected_components=selected_components)
                                 aep_common_success, aep_common_step, aep_common_error = aep_common_result
                                 operation_logger.log_pod_status(pod_id=f"{pod_id_str}-common", status="success" if aep_common_success else "failed", step=aep_common_step, error=aep_common_error)
                                 all_results.append({"identifier": f"{pod_id_str}-common", "class_identifier": None, "status": "success" if aep_common_success else "failed", "failed_step": aep_common_step, "error_message": aep_common_error})
                                 if not aep_common_success:
                                     logger.error("AEP common build failed, skipping student part.")
                                     continue # Skip student part if common failed

                             pod_config["type"] = "student" # Ensure type is set for student build
                             build_args = {"service_instance": service_instance, "pod_config": pod_config, "selected_components": selected_components} # Update args for AEP student
                        # Remove 'full' if not used by avaya funcs
                        build_args.pop("full", None) # Safely remove


                    # Submit the task
                    logger.info(f"Submitting build task for pod {pod}...")
                    future = executor.submit(current_build_func, **build_args)
                    future.pod_number = pod # Attach pod number for context
                    future.class_number = None # Attach class if applicable
                    futures.append(future)

                except ValueError as e: # Catch config fetch errors
                     logger.error(f"Skipping pod {pod} due to configuration error: {e}")
                     operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e))
                     all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "config_error", "error_message": str(e)})
                except Exception as e:
                    logger.error(f"Error submitting task for pod {pod}: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e))
                    all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "submit_task_error", "error_message": str(e)})


        # Wait for tasks and process results AFTER they complete
        task_results = wait_for_tasks(futures, description=f"{vendor_shortcode} pod builds")

        # Now iterate through results, log status, and update DB/Monitor
        for result_data in task_results:
            pod_id = result_data["identifier"] # This is already a string from wait_for_tasks
            class_id = result_data["class_identifier"] # Already string or None
            status = result_data["status"]
            step = result_data["failed_step"]
            error = result_data["error_message"]

            # Log status using OperationLogger
            operation_logger.log_pod_status(pod_id=pod_id, status=status, step=step, error=error, class_id=class_id)

            # Find the original config for this pod
            pod_config_for_update = pod_configs_map.get(pod_id)

            if status == "success":
                if pod_config_for_update:
                    try:
                        # Call monitor/DB update only on success
                        update_monitor_and_database(pod_config_for_update, args, data_accumulator)
                        logger.info(f"Pod {pod_id} build successful. Monitor & DB updated.")
                    except Exception as update_err:
                        logger.error(f"Error updating monitor/DB for successful pod {pod_id}: {update_err}", exc_info=True)
                        # Log this specific failure
                        operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db", error=str(update_err), class_id=class_id)
                        # Update the status in the collected results?
                        result_data["status"] = "failed" # Mark as failed overall if DB update fails
                        result_data["failed_step"] = "update_monitor_db"
                        result_data["error_message"] = str(update_err)
                else:
                    logger.warning(f"Could not find config for successful pod {pod_id} to update monitor/DB.")
                    # Log this issue?
                    operation_logger.log_pod_status(pod_id=pod_id, status="failed", step="update_monitor_db", error="Config not found", class_id=class_id)
                    result_data["status"] = "failed"
                    result_data["failed_step"] = "update_monitor_db"
                    result_data["error_message"] = "Config not found for update"
            # else: # Failure case already logged by logger.error() above
                 # logger.error(f"Pod {pod_id} build FAILED at step '{step}': {error}") # Already logged by wait_for_tasks

        all_results.extend(task_results) # Add results from wait_for_tasks
        logger.info(f"Finished processing setup for pods {args.start_pod}-{args.end_pod}.")

    return all_results # Return collected results

def vendor_teardown(service_instance: VCenter, host_details: Dict[str, Any], args: argparse.Namespace, course_config: Dict[str, Any], operation_logger: OperationLogger) -> List[Dict[str, Any]]: # Add logger and return type
    """Unified vendor teardown routine."""
    vendor_shortcode = course_config.get("vendor_shortcode")
    course_name = course_config.get("course_name", args.course)
    logger.info(f"Dispatching teardown for vendor '{vendor_shortcode}'. Run ID: {operation_logger.run_id}")
    all_results = []

    # --- F5 Specific Handling ---
    if vendor_shortcode == "f5":
        if not args.class_number:
             logger.error("F5 teardown requires --class_number.")
             # Log failure and return
             operation_logger.log_pod_status(pod_id=f"class-{args.class_number}", status="failed", step="missing_class_number", error="--class_number is required.")
             return [{"identifier": f"class-{args.class_number}", "class_identifier": args.class_number, "status": "failed", "failed_step": "missing_class_number", "error_message": "--class_number is required."}]

        class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number)
        class_config.update({
            "host_fqdn": host_details["fqdn"],
            "class_number": args.class_number, # Ensure class_number is in config
            "class_name": f"f5-class{args.class_number}"
        })
        class_id_str = f"class-{args.class_number}"
        logger.info(f"Tearing down F5 class {args.class_number}...")
        teardown_success = True
        teardown_error = None
        teardown_step = None
        try:
            f5.teardown_class(service_instance, class_config)
            logger.info(f"F5 class {args.class_number} teardown initiated.")

            # Delete monitor and database entry for the class
            with mongo_client() as client:
                if not client:
                     logger.warning(f"Skipping PRTG delete for F5 class {args.class_number}: MongoDB connection failed.")
                else:
                    # PRTG URL for F5 class uses class_number, pod_number=None
                    prtg_url = get_prtg_url(args.tag, course_name, pod_number=None, class_number=args.class_number)
                    if prtg_url:
                        if PRTGManager.delete_monitor(prtg_url, client):
                            logger.info(f"Deleted PRTG monitor for F5 class {args.class_number}.")
                        else:
                            logger.warning(f"Failed to delete PRTG monitor for F5 class {args.class_number}.")
                            # Optionally mark as partial failure?
                    else:
                        logger.warning(f"No PRTG URL found for F5 class {args.class_number} to delete.")

            delete_from_database(args.tag, course_name=course_name, class_number=args.class_number)
            logger.info(f"Database entry deleted for F5 class {args.class_number}.")

        except Exception as e:
            teardown_success = False
            teardown_error = str(e)
            teardown_step = "teardown_class_exception"
            logger.error(f"Error during F5 class teardown for class {args.class_number}: {e}", exc_info=True)

        # Log class teardown result
        operation_logger.log_pod_status(
            pod_id=class_id_str,
            status="success" if teardown_success else "failed",
            step=teardown_step,
            error=teardown_error,
            class_id=args.class_number
        )
        all_results.append({
            "identifier": class_id_str,
            "class_identifier": args.class_number,
            "status": "success" if teardown_success else "failed",
            "failed_step": teardown_step,
            "error_message": teardown_error
        })
        # Note: F5 doesn't teardown individual pods typically, just the class resource pool

    # --- Other Vendors (Async Pod Teardown) ---
    else:
        teardown_func = VENDOR_TEARDOWN_MAP.get(vendor_shortcode)
        if not teardown_func:
            logger.error(f"Unsupported vendor for teardown: {vendor_shortcode}")
            operation_logger.log_pod_status(pod_id=f"vendor-{vendor_shortcode}", status="failed", step="unsupported_vendor", error=f"Vendor code '{vendor_shortcode}' not found in VENDOR_TEARDOWN_MAP.")
            return [{"identifier": f"vendor-{vendor_shortcode}", "status": "failed", "failed_step": "unsupported_vendor", "error_message": f"Vendor '{vendor_shortcode}' not supported."}]

        futures = []
        with ThreadPoolExecutor(max_workers=args.thread) as executor: # Use thread arg for consistency
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod)
                try:
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["pod_number"] = pod
                    course_name_lower = pod_config.get("course_name", "").lower()

                    # Adjust teardown function based on course specifics (Palo, Avaya)
                    current_teardown_func = teardown_func
                    if vendor_shortcode == "pa":
                        if "1110" in course_name_lower: current_teardown_func = palo.teardown_1110
                        elif "1100" in course_name_lower: current_teardown_func = palo.teardown_1100
                        elif "cortex" in course_name_lower: current_teardown_func = palo.teardown_cortex
                        else:
                             logger.error(f"Unsupported Palo course for teardown: {course_name_lower}")
                             operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Unsupported Palo course: {course_name_lower}")
                             all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "unsupported_course", "error_message": f"Unsupported Palo course: {course_name_lower}"})
                             continue
                    elif vendor_shortcode == "av":
                         if "ipo" in course_name_lower: current_teardown_func = avaya.teardown_ipo
                         elif "aura" in course_name_lower: current_teardown_func = avaya.teardown_aura
                         # Add AEP teardown if exists
                         # elif "aep" in course_name_lower: current_teardown_func = avaya.teardown_aep
                         else:
                             logger.error(f"Unsupported Avaya course for teardown: {course_name_lower}")
                             operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="unsupported_course", error=f"Unsupported Avaya course: {course_name_lower}")
                             all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "unsupported_course", "error_message": f"Unsupported Avaya course: {course_name_lower}"})
                             continue

                    logger.info(f"Submitting teardown task for pod {pod}...")
                    future = executor.submit(current_teardown_func, service_instance, pod_config)
                    future.pod_number = pod # Attach pod number for context
                    future.class_number = None # Attach class if applicable

                    # Delete monitor and database entry immediately (Could be moved to after task completion)
                    try:
                        with mongo_client() as client:
                             if not client:
                                 logger.warning(f"Skipping PRTG delete for pod {pod}: MongoDB connection failed.")
                             else:
                                 prtg_url = get_prtg_url(args.tag, course_name, pod_number=pod)
                                 if prtg_url:
                                     if PRTGManager.delete_monitor(prtg_url, client):
                                         logger.info(f"Deleted PRTG monitor for pod {pod}.")
                                     else:
                                         logger.warning(f"Failed to delete PRTG monitor for pod {pod}.")
                                 else:
                                     logger.warning(f"No PRTG URL found for pod {pod} to delete.")

                        delete_from_database(args.tag, course_name=course_name, pod_number=pod)
                        logger.info(f"Database entry deleted for pod {pod}.")
                    except Exception as db_mon_err:
                         logger.error(f"Error deleting monitor/DB entry for pod {pod}: {db_mon_err}", exc_info=True)
                         # Should this mark the pod teardown as failed? For now, just log.

                    futures.append(future)

                except ValueError as e: # Catch config fetch errors
                     logger.error(f"Skipping pod {pod} teardown due to configuration error: {e}")
                     operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e))
                     all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "config_error", "error_message": str(e)})
                except Exception as e:
                    logger.error(f"Error submitting teardown task for pod {pod}: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e))
                    all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": "failed", "failed_step": "submit_task_error", "error_message": str(e)})

        # Wait for tasks and process results
        task_results = wait_for_tasks(futures, description=f"{vendor_shortcode} pod teardowns")

        # Log status for each pod AFTER completion
        for result_data in task_results:
             operation_logger.log_pod_status(
                 pod_id=result_data["identifier"],
                 status=result_data["status"],
                 step=result_data["failed_step"],
                 error=result_data["error_message"],
                 class_id=result_data["class_identifier"] # Pass class if available
             )
             # Note: DB/Monitor deletion happened earlier.

        all_results.extend(task_results) # Add results from wait_for_tasks
        logger.info(f"Finished processing teardown for pods {args.start_pod}-{args.end_pod}.")

    return all_results # Return collected results

# -----------------------------------------------------------------------------
# Helper to list courses for a given vendor
# -----------------------------------------------------------------------------
def list_vendor_courses(vendor: str):
    """List available courses for the given vendor from MongoDB."""
    print(f"Looking for courses for vendor '{vendor}'...")
    try:
        with mongo_client() as client:
            if not client:
                 print("\nError: Could not connect to MongoDB to list courses.")
                 sys.exit(1)

            db = client[DB_NAME]
            collection = db[COURSE_CONFIG_COLLECTION]
            # Case-insensitive query using regex
            courses_cursor = collection.find(
                {"vendor_shortcode": {"$regex": f"^{vendor}$", "$options": "i"}},
                {"course_name": 1, "_id": 0} # Projection
            )
            courses = [doc["course_name"] for doc in courses_cursor if "course_name" in doc]

            if courses:
                print(f"\nAvailable courses for vendor '{vendor}':")
                for course in sorted(courses):
                    print(f"  - {course}")
            else:
                print(f"\nNo courses found for vendor '{vendor}'.")
    except PyMongoError as e:
         print(f"\nError accessing course configurations (PyMongoError): {e}")
    except Exception as e:
        print(f"\nError accessing course configurations: {e}")
    sys.exit(0) # Exit after listing

# -----------------------------------------------------------------------------
# Environment Operations: Setup, Teardown, and Manage
# -----------------------------------------------------------------------------
def setup_environment(args: argparse.Namespace, operation_logger: OperationLogger): # Add logger
    """
    Set up the lab environment based on provided arguments.
    """
    # --- Initial Setup & Validation ---
    if args.course == "?":
        list_vendor_courses(args.vendor) # Exits

    course_config = None
    try:
         # Fetch config early for component listing/validation
         course_config = fetch_and_prepare_course_config(args.course)
    except Exception as e:
         logger.critical(f"Failed to fetch initial course config for '{args.course}': {e}")
         operation_logger.log_pod_status(pod_id="config_fetch", status="failed", step="fetch_initial_config", error=str(e))
         # We need to return something for finalize, even if empty or failure marker
         return [{"identifier": "config_fetch", "status": "failed", "failed_step": "fetch_initial_config", "error_message": str(e)}]


    if getattr(args, 'component', None) == "?":
        if not course_config:
             logger.error("Cannot list components: Course config could not be loaded.")
             # Already logged failure above
             return [{"identifier": "component_list", "status": "failed", "failed_step": "config_not_loaded", "error_message": "Config not loaded"}]
        comps = extract_components(course_config)
        print(f"\nAvailable components for course '{args.course}':")
        for comp in comps:
            print(f"  - {comp}")
        sys.exit(0) # Exit after listing

    selected_components = None
    if args.component: # No need to check for "?" again
        selected_components = [comp.strip() for comp in args.component.split(",")]
        available = extract_components(course_config)
        invalid = [comp for comp in selected_components if comp not in available]
        if invalid:
            err_msg = f"Invalid components specified: {', '.join(invalid)}. Available: {', '.join(available)}"
            logger.error(err_msg)
            operation_logger.log_pod_status(pod_id="component_validation", status="failed", step="invalid_component", error=err_msg)
            return [{"identifier": "component_validation", "status": "failed", "failed_step": "invalid_component", "error_message": err_msg}]


    all_results = [] # Collect results here

    # --- Special Modes ---
    if args.db_only:
        logger.info("DB-only mode: Updating database without building resources.")
        data = {"tag": args.tag, "course_name": args.course, "vendor": args.vendor, "pod_details": []}
        start_pod = int(args.start_pod)
        end_pod = int(args.end_pod)
        for pod_num in range(start_pod, end_pod + 1):
            pod_detail = {"pod_number": pod_num, "host": args.host, "poweron": "False", "prtg_url": None}
            if args.vendor.lower() == "f5":
                pod_detail["class_number"] = args.class_number
            data["pod_details"].append(pod_detail)
        update_database(data)
        logger.info("DB-only setup update complete.")
        # Log a single "db_only" event
        operation_logger.log_pod_status(pod_id="db_only", status="skipped", step="db_only_mode")
        return [{"identifier": "db_only", "status": "skipped", "class_identifier": args.class_number if args.vendor.lower() == 'f5' else None}] # Return dummy result


    # Fetch host details for modes that need vCenter interaction
    host_details = None
    service_instance = None
    needs_vcenter = not args.db_only # Perm, Monitor, and Normal modes need vCenter

    if needs_vcenter:
        host_details = get_host_by_name(args.host)
        if not host_details:
            err_msg = f"Host details could not be retrieved for host '{args.host}'."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="host_fetch", status="failed", step="get_host_details", error=err_msg)
            return [{"identifier": "host_fetch", "status": "failed", "failed_step": "get_host_details", "error_message": err_msg}]

        service_instance = get_vcenter_instance(host_details)
        if not service_instance:
            err_msg = f"Failed to connect to vCenter specified by host '{args.host}'."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="vcenter_connect", status="failed", step="get_vcenter_instance", error=err_msg)
            return [{"identifier": "vcenter_connect", "status": "failed", "failed_step": "get_vcenter_instance", "error_message": err_msg}]


    # Permission-only mode (CheckPoint specific?)
    if args.perm:
        if args.vendor.lower() == 'cp':
            logger.info("Permission-only mode enabled. Running CheckPoint permission functions.")
            success_count = 0
            fail_count = 0
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod)
                pod_status = "success"
                pod_step = None
                pod_error = None
                try:
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"] # host_details fetched above
                    pod_config["pod_number"] = pod
                    logger.info(f"Running permission functions for pod {pod}.")
                    checkpoint.perm_only_cp_pod(service_instance, pod_config) # service_instance fetched above
                    success_count +=1
                except Exception as e:
                     pod_status = "failed"
                     pod_step = "perm_only_exception"
                     pod_error = str(e)
                     fail_count += 1
                     logger.error(f"Error running perm-only for pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=pod_status, step=pod_step, error=pod_error)
                all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": pod_status, "failed_step": pod_step, "error_message": pod_error})
            logger.info("Permission-only process complete.")
        else:
            logger.error("Permission-only mode (--perm) is currently only supported for CheckPoint (cp).")
            operation_logger.log_pod_status(pod_id="perm_only", status="failed", step="unsupported_vendor", error="--perm only for cp")
            all_results.append({"identifier": "perm_only", "status": "failed", "failed_step": "unsupported_vendor", "error_message": "--perm only for cp"})
        return all_results # Return collected results

    # Monitor-only mode
    if args.monitor_only:
        logger.info("Monitor-only mode enabled. Creating monitors without building pods.")
        data_accumulator = {"tag": args.tag, "course_name": args.course, "vendor": args.vendor, "pod_details": []}
        vendor_shortcode = course_config.get("vendor_shortcode")
        success_count = 0
        fail_count = 0

        if vendor_shortcode == "f5":
            if not args.class_number:
                logger.error("F5 monitor-only requires --class_number.")
                operation_logger.log_pod_status(pod_id="monitor_only", status="failed", step="missing_class_number", error="--class_number required for F5 monitor-only")
                return [{"identifier": "monitor_only", "status": "failed", "failed_step": "missing_class_number", "error_message": "--class_number required for F5 monitor-only"}]

            # Add monitor for the class itself
            class_id_str = f"class-{args.class_number}"
            class_status = "success"
            class_step = None
            class_error = None
            try:
                class_cfg = fetch_and_prepare_course_config(args.course, f5_class=args.class_number)
                class_cfg.update({
                    "host_fqdn": host_details["fqdn"],
                    "class_number": args.class_number,
                    "vendor_shortcode": vendor_shortcode
                })
                # Use update_monitor_and_database which handles F5 class structure
                update_monitor_and_database(class_cfg, args, data_accumulator, {"class_number": args.class_number})
                success_count +=1
            except Exception as e:
                 class_status = "failed"
                 class_step = "monitor_only_class_exception"
                 class_error = str(e)
                 fail_count += 1
                 logger.error(f"Error creating monitor for F5 class {args.class_number}: {e}", exc_info=True)
            operation_logger.log_pod_status(pod_id=class_id_str, status=class_status, step=class_step, error=class_error, class_id=args.class_number)
            all_results.append({"identifier": class_id_str, "class_identifier": args.class_number, "status": class_status, "failed_step": class_step, "error_message": class_error})

            # Add monitors for pods
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod)
                pod_status = "success"
                pod_step = None
                pod_error = None
                try:
                    pod_cfg = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number)
                    pod_cfg.update({
                        "host_fqdn": host_details["fqdn"],
                        "class_number": args.class_number,
                        "pod_number": pod,
                        "vendor_shortcode": vendor_shortcode
                    })
                    update_monitor_and_database(pod_cfg, args, data_accumulator, {"class_number": args.class_number})
                    success_count += 1
                except Exception as e:
                     pod_status = "failed"
                     pod_step = "monitor_only_pod_exception"
                     pod_error = str(e)
                     fail_count += 1
                     logger.error(f"Error creating monitor for F5 pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=pod_status, step=pod_step, error=pod_error, class_id=args.class_number)
                all_results.append({"identifier": pod_id_str, "class_identifier": args.class_number, "status": pod_status, "failed_step": pod_step, "error_message": pod_error})

        else: # Non-F5 vendors
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_id_str = str(pod)
                pod_status = "success"
                pod_step = None
                pod_error = None
                try:
                    pod_cfg = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_cfg.update({
                        "host_fqdn": host_details["fqdn"],
                        "pod_number": pod,
                        "vendor_shortcode": vendor_shortcode
                    })
                    update_monitor_and_database(pod_cfg, args, data_accumulator)
                    success_count += 1
                except Exception as e:
                     pod_status = "failed"
                     pod_step = "monitor_only_exception"
                     pod_error = str(e)
                     fail_count += 1
                     logger.error(f"Error creating monitor for pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status=pod_status, step=pod_step, error=pod_error)
                all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": pod_status, "failed_step": pod_step, "error_message": pod_error})

        logger.info("Monitor-only setup complete.")
        return all_results # Return collected results


    # --- Normal Build Process ---
    logger.info(f"Starting normal setup for course '{args.course}' on host '{args.host}' (Pods {args.start_pod}-{args.end_pod})")
    # Pass operation_logger to vendor_setup
    all_results = vendor_setup(service_instance, host_details, args, course_config, selected_components, operation_logger)
    logger.info(f"Setup process finished for course '{args.course}'.") # Changed from 'initiated'
    return all_results # Return results from vendor_setup


def teardown_environment(args: argparse.Namespace, operation_logger: OperationLogger): # Add logger
    """Tear down the lab environment."""
    all_results = []
    host_details = None
    service_instance = None
    course_config = None

    try:
        course_config = fetch_and_prepare_course_config(args.course)
    except Exception as e:
         logger.critical(f"Failed to fetch course config for teardown '{args.course}': {e}")
         operation_logger.log_pod_status(pod_id="config_fetch", status="failed", step="fetch_config_teardown", error=str(e))
         return [{"identifier": "config_fetch", "status": "failed", "failed_step": "fetch_config_teardown", "error_message": str(e)}]

    vendor_shortcode = course_config.get("vendor_shortcode")
    course_name = course_config.get("course_name", args.course)

    # --- Special Modes ---
    if args.db_only:
        logger.info("DB-only teardown: Deleting database entries only.")
        if vendor_shortcode == "f5":
            if not args.class_number:
                err_msg = "F5 DB-only teardown requires --class_number."
                logger.error(err_msg)
                operation_logger.log_pod_status(pod_id="db_only", status="failed", step="missing_class_number", error=err_msg)
                return [{"identifier": "db_only", "status": "failed", "failed_step": "missing_class_number", "error_message": err_msg}]
            delete_from_database(args.tag, course_name=course_name, class_number=args.class_number)
            all_results.append({"identifier": f"class-{args.class_number}", "status": "skipped", "class_identifier": args.class_number})
        else:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                delete_from_database(args.tag, course_name=course_name, pod_number=pod)
                all_results.append({"identifier": str(pod), "status": "skipped", "class_identifier": None})
        logger.info("DB-only teardown complete.")
        operation_logger.log_pod_status(pod_id="db_only", status="skipped", step="db_only_mode")
        return all_results

    if args.monitor_only:
        logger.info("Monitor-only teardown: Deleting monitors and DB entries only.")
        success_count = 0
        fail_count = 0
        try:
            with mongo_client() as client:
                if not client:
                     err_msg = "MongoDB connection failed for monitor-only teardown."
                     logger.error(err_msg)
                     operation_logger.log_pod_status(pod_id="monitor_only", status="failed", step="db_connection", error=err_msg)
                     return [{"identifier": "monitor_only", "status": "failed", "failed_step": "db_connection", "error_message": err_msg}]

                if vendor_shortcode == "f5":
                    if not args.class_number:
                        err_msg = "F5 monitor-only teardown requires --class_number."
                        logger.error(err_msg)
                        operation_logger.log_pod_status(pod_id="monitor_only", status="failed", step="missing_class_number", error=err_msg)
                        return [{"identifier": "monitor_only", "status": "failed", "failed_step": "missing_class_number", "error_message": err_msg}]

                    class_id_str = f"class-{args.class_number}"
                    mon_status="success"; mon_step=None; mon_error=None
                    try:
                        # Delete class monitor
                        prtg_url = get_prtg_url(args.tag, course_name, pod_number=None, class_number=args.class_number)
                        if prtg_url:
                            if not PRTGManager.delete_monitor(prtg_url, client):
                                logger.warning(f"Failed to delete PRTG monitor for F5 class {args.class_number}.")
                                mon_status="failed"; mon_step="prtg_delete"; mon_error="PRTG delete failed"
                        else:
                             logger.warning(f"No PRTG URL found for F5 class {args.class_number}.")
                        # Delete class entry from DB
                        delete_from_database(args.tag, course_name=course_name, class_number=args.class_number)
                        if mon_status == "success": success_count += 1
                        else: fail_count += 1
                    except Exception as e:
                        mon_status="failed"; mon_step="monitor_only_class_exception"; mon_error=str(e); fail_count += 1
                        logger.error(f"Error during monitor-only teardown for class {args.class_number}: {e}", exc_info=True)
                    operation_logger.log_pod_status(pod_id=class_id_str, status=mon_status, step=mon_step, error=mon_error, class_id=args.class_number)
                    all_results.append({"identifier": class_id_str, "class_identifier": args.class_number, "status": mon_status, "failed_step": mon_step, "error_message": mon_error})

                else: # Non-F5 vendors
                    for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                        pod_id_str = str(pod)
                        mon_status="success"; mon_step=None; mon_error=None
                        try:
                            prtg_url = get_prtg_url(args.tag, course_name, pod_number=pod)
                            if prtg_url:
                                if not PRTGManager.delete_monitor(prtg_url, client):
                                     logger.warning(f"Failed to delete PRTG monitor for pod {pod}.")
                                     mon_status="failed"; mon_step="prtg_delete"; mon_error="PRTG delete failed"
                            else:
                                 logger.warning(f"No PRTG URL found for pod {pod} to delete.")
                            delete_from_database(args.tag, course_name=course_name, pod_number=pod)
                            if mon_status == "success": success_count += 1
                            else: fail_count += 1
                        except Exception as e:
                            mon_status="failed"; mon_step="monitor_only_pod_exception"; mon_error=str(e); fail_count += 1
                            logger.error(f"Error during monitor-only teardown for pod {pod}: {e}", exc_info=True)
                        operation_logger.log_pod_status(pod_id=pod_id_str, status=mon_status, step=mon_step, error=mon_error)
                        all_results.append({"identifier": pod_id_str, "class_identifier": None, "status": mon_status, "failed_step": mon_step, "error_message": mon_error})

            logger.info("Monitor-only teardown complete.")
        except Exception as e:
             logger.error(f"Error during monitor-only teardown: {e}", exc_info=True)
             # Log general failure if top-level exception occurs
             operation_logger.log_pod_status(pod_id="monitor_only", status="failed", step="monitor_only_main_exception", error=str(e))
             all_results.append({"identifier": "monitor_only", "status": "failed", "failed_step": "monitor_only_main_exception", "error_message": str(e)})
        return all_results


    # --- Normal Teardown ---
    # Fetch host details and connect to vCenter only for normal teardown
    host_details = get_host_by_name(args.host)
    if not host_details:
        err_msg = f"Host details could not be retrieved for host '{args.host}'."
        logger.critical(err_msg)
        operation_logger.log_pod_status(pod_id="host_fetch", status="failed", step="get_host_details_teardown", error=err_msg)
        return [{"identifier": "host_fetch", "status": "failed", "failed_step": "get_host_details_teardown", "error_message": err_msg}]

    service_instance = get_vcenter_instance(host_details)
    if not service_instance:
         err_msg = f"Failed to connect to vCenter specified by host '{args.host}' for teardown."
         logger.critical(err_msg)
         operation_logger.log_pod_status(pod_id="vcenter_connect", status="failed", step="get_vcenter_instance_teardown", error=err_msg)
         return [{"identifier": "vcenter_connect", "status": "failed", "failed_step": "get_vcenter_instance_teardown", "error_message": err_msg}]


    logger.info(f"Starting normal teardown for course '{args.course}' on host '{args.host}' (Pods {args.start_pod}-{args.end_pod})")
    # Pass operation_logger to vendor_teardown
    all_results = vendor_teardown(service_instance, host_details, args, course_config, operation_logger)
    logger.info(f"Teardown process finished for course '{args.course}'.") # Changed from initiated
    return all_results # Return results

def manage_environment(args: argparse.Namespace, operation_logger: OperationLogger): # Add logger
    """Manage VM power states."""
    all_results = []
    host_details = None
    service_instance = None
    course_config = None

    try:
         host_details = get_host_by_name(args.host)
         if not host_details:
             raise ValueError(f"Host details could not be retrieved for host '{args.host}'.")
         service_instance = get_vcenter_instance(host_details)
         if not service_instance:
             raise ConnectionError(f"Failed to connect to vCenter specified by host '{args.host}'.")
         course_config = fetch_and_prepare_course_config(args.course)
    except Exception as e:
        err_msg = f"Prerequisite failed for manage environment: {e}"
        logger.critical(err_msg)
        operation_logger.log_pod_status(pod_id="prereq_check", status="failed", step="manage_prereqs", error=err_msg)
        return [{"identifier": "prereq_check", "status": "failed", "failed_step": "manage_prereqs", "error_message": err_msg}]


    selected_components = None
    if args.component:
        if args.component == "?":
            comps = extract_components(course_config)
            print(f"\nAvailable components for course '{args.course}':")
            for comp in comps:
                print(f"  - {comp}")
            sys.exit(0) # Exit after listing

        selected_components = [comp.strip() for comp in args.component.split(",")]
        available = extract_components(course_config)
        invalid = [comp for comp in selected_components if comp not in available]
        if invalid:
            err_msg = f"Invalid components specified: {', '.join(invalid)}. Available: {', '.join(available)}"
            logger.error(err_msg)
            operation_logger.log_pod_status(pod_id="component_validation", status="failed", step="invalid_component_manage", error=err_msg)
            return [{"identifier": "component_validation", "status": "failed", "failed_step": "invalid_component_manage", "error_message": err_msg}]


    futures = []
    logger.info(f"Dispatching VM operation '{args.operation}' for pods {args.start_pod}-{args.end_pod}. Run ID: {operation_logger.run_id}")
    with ThreadPoolExecutor(max_workers=args.thread) as executor: # Use thread arg
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            pod_id_str = str(pod)
            class_num_for_log = args.class_number if args.vendor.lower() == 'f5' else None
            try:
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=class_num_for_log) # Pass class num if F5
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                if class_num_for_log is not None:
                    pod_config["class_number"] = class_num_for_log


                # Submit the management task
                future = executor.submit(
                    vm_operations.perform_vm_operations,
                    service_instance,
                    pod_config,
                    args.operation,
                    selected_components
                )
                future.pod_number = pod # Attach pod number for context
                future.class_number = class_num_for_log # Attach class number if F5
                futures.append(future)

            except ValueError as e: # Catch config fetch errors
                 logger.error(f"Skipping pod {pod} management due to configuration error: {e}")
                 operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="config_error", error=str(e), class_id=class_num_for_log)
                 all_results.append({"identifier": pod_id_str, "class_identifier": class_num_for_log, "status": "failed", "failed_step": "config_error", "error_message": str(e)})
            except Exception as e:
                logger.error(f"Error submitting management task for pod {pod}: {e}", exc_info=True)
                operation_logger.log_pod_status(pod_id=pod_id_str, status="failed", step="submit_task_error", error=str(e), class_id=class_num_for_log)
                all_results.append({"identifier": pod_id_str, "class_identifier": class_num_for_log, "status": "failed", "failed_step": "submit_task_error", "error_message": str(e)})

    # Wait for tasks and process results
    task_results = wait_for_tasks(futures, description=f"VM management ({args.operation})")

    # Log status for each pod AFTER completion
    for result_data in task_results:
         operation_logger.log_pod_status(
             pod_id=result_data["identifier"],
             status=result_data["status"], # Assumes wait_for_tasks structures this
             step=result_data["failed_step"],
             error=result_data["error_message"],
             class_id=result_data["class_identifier"]
         )
         # Optionally update DB power status here if desired
         # power_status = "True" if args.operation == "start" else "False"
         # update_database(...) # Need to construct the data dict

    all_results.extend(task_results)
    logger.info("VM management operations submitted.")
    return all_results # Return collected results


# -----------------------------------------------------------------------------
# Main Entry Point and Argument Parsing
# -----------------------------------------------------------------------------
def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    argcomplete.autocomplete(parser)
    subparsers = parser.add_subparsers(dest='command', title='commands', help='Available commands', required=True) # Make command required

    # --- Common Arguments ---
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-v', '--vendor', required=True, help='Vendor code (e.g., pa, cp, f5, av, pr, nu).')
    common_parser.add_argument('-g', '--course', required=True, help='Course configuration name or "?" to list courses for the vendor.')
    common_parser.add_argument('--host', help='Target vSphere host name (from DB). Required for setup/manage/teardown unless listing.')
    common_parser.add_argument('-t', '--tag', default="untagged", help='Tag for grouping the build/management operation.')
    common_parser.add_argument('--verbose', action='store_true', help='Enable verbose debug logging.')
    common_parser.add_argument('-th', '--thread', type=int, default=4, help='Number of concurrent threads for pod operations.')

    pod_range_parser = argparse.ArgumentParser(add_help=False)
    pod_range_parser.add_argument('-s', '--start-pod', type=int, help='Starting pod number. Required for most operations.')
    pod_range_parser.add_argument('-e', '--end-pod', type=int, help='Ending pod number. Required for most operations.')

    f5_parser = argparse.ArgumentParser(add_help=False)
    f5_parser.add_argument('-cn', '--class_number', type=int, help='Required for F5 vendor/courses.')

    # --- Setup Command ---
    setup_parser = subparsers.add_parser('setup', help='Set up the lab environment.', parents=[common_parser, pod_range_parser, f5_parser])
    setup_parser.add_argument('-c', '--component', help='Comma-separated components to build or "?" to list components for the course.')
    setup_parser.add_argument('-ds', '--datastore', default="vms", help='Folder for pod storage (currently informational).')
    setup_parser.add_argument('-r', '--re-build', action='store_true', help='Rebuild existing resources (delete and recreate).')
    setup_parser.add_argument('-mem', '--memory', type=int, help='Memory override for F5 bigip component (MB).')
    setup_parser.add_argument('--full', action='store_true', help='Create full clones instead of linked clones.')
    # setup_parser.add_argument('--clonefrom', action='store_true', help='Clone from an existing pod (Not implemented).') # Marked as not implemented
    setup_parser.add_argument('--monitor-only', action='store_true', help='Create only PRTG monitors for the pod range.')
    setup_parser.add_argument('--prtg-server', help='Specify a PRTG server name (from DB config) for monitor creation.')
    setup_parser.add_argument('--perm', action='store_true', help='CheckPoint Only: Run only permission assignment functions.')
    setup_parser.add_argument('--db-only', action='store_true', help='Only update the allocation database; skip build operations.')
    setup_parser.set_defaults(func=setup_environment)

    # --- Manage Command ---
    manage_parser = subparsers.add_parser('manage', help='Manage VM power states.', parents=[common_parser, pod_range_parser, f5_parser])
    manage_parser.add_argument('-c', '--component', help='Comma-separated components to manage or "?" to list.')
    manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True, help='VM power operation.')
    manage_parser.set_defaults(func=manage_environment)

    # --- Teardown Command ---
    teardown_parser = subparsers.add_parser('teardown', help='Tear down the lab environment.', parents=[common_parser, pod_range_parser, f5_parser])
    teardown_parser.add_argument('--monitor-only', action='store_true', help='Delete only PRTG monitors and DB entries.')
    teardown_parser.add_argument('--db-only', action='store_true', help='Only delete allocation database entries; skip teardown operations.')
    # teardown_parser.add_argument('--prtg-server', help='Specify PRTG server for monitor deletion (usually not needed).') # Not typically needed for delete
    teardown_parser.set_defaults(func=teardown_environment)


    args = parser.parse_args()

    # --- Initialize Operation Logger ---
    # Convert Namespace to dict for logging, removing potentially sensitive info
    args_dict = vars(args).copy()
    args_dict.pop('MONGO_PASSWORD', None) # Example sanitation
    args_dict.pop('func', None) # <<< FIX: Remove the function object >>>
    # Create logger instance AFTER parsing args
    try:
        operation_logger = OperationLogger(args.command, args_dict)
    except Exception as e:
        # Log critical failure if OperationLogger fails to init (might not reach MongoDB)
        logging.critical(f"Failed to initialize OperationLogger: {e}", exc_info=True)
        print(f"CRITICAL: Failed to initialize OperationLogger: {e}", file=sys.stderr) # Also print to stderr
        sys.exit(1)

    # --- Logging Level ---
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger.setLevel(log_level)
    # Also set level for manager/lab loggers if they exist and you want them controlled
    try:
        logging.getLogger("VmManager").setLevel(log_level)
        logging.getLogger("NetworkManager").setLevel(log_level)
        logging.getLogger("ResourcePoolManager").setLevel(log_level)
        logging.getLogger("FolderManager").setLevel(log_level)
        logging.getLogger("PRTGManager").setLevel(log_level)
    except Exception: # Catch if loggers haven't been created yet
        pass
    # Add other loggers as needed

    # --- Handle Listing Commands ---
    if args.course == "?":
        list_vendor_courses(args.vendor) # Exits after listing

    # Component listing requires course config, fetch it (already done in setup_environment)
    # But need to handle it here before calling the main function if component == '?'
    if getattr(args, 'component', None) == "?":
        if args.command not in ['setup', 'manage']: # Only setup/manage support component listing
             logger.error(f"Command '{args.command}' does not support component listing ('?').")
             sys.exit(1)
        # Call the appropriate function which handles '?' internally
        # No need for separate logic here as the called function will exit
        pass # Let the command function handle '?'

    # --- Validate Core Arguments for Action Commands ---
    # Check if core arguments (host, pods) are needed based on command and flags
    needs_core_args = args.command in ['setup', 'manage', 'teardown'] and \
                      not getattr(args, 'db_only', False) and \
                      not getattr(args, 'monitor_only', False) and \
                      getattr(args, 'course', '') != '?' and \
                      getattr(args, 'component', '') != '?'

    is_setup_perm_only = args.command == 'setup' and getattr(args, 'perm', False)

    if needs_core_args or is_setup_perm_only:
        missing_args = []
        if not args.host: missing_args.append("--host")

        # Pod range needed unless it's F5 class-only teardown/setup without pods
        is_f5_class_only_op = args.vendor.lower() == 'f5' and args.start_pod is None and args.end_pod is None
        is_f5_class_teardown = args.command == 'teardown' and is_f5_class_only_op
        # Allow setup of F5 class only without pods specified
        is_f5_class_setup = args.command == 'setup' and is_f5_class_only_op

        if not (is_f5_class_teardown or is_f5_class_setup) :
            # Ensure start/end pod are provided if not F5 class-only op
            # Check they are not None before checking if they are required
            if args.start_pod is None: missing_args.append("--start-pod")
            if args.end_pod is None: missing_args.append("--end-pod")


        # F5 class number check (check even if pods aren't specified for class-only ops)
        if args.vendor.lower() == "f5":
             # Course config might not be fetched yet if only listing vendor courses
             course_config_temp = None
             if args.course != '?':
                  try: course_config_temp = fetch_and_prepare_course_config(args.course)
                  except: pass # Ignore errors here, focus on arg validation

             if args.class_number is None:
                  # Check if course name implies f5 even if vendor arg wasn't f5
                  if course_config_temp and "f5" in course_config_temp.get("course_name","").lower():
                       missing_args.append("--class_number (required for F5 courses)")
                  elif args.vendor.lower() == "f5": # Explicit vendor requires it
                       missing_args.append("--class_number (required for F5 vendor)")


        if missing_args:
            err_msg = f"Missing required arguments for '{args.command}': {', '.join(missing_args)}"
            logger.critical(err_msg)
            # Log failure using operation_logger if possible
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="missing_arguments", error=err_msg)
            operation_logger.finalize("failed", 0, 0) # Finalize immediately
            sys.exit(1)

        # Validate pod range logic if pods are provided
        if args.start_pod is not None and args.end_pod is not None and args.start_pod > args.end_pod:
            err_msg = "Error: --start-pod cannot be greater than --end-pod."
            logger.critical(err_msg)
            operation_logger.log_pod_status(pod_id="arg_validation", status="failed", step="invalid_pod_range", error=err_msg)
            operation_logger.finalize("failed", 0, 0)
            sys.exit(1)

    # --- Execute Command ---
    start_time = time.perf_counter()
    overall_status = "failed" # Default status
    total_success = 0
    total_failure = 0
    all_pod_results = [] # Collect results from the command function

    try:
        logger.info(f"Executing command: {args.command} (Run ID: {operation_logger.run_id})")
        # Pass operation_logger to the handler function
        # No need to pass vcenter/host details, the functions get them if needed
        all_pod_results = args.func(args, operation_logger) # Call the function assigned by set_defaults


        # Determine overall status based on pod results
        if isinstance(all_pod_results, list): # Check if we got results back
            total_success = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "success")
            total_failure = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "failed")
            total_skipped = sum(1 for r in all_pod_results if isinstance(r, dict) and r.get("status") == "skipped")

            if total_failure > 0:
                overall_status = "completed_with_errors"
            elif total_success > 0:
                overall_status = "completed"
            elif total_skipped > 0 and total_success == 0 and total_failure == 0:
                 overall_status = "completed" # Treat skipped-only runs as completed (e.g., db_only)
            else: # No successes, no failures, no skips -> likely no tasks run
                overall_status = "completed_no_tasks"

        else:
             logger.warning("Command function did not return a list of pod results. Summary may be inaccurate.")
             overall_status = "unknown" # Could not determine status

    except KeyboardInterrupt:
        print("\nProgram terminated by user (Ctrl+C).")
        logger.warning("Program terminated by user.")
        overall_status = "terminated_by_user"
        # Finalize will be called below
        # We exit here cleanly
        sys.exit(0)
    except Exception as e:
        logger.critical(f"An unhandled error occurred during command execution: {e}", exc_info=True)
        overall_status = "failed_exception"
        # Log the exception details if possible via operation logger
        # Use a generic identifier since we don't know the pod context here
        operation_logger.log_pod_status(pod_id="main_exception", status="failed", error=f"{type(e).__name__}: {e}")
        # Finalize will be called below
        sys.exit(1) # Exit with error code
    finally:
        end_time = time.perf_counter()
        duration_minutes = (end_time - start_time) / 60
        logger.info(f"Command '{args.command}' finished in {duration_minutes:.2f} minutes.")
        # Finalize the operation log (ensure it happens even on sys.exit(0) for termination)
        if 'operation_logger' in locals(): # Ensure logger was initialized
             operation_logger.finalize(overall_status, total_success, total_failure)
        else:
             # This case should be rare due to the exit early if init fails
             logger.error("OperationLogger was not initialized, cannot finalize operation log.")


if __name__ == "__main__":
    main()