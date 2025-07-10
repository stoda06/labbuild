"""Database utility functions for MongoDB interaction."""

import logging
from contextlib import contextmanager
from typing import Optional, Dict, List, Any, Generator
import os
from urllib.parse import quote_plus

import pymongo
from pymongo.errors import PyMongoError, ConnectionFailure
from dotenv import load_dotenv

# Import constants
from constants import DB_NAME, ALLOCATION_COLLECTION, HOST_COLLECTION

# Load environment variables for MongoDB connection
load_dotenv() # Assumes .env is in the project root relative to where labbuild.py runs
logger = logging.getLogger('labbuild.db') # Use a specific logger name

MONGO_USER = quote_plus(os.getenv("MONGO_USER", "labbuild_user"))
MONGO_PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD", "$$u1QBd6&372#$rF"))
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_URI = None

if MONGO_HOST:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{DB_NAME}"
else:
    # Log critical error if host is missing, as DB operations will fail
    logger.critical("MONGO_HOST environment variable not set. MongoDB operations will fail.")
    # Optionally raise an error or set MONGO_URI to None and handle elsewhere


@contextmanager
def mongo_client() -> Generator[Optional[pymongo.MongoClient], None, None]:
    """Context manager for MongoDB client. Yields the client or None on failure."""
    client: Optional[pymongo.MongoClient] = None
    if not MONGO_URI:
        logger.error("MongoDB URI not set. Cannot create client.")
        yield None
        return

    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        logger.debug("Opened MongoDB connection.")
        yield client
    except ConnectionFailure as e:
        logger.critical(f"MongoDB connection failed: {e}")
        yield None # Yield None if connection fails
    except Exception as e:
        logger.error(f"Error during MongoDB client context management: {e}", exc_info=True)
        yield None # Yield None on other errors
    finally:
        if client:
            client.close()
            logger.debug("Closed MongoDB connection.")

# --- NEW FUNCTION ---
def get_vcenter_by_host(host_name: str) -> Optional[str]:
    """
    Queries the hosts collection to get the vCenter FQDN for a given host name.
    """
    try:
        with mongo_client() as client:
            if not client:
                logger.error(f"Get vCenter: DB connection failed for host '{host_name}'.")
                return None
            
            db = client[DB_NAME]
            collection = db[HOST_COLLECTION]
            
            logger.debug(f"Searching for host '{host_name}' in collection '{HOST_COLLECTION}'.")
            # Query is case-insensitive for robustness
            doc = collection.find_one({"host_name": host_name.lower()})

            if not doc:
                logger.warning(f"Host '{host_name}' not found in the hosts collection.")
                return None

            vcenter_fqdn = doc.get('vcenter')
            if not vcenter_fqdn:
                logger.warning(f"Host '{host_name}' found, but is missing a 'vcenter' field.")
                return None

            logger.debug(f"Found vCenter '{vcenter_fqdn}' for host '{host_name}'.")
            return vcenter_fqdn

    except PyMongoError as e:
        logger.error(f"Get vCenter (PyMongoError) for host '{host_name}': {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Get vCenter error for host '{host_name}': {e}", exc_info=True)
        return None
# --- END NEW FUNCTION ---

def update_database(data: Dict[str, Any]):
    """
    Update or insert allocation data using targeted upserts to avoid overwriting.
    """
    try:
        with mongo_client() as client:
            if not client:
                logger.error("DB Update: Connection failed.")
                return False

            db = client[DB_NAME]
            collection = db[ALLOCATION_COLLECTION]
            
            tag = data.get("tag")
            course_name = data.get("course_name")
            if not tag or not course_name:
                logger.error("DB Update failed: Missing tag or course_name in data.")
                return False
            
            # --- 1. Prepare the course-level update payload ---
            # Include only fields that are present in the input data
            course_update_payload = {}
            optional_fields = ["vendor", "start_date", "end_date", "trainer_name", "apm_username", "apm_password"]
            for field in optional_fields:
                if data.get(field) is not None:
                    course_update_payload[f"courses.$.{field}"] = data[field]

            # --- 2. Attempt to update an existing course document ---
            # This operation will only succeed if the tag exists AND the course exists within that tag's 'courses' array.
            if course_update_payload:
                result = collection.update_one(
                    {"tag": tag, "courses.course_name": course_name},
                    {"$set": course_update_payload}
                )
                if result.modified_count > 0:
                    logger.info(f"Updated metadata for course '{course_name}' in tag '{tag}'.")

            # --- 3. Upsert pod details for the course ---
            # This loop will update existing pods or add new ones to the course's 'pod_details' array.
            for pod_detail in data.get("pod_details", []):
                pod_key_field = "class_number" if "class_number" in pod_detail else "pod_number"
                pod_key_value = pod_detail.get(pod_key_field)
                if pod_key_value is None:
                    continue

                # First, try to update an existing pod in the pod_details array
                pod_update_result = collection.update_one(
                    {"tag": tag, "courses.course_name": course_name, f"courses.pod_details.{pod_key_field}": pod_key_value},
                    {"$set": {f"courses.$[course].pod_details.$[pod]": pod_detail}},
                    array_filters=[
                        {"course.course_name": course_name},
                        {f"pod.{pod_key_field}": pod_key_value}
                    ]
                )

                # If the pod didn't exist, push it into the array
                if pod_update_result.matched_count == 0:
                    collection.update_one(
                        {"tag": tag, "courses.course_name": course_name},
                        {"$addToSet": {"courses.$.pod_details": pod_detail}}
                    )

            # --- 4. Handle cases where the course or tag itself does not exist ---
            # Check if the tag exists at all. If not, insert it completely.
            tag_exists = collection.count_documents({"tag": tag}) > 0
            if not tag_exists:
                logger.info(f"Tag '{tag}' not found. Creating new tag document.")
                new_tag_entry = {
                    "tag": tag,
                    "courses": [{
                        "course_name": course_name,
                        "vendor": data.get("vendor"),
                        "start_date": data.get("start_date"),
                        "end_date": data.get("end_date"),
                        "trainer_name": data.get("trainer_name"),
                        "apm_username": data.get("apm_username"),
                        "apm_password": data.get("apm_password"),
                        "pod_details": data.get("pod_details", [])
                    }]
                }
                collection.insert_one(new_tag_entry)
                logger.info(f"DB updated: Created new tag '{tag}' with course '{course_name}'.")
                return True

            # If the tag exists, check if the course exists within it. If not, push the course.
            course_exists = collection.count_documents({"tag": tag, "courses.course_name": course_name}) > 0
            if not course_exists:
                logger.info(f"Course '{course_name}' not found in tag '{tag}'. Adding new course entry.")
                new_course_entry = {
                    "course_name": course_name,
                    "vendor": data.get("vendor"),
                    "start_date": data.get("start_date"),
                    "end_date": data.get("end_date"),
                    "trainer_name": data.get("trainer_name"),
                    "apm_username": data.get("apm_username"),
                    "apm_password": data.get("apm_password"),
                    "pod_details": data.get("pod_details", [])
                }
                collection.update_one(
                    {"tag": tag},
                    {"$push": {"courses": new_course_entry}}
                )

            logger.info(f"DB update process complete for tag '{tag}', course '{course_name}'.")
            return True

    except PyMongoError as e:
        logger.error(f"DB update error (PyMongoError): {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"DB update error: {e}", exc_info=True)
        return False

def delete_from_database(tag: str, course_name: Optional[str] = None, pod_number: Optional[int] = None, class_number: Optional[int] = None):
    """Delete an entry, course, or pod from the allocation collection."""
    try:
        with mongo_client() as client:
            if not client: logger.error("DB Delete: Connection failed."); return False
            db = client[DB_NAME]; collection = db[ALLOCATION_COLLECTION]; logger.debug(f"Attempting DB delete: tag='{tag}', course='{course_name}', pod='{pod_number}', class='{class_number}'")
            tag_entry = collection.find_one({"tag": tag})
            if not tag_entry: logger.warning(f"Tag '{tag}' not found for deletion."); return True

            if not course_name and pod_number is None and class_number is None: collection.delete_one({"tag": tag}); logger.info(f"Deleted entire tag '{tag}'."); return True

            courses = tag_entry.get("courses", []); updated_courses = []; modified = False
            if not isinstance(courses, list): logger.warning(f"Tag '{tag}' has malformed courses field."); return False # Cannot process

            for course in courses:
                if not isinstance(course, dict): logger.warning(f"Skipping malformed course in tag '{tag}': {course}"); updated_courses.append(course); continue # Keep malformed? Or skip? Skipping update of this item.

                if course.get("course_name") == course_name:
                    if pod_number is None and class_number is None: modified = True; logger.info(f"Deleting course '{course_name}'."); continue
                    pods = course.get("pod_details", []); updated_pods = []
                    if not isinstance(pods, list): logger.warning(f"Course '{course_name}' malformed pod_details. Cannot process pod deletion."); updated_courses.append(course); continue # Keep course as is

                    for pod in pods:
                        if not isinstance(pod, dict): logger.warning(f"Skipping malformed pod_detail in '{course_name}': {pod}"); updated_pods.append(pod); continue
                        match = False
                        pod_pod_num = pod.get("pod_number"); pod_class_num = pod.get("class_number")
                        if class_number is not None and pod_number is None and pod_class_num == class_number: match = True
                        elif pod_number is not None and pod_pod_num == pod_number:
                            if class_number is None or pod_class_num == class_number: match = True
                        elif class_number is not None and pod_number is not None and pod_class_num == class_number:
                             nested_pods = pod.get("pods", []); updated_nested_pods = [np for np in nested_pods if not (isinstance(np, dict) and np.get("pod_number") == pod_number)]
                             if len(updated_nested_pods) < len(nested_pods): pod["pods"] = updated_nested_pods; modified = True; logger.info(f"Deleting nested pod {pod_number} from class {class_number}."); updated_pods.append(pod)
                             else: updated_pods.append(pod)
                             continue
                        if not match: updated_pods.append(pod)
                        else: modified = True; log_id = f"class {class_number}" if pod_number is None else f"pod {pod_number}"; logger.info(f"Deleting {log_id} from course '{course_name}'.")

                    if updated_pods: course["pod_details"] = updated_pods; updated_courses.append(course)
                    elif "pods" in pod and pod.get("pods"): updated_courses.append(course) # Keep F5 class if nested pods remain
                    else: modified = True; logger.info(f"Course '{course_name}' empty.")
                else: updated_courses.append(course)

            if modified:
                if updated_courses: collection.update_one({"tag": tag}, {"$set": {"courses": updated_courses}}); logger.info(f"Updated tag '{tag}'.")
                else: collection.delete_one({"tag": tag}); logger.info(f"Deleted tag '{tag}'.")
            else: logger.warning(f"No matching item found for deletion under tag '{tag}'.")
            return True
    except PyMongoError as e: logger.error(f"DB delete error (PyMongoError): {e}", exc_info=True); return False
    except Exception as e: logger.error(f"DB delete error: {e}", exc_info=True); return False

def get_test_params_by_tag(tag: str) -> Optional[Dict[str, Any]]:
    """
    Queries the current allocations collection by tag to retrieve parameters for the test suite.

    Args:
        tag: The allocation tag string.

    Returns:
        A dictionary containing test parameters, or None on failure.
    """
    try:
        with mongo_client() as client:
            if not client:
                logger.error("Get Test Params: DB connection failed.")
                return None
            
            db = client[DB_NAME]
            collection = db[ALLOCATION_COLLECTION]
            
            logger.debug(f"Searching for tag '{tag}' in collection '{ALLOCATION_COLLECTION}'.")
            doc = collection.find_one({"tag": tag})

            if not doc:
                logger.warning(f"Tag '{tag}' not found in current allocations.")
                return None

            if not doc.get('courses'):
                logger.warning(f"Document for tag '{tag}' has no 'courses' array.")
                return None
            
            # Assume we always work with the first course in the list
            course_info = doc['courses'][0]
            vendor = course_info.get('vendor')
            course_name = course_info.get('course_name') # <-- Get the course name
            
            if not vendor or not course_name:
                logger.warning(f"Course in tag '{tag}' is missing a 'vendor' or 'course_name' field.")
                return None
            
            # --- THIS IS THE CORRECTED LINE ---
            params = {'vendor': vendor.lower(), 'group': course_name}
            # --- END OF CORRECTION ---
            
            pod_details = course_info.get('pod_details', [])
            if not pod_details:
                logger.warning(f"No pod details found for course in tag '{tag}'.")
                return None

            if vendor.lower() == 'f5':
                # F5 structure parsing
                class_details = pod_details[0]
                params['host'] = class_details.get('host')
                params['class_number'] = class_details.get('class_number')
                pod_numbers = [p.get('pod_number') for p in class_details.get('pods', []) if p.get('pod_number') is not None]
                if not pod_numbers:
                    logger.warning(f"No pods found for F5 class in tag '{tag}'.")
                    return None
                params['start_pod'] = min(pod_numbers)
                params['end_pod'] = max(pod_numbers)
            else:
                # Non-F5 structure parsing
                params['host'] = pod_details[0].get('host')
                pod_numbers = [p.get('pod_number') for p in pod_details if p.get('pod_number') is not None]
                if not pod_numbers:
                    logger.warning(f"No pods found for tag '{tag}'.")
                    return None
                params['start_pod'] = min(pod_numbers)
                params['end_pod'] = max(pod_numbers)
                params['class_number'] = None
            
            # Final validation of extracted parameters
            if not params.get('host') or params.get('start_pod') is None or params.get('end_pod') is None:
                logger.error(f"Could not extract all required test parameters (host, pods) for tag '{tag}'.")
                return None

            return params

    except PyMongoError as e:
        logger.error(f"Get Test Params error (PyMongoError): {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Get Test Params error: {e}", exc_info=True)
        return None

def get_prtg_url(tag: str, course_name: str, pod_number: Optional[int] = None, class_number: Optional[int] = None) -> Optional[str]:
    """Retrieve the PRTG monitor URL from the allocation collection."""
    try:
        with mongo_client() as client:
            if not client: logger.error("Get PRTG URL: DB connection failed."); return None
            db = client[DB_NAME]; collection = db[ALLOCATION_COLLECTION]; logger.debug(f"Searching PRTG URL: tag='{tag}', course='{course_name}', pod='{pod_number}', class='{class_number}'")
            tag_entry = collection.find_one({"tag": tag})
            if not tag_entry: logger.warning(f"Tag '{tag}' not found."); return None

            for course in tag_entry.get("courses", []):
                 if not isinstance(course, dict): continue # Skip malformed
                 if course.get("course_name") == course_name:
                    for pod_detail in course.get("pod_details", []):
                        if not isinstance(pod_detail, dict): continue # Skip malformed
                        pd_class_num = pod_detail.get("class_number"); pd_pod_num = pod_detail.get("pod_number")
                        if class_number is not None and pd_class_num == class_number and pod_number is None:
                             if "prtg_url" in pod_detail: logger.debug(f"Found PRTG URL class '{class_number}'."); return pod_detail.get("prtg_url")
                        elif pod_number is not None and class_number is None and pd_pod_num == pod_number: logger.debug(f"Found PRTG URL pod '{pod_number}'."); return pod_detail.get("prtg_url")
                        elif class_number is not None and pod_number is not None and pd_class_num == class_number:
                            for nested_pod in pod_detail.get("pods", []):
                                if isinstance(nested_pod, dict) and nested_pod.get("pod_number") == pod_number: logger.debug(f"Found PRTG URL F5 pod '{pod_number}'."); return nested_pod.get("prtg_url")

            logger.warning(f"No PRTG URL found matching criteria."); return None
    except PyMongoError as e: logger.error(f"Get PRTG URL error (PyMongoError): {e}", exc_info=True); return None
    except Exception as e: logger.error(f"Get PRTG URL error: {e}", exc_info=True); return None