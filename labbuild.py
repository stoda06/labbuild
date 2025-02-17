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
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from urllib.parse import quote_plus

import pymongo
from dotenv import load_dotenv

# Vendor-specific modules
import labs.manage.vm_operations as vm_operations
import labs.setup.avaya as avaya
import labs.setup.checkpoint as checkpoint
import labs.setup.f5 as f5
import labs.setup.palo as palo
import labs.setup.pr as pr
import labs.setup.nu as nu

from logger.log_config import setup_logger
from managers.vcenter import VCenter
from monitor.prtg import PRTGManager

# -----------------------------------------------------------------------------
# Environment Setup: load environment variables and logging
# -----------------------------------------------------------------------------
load_dotenv()
logger = setup_logger()

# MongoDB credentials and URI
MONGO_USER = quote_plus("labbuild_user")
MONGO_PASSWORD = quote_plus("$$u1QBd6&372#$rF")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/labbuild_db"


# -----------------------------------------------------------------------------
# Database Access
# -----------------------------------------------------------------------------
@contextmanager
def mongo_client():
    """
    Context manager for MongoDB client.
    Yields a pymongo.MongoClient and ensures it is closed after use.
    """
    client = pymongo.MongoClient(MONGO_URI)
    logger.debug("Opened MongoDB connection.")
    try:
        yield client
    finally:
        client.close()
        logger.debug("Closed MongoDB connection.")


def update_database(data: dict):
    """
    Update or insert an entry in the 'currentallocation' collection.
    
    :param data: Dictionary with keys 'tag', 'course_name', and 'pod_details'.
    """
    try:
        with mongo_client() as client:
            db = client["labbuild_db"]
            collection = db["currentallocation"]
            tag = data["tag"]
            course_name = data["course_name"]
            pod_details = data["pod_details"]

            logger.debug("Updating database for tag '%s', course '%s'.", tag, course_name)
            tag_entry = collection.find_one({"tag": tag})
            if tag_entry:
                existing_course = next(
                    (course for course in tag_entry.get("courses", [])
                     if course.get("course_name") == course_name),
                    None
                )
                if existing_course:
                    for new_pod in pod_details:
                        existing_pod = next(
                            (pod for pod in existing_course["pod_details"]
                             if pod.get("pod_number") == new_pod.get("pod_number")),
                            None
                        )
                        if existing_pod:
                            existing_pod.update(new_pod)
                            logger.debug("Updated pod %s for course '%s'.", new_pod.get("pod_number"), course_name)
                        else:
                            existing_course["pod_details"].append(new_pod)
                            logger.debug("Added new pod %s for course '%s'.", new_pod.get("pod_number"), course_name)
                else:
                    tag_entry.setdefault("courses", []).append({
                        "course_name": course_name,
                        "pod_details": pod_details
                    })
                    logger.debug("Added new course '%s' to tag '%s'.", course_name, tag)
                collection.update_one({"tag": tag}, {"$set": {"courses": tag_entry["courses"]}})
            else:
                new_entry = {
                    "tag": tag,
                    "courses": [{
                        "course_name": course_name,
                        "pod_details": pod_details
                    }]
                }
                collection.insert_one(new_entry)
                logger.debug("Inserted new tag entry '%s' into the database.", tag)
            logger.info("Database updated for tag '%s' and course '%s'.", tag, course_name)
    except Exception as e:
        logger.error("Database update error: %s", e)


def delete_from_database(tag: str, course_name: str = None, pod_number: int = None, class_number: int = None):
    """
    Delete an entry from the 'currentallocation' collection.

    Depending on the parameters, deletes an entire tag, a course under a tag,
    or a specific pod.
    
    :param tag: The tag identifier.
    :param course_name: (Optional) Course name.
    :param pod_number: (Optional) Pod number.
    :param class_number: (Optional) Class number for F5 setups.
    """
    try:
        with mongo_client() as client:
            db = client["labbuild_db"]
            collection = db["currentallocation"]
            logger.debug("Deleting from database for tag '%s'.", tag)

            if not course_name and pod_number is None:
                collection.delete_one({"tag": tag})
                logger.info("Deleted entire tag '%s'.", tag)
                return

            tag_entry = collection.find_one({"tag": tag})
            if not tag_entry:
                logger.warning("Tag '%s' not found during deletion.", tag)
                return

            if course_name and pod_number is None:
                updated_courses = [
                    course for course in tag_entry.get("courses", [])
                    if course.get("course_name") != course_name
                ]
                if updated_courses:
                    collection.update_one({"tag": tag}, {"$set": {"courses": updated_courses}})
                else:
                    collection.delete_one({"tag": tag})
                logger.info("Deleted course '%s' under tag '%s'.", course_name, tag)
                return

            if course_name and pod_number is not None:
                for course in tag_entry.get("courses", []):
                    if course.get("course_name") == course_name:
                        updated_pods = [
                            pod for pod in course.get("pod_details", [])
                            if pod.get("pod_number") != pod_number or 
                               (class_number is not None and pod.get("class_number") != class_number)
                        ]
                        course["pod_details"] = updated_pods
                        break

                tag_entry["courses"] = [
                    course for course in tag_entry.get("courses", [])
                    if course.get("pod_details")
                ]
                if tag_entry["courses"]:
                    collection.update_one({"tag": tag}, {"$set": {"courses": tag_entry["courses"]}})
                else:
                    collection.delete_one({"tag": tag})
                logger.info("Deleted pod '%s' from course '%s' under tag '%s'.", pod_number, course_name, tag)
    except Exception as e:
        logger.error("Error during deletion: %s", e)


def get_prtg_url(tag: str, course_name: str, pod_number: int):
    """
    Retrieve the PRTG monitor URL from the 'currentallocation' collection.

    :param tag: Tag identifier.
    :param course_name: Course name.
    :param pod_number: Pod number.
    :return: PRTG URL string if found; otherwise, None.
    """
    try:
        with mongo_client() as client:
            db = client["labbuild_db"]
            collection = db["currentallocation"]
            logger.debug("Searching for PRTG URL for tag '%s', course '%s', pod '%s'.", tag, course_name, pod_number)
            tag_entry = collection.find_one({"tag": tag})
            if not tag_entry:
                logger.warning("Tag '%s' not found while searching for PRTG URL.", tag)
                return None

            for course in tag_entry.get("courses", []):
                if course.get("course_name") == course_name:
                    for pod in course.get("pod_details", []):
                        if pod.get("pod_number") == pod_number:
                            logger.debug("Found PRTG URL for pod '%s' in course '%s'.", pod_number, course_name)
                            return pod.get("prtg_url")
            logger.warning("No PRTG URL found for tag '%s', course '%s', pod '%s'.", tag, course_name, pod_number)
            return None
    except Exception as e:
        logger.error("Error retrieving PRTG URL: %s", e)
        return None


# -----------------------------------------------------------------------------
# Configuration & Host Helpers
# -----------------------------------------------------------------------------
def fetch_and_prepare_course_config(setup_name: str, pod: int = None, f5_class: int = None) -> dict:
    """
    Retrieve and prepare the course configuration from MongoDB.
    Optionally replaces placeholders (e.g. "{X}" for pod and "{Y}" for class).

    :param setup_name: Course configuration name.
    :param pod: Optional pod number.
    :param f5_class: Optional F5 class number.
    :return: The configuration dictionary.
    """
    try:
        with mongo_client() as client:
            db = client["labbuild_db"]
            collection = db["courseconfig"]
            logger.debug("Fetching course configuration for '%s'.", setup_name)
            config = collection.find_one({"course_name": setup_name})
            if config is None:
                logger.error("Course configuration '%s' not found.", setup_name)
                sys.exit(1)
            config.pop("_id", None)
            config_str = json.dumps(config)
            if pod is not None:
                config_str = config_str.replace("{X}", str(pod))
            if f5_class is not None:
                config_str = config_str.replace("{Y}", str(f5_class))
            logger.debug("Course configuration for '%s' prepared successfully.", setup_name)
            return json.loads(config_str)
    except pymongo.errors.PyMongoError as e:
        logger.error("Error fetching course configuration: %s", e)
        sys.exit(1)


def extract_components(course_config: dict) -> list:
    """
    Extract unique component names from the course configuration.

    :param course_config: Configuration dictionary.
    :return: List of unique component names.
    """
    components = []
    if "components" in course_config:
        components.extend(comp["component_name"] for comp in course_config["components"])
    if "groups" in course_config:
        for group in course_config["groups"]:
            if "component" in group:
                components.extend(comp["component_name"] for comp in group["component"])
    logger.debug("Extracted components: %s", components)
    return list(set(components))


def get_host_by_name(hostname: str) -> dict:
    """
    Retrieve host details from the 'host' collection.

    :param hostname: Host name.
    :return: Host details dictionary, or None if not found.
    """
    try:
        with mongo_client() as client:
            db = client["labbuild_db"]
            collection = db["host"]
            logger.debug("Fetching details for host '%s'.", hostname)
            host = collection.find_one({"host_name": hostname})
            if host is None:
                logger.error("Host '%s' not found.", hostname)
                return None
            host.pop("_id", None)
            logger.debug("Found host details for '%s'.", hostname)
            return host
    except pymongo.errors.PyMongoError as e:
        logger.error("Error fetching host details: %s", e)
        return None


# -----------------------------------------------------------------------------
# vCenter Helper: Centralize vCenter connection logic
# -----------------------------------------------------------------------------
def get_vcenter_instance(host_details: dict) -> VCenter:
    """
    Create and connect a vCenter service instance using the host details
    and environment variables.

    :param host_details: Dictionary containing host information, including 'vcenter'.
    :return: A connected VCenter instance.
    """
    vc_host = host_details["vcenter"]
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    service_instance = VCenter(vc_host, vc_user, vc_password, 443)
    service_instance.connect()
    return service_instance


# -----------------------------------------------------------------------------
# Monitor & Database Update Helper
# -----------------------------------------------------------------------------
def update_monitor_and_database(pod_config: dict, args, data: dict, extra_details: dict = None):
    """
    Add a PRTG monitor for the pod configuration and update the database.

    :param pod_config: Pod configuration dictionary.
    :param args: Command-line arguments.
    :param data: Data dictionary for the database update.
    :param extra_details: Optional extra details to merge into the pod record.
    """
    with mongo_client() as client:
        vendor_shortcode = pod_config["vendor_shortcode"]
        if vendor_shortcode == "cp":
            prtg_url = checkpoint.add_monitor(pod_config, client)
        elif vendor_shortcode == "pa":
            prtg_url = palo.add_monitor(pod_config, client)
        else:
            prtg_url = PRTGManager.add_monitor(pod_config, client)
        logger.debug("PRTG monitor added with URL: %s", prtg_url)
    pod_details = {
        "pod_number": pod_config.get("pod_number"),
        "pod_host": args.host,
        "poweron": "True",
        "prtg_url": prtg_url
    }
    if extra_details:
        pod_details.update(extra_details)
    data["pod_details"].append(pod_details)
    logger.info("Updating database with pod details: %s", pod_details)
    update_database(data)


# -----------------------------------------------------------------------------
# Utility: Wait for Futures (Improved)
# -----------------------------------------------------------------------------
def wait_for_futures(futures: list):
    """
    Wait for all futures to complete. If a future fails, log additional details such as the pod number
    and the actual error message.

    Each future is expected to have a 'pod_number' attribute if applicable; otherwise, it will be logged as 'Unknown'.

    :param futures: List of futures.
    """
    for future in futures:
        try:
            result = future.result()
            pod_number = getattr(future, "pod_number", "Unknown")
            logger.debug("Task for pod %s completed with result: %s", pod_number, result)
        except Exception as e:
            pod_number = getattr(future, "pod_number", "Unknown")
            logger.error("Task for pod %s failed with error: %s", pod_number, e, exc_info=True)


# -----------------------------------------------------------------------------
# Callback for Successful Build
# -----------------------------------------------------------------------------
def on_success_update(future, pod_config, args, data, extra_details=None):
    """
    Callback to update the monitor and database only if the build succeeded.
    
    :param future: The completed future.
    :param pod_config: Pod configuration used for the build.
    :param args: Command-line arguments.
    :param data: Data dictionary for the database update.
    :param extra_details: Optional extra details.
    """
    pod_number = getattr(future, "pod_number", "Unknown")
    if future.exception() is None:
        logger.info("Build successful for pod %s. Updating monitor and database.", pod_number)
        update_monitor_and_database(pod_config, args, data, extra_details)
    else:
        logger.error("Build failed for pod %s. Skipping monitor and database update.", pod_number)


# -----------------------------------------------------------------------------
# Unified Vendor Operations: Setup and Teardown
# -----------------------------------------------------------------------------
def vendor_setup(service_instance, host_details, args, course_config, selected_components, data):
    """
    Unified vendor setup routine that dispatches vendor‑specific build operations.
    For F5, the class is built first (synchronously) then pods are built.
    For other vendors, pods are built asynchronously.
    """
    vendor = course_config.get("vendor_shortcode")
    logger.info("Dispatching setup for vendor '%s'.", vendor)

    if vendor == "f5":
        # Build F5 class first
        class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number)
        class_config["host_fqdn"] = host_details["fqdn"]
        class_config["class_number"] = args.class_number
        class_config["class_name"] = f"f5-class{args.class_number}"
        logger.debug("Building F5 class '%s'.", class_config["class_name"])
        f5.build_class(service_instance, class_config, rebuild=args.re_build, full=args.full,
                      selected_components=selected_components)

        # Then build each pod synchronously
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            logger.debug("Preparing configuration for F5 pod %s.", pod)
            pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number)
            pod_config["host_fqdn"] = host_details["fqdn"]
            pod_config["class_number"] = args.class_number
            pod_config["pod_number"] = str(pod)
            try:
                f5.build_pod(service_instance, pod_config, mem=args.memory, rebuild=args.re_build,
                             full=args.full, selected_components=selected_components)
                logger.debug("F5 pod %s built successfully.", pod)
            except Exception as e:
                logger.error("F5 build for pod %s failed: %s", pod, e, exc_info=True)
                continue
            # Update monitor and database immediately after synchronous build
            update_monitor_and_database(pod_config, args, data, extra_details={"class_number": args.class_number})
    else:
        # For vendors other than F5, build pods asynchronously
        futures = []
        with ThreadPoolExecutor(max_workers=args.thread) as executor:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                course_name_lower = pod_config.get("course_name", "").lower()
                build_func = None

                if vendor == "cp":
                    build_func = checkpoint.build_cp_pod
                elif vendor == "pa":
                    if "cortex" in course_name_lower:
                        build_func = palo.build_cortex_pod
                    elif "1100-210" in course_name_lower:
                        build_func = palo.build_1100_210_pod
                    elif "1110" in course_name_lower:
                        build_func = palo.build_1110_pod
                    elif "1100-220" in course_name_lower:
                        build_func = palo.build_1100_220_pod
                    else:
                        logger.error("Unsupported Palo course: %s", course_name_lower)
                        continue
                elif vendor == "av":
                    if "aura" in course_name_lower:
                        build_func = avaya.build_aura_pod
                    elif "ipo" in course_name_lower:
                        build_func = avaya.build_ipo_pod
                    else:
                        logger.error("Unsupported Avaya course: %s", course_name_lower)
                        continue
                elif vendor == "prtg":
                    build_func = pr.build_pr_pod
                elif vendor == "nu":
                    build_func = nu.build_nu_pod
                else:
                    logger.error("Unsupported vendor: %s", vendor)
                    continue

                # Handle differences in parameters for some vendors
                if vendor == "cp":
                    future = executor.submit(
                        build_func,
                        service_instance,
                        pod_config,
                        rebuild=args.re_build,
                        thread=args.thread,
                        full=args.full,
                        selected_components=selected_components
                    )
                elif vendor == "av":
                    # For Avaya, only pass extra parameters if "ipo" is in course name
                    if "ipo" in course_name_lower:
                        future = executor.submit(
                            build_func,
                            service_instance,
                            pod_config,
                            rebuild=args.re_build,
                            selected_components=selected_components
                        )
                    else:
                        future = executor.submit(build_func, service_instance, pod_config)
                else:
                    future = executor.submit(
                        build_func,
                        service_instance,
                        pod_config,
                        rebuild=args.re_build,
                        full=args.full,
                        selected_components=selected_components
                    )

                future.pod_number = pod  # Attach pod number to the future
                future.add_done_callback(lambda fut, p_config=pod_config: on_success_update(fut, p_config, args, data))
                futures.append(future)
        wait_for_futures(futures)


def vendor_teardown(service_instance, host_details, args, course_config):
    """
    Unified vendor teardown routine that dispatches vendor‑specific teardown operations.
    For F5, only the class teardown is performed; for other vendors, pods are torn down asynchronously.
    """
    vendor = course_config.get("vendor_shortcode")
    logger.info("Dispatching teardown for vendor '%s'.", vendor)

    if vendor == "f5":
        class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number)
        class_config["host_fqdn"] = host_details["fqdn"]
        class_config["class_name"] = f"f5-class{args.class_number}"
        logger.debug("Tearing down F5 class '%s'.", class_config["class_name"])
        f5.teardown_class(service_instance, class_config)
        with mongo_client() as client:
            prtg_url = get_prtg_url(args.tag, class_config["course_name"], None)
            PRTGManager.delete_monitor(prtg_url, client)
        delete_from_database(args.tag, course_name=class_config["course_name"], class_number=args.class_number)
    else:
        futures = []
        with ThreadPoolExecutor() as executor:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                course_name_lower = pod_config.get("course_name", "").lower()
                teardown_func = None

                if vendor == "cp":
                    teardown_func = checkpoint.teardown_pod
                elif vendor == "pa":
                    if "1110" in course_name_lower:
                        teardown_func = palo.teardown_1110
                    elif "1100" in course_name_lower:
                        teardown_func = palo.teardown_1100
                    elif "cortex" in course_name_lower:
                        teardown_func = palo.teardown_cortex
                    else:
                        logger.error("Unsupported Palo course for teardown: %s", course_name_lower)
                        continue
                elif vendor == "av":
                    if "ipo" in course_name_lower:
                        teardown_func = avaya.teardown_ipo
                    elif "aura" in course_name_lower:
                        teardown_func = avaya.teardown_aura
                    else:
                        logger.error("Unsupported Avaya course for teardown: %s", course_name_lower)
                        continue
                elif vendor == "prtg":
                    teardown_func = pr.teardown_pr_pod
                elif vendor == "nu":
                    teardown_func = nu.teardown_nu_pod
                    continue
                else:
                    logger.error("Unsupported vendor for teardown: %s", vendor)
                    continue

                future = executor.submit(teardown_func, service_instance, pod_config)
                future.pod_number = pod  # Attach pod number to the future
                with mongo_client() as client:
                    prtg_url = get_prtg_url(args.tag, pod_config["course_name"], pod)
                    PRTGManager.delete_monitor(prtg_url, client)
                delete_from_database(args.tag, course_name=pod_config["course_name"], pod_number=pod)
                futures.append(future)
        wait_for_futures(futures)


# -----------------------------------------------------------------------------
# Helper to list courses for a given vendor
# -----------------------------------------------------------------------------
def list_vendor_courses(vendor: str):
    """
    List available courses for the given vendor by querying the "courseconfig" collection
    from the "labbuild_db" MongoDB database.

    Each document in the collection is expected to have a "vendor_shortcode" field and a "course_name" field.
    If a document's vendor_shortcode matches the provided vendor (case insensitive), its course_name is returned.

    If matching courses are found, they are printed; otherwise, an appropriate message is printed.
    """
    try:
        with mongo_client() as client:
            db = client["labbuild_db"]
            collection = db["courseconfig"]
            # Query for documents where vendor_shortcode matches vendor (case-insensitive)
            courses_cursor = collection.find({"vendor_shortcode": vendor.lower()})
            courses = [doc["course_name"] for doc in courses_cursor if "course_name" in doc]
            
            if courses:
                print(f"Available courses for vendor '{vendor}':")
                for course in courses:
                    print(f"  - {course}")
            else:
                print(f"No courses found for vendor '{vendor}'.")
    except Exception as e:
        print(f"Error accessing course configurations: {e}")
    
    sys.exit(0)


# -----------------------------------------------------------------------------
# Environment Operations: Setup, Teardown, and Manage
# -----------------------------------------------------------------------------
def setup_environment(args):
    """
    Set up the lab environment based on provided arguments.
    
    Validates components, establishes a vCenter connection, and dispatches
    the unified vendor‑specific setup operation.
    
    :param args: Parsed command-line arguments.
    """
    # If --course is "?" list available courses and exit.
    if args.course == "?":
        list_vendor_courses(args.vendor)

    if args.component != "?":
        logger.info("Fetching course configuration for setup.")
    course_config = fetch_and_prepare_course_config(args.course)

    # If --component is "?" list available components for the provided course.
    if args.component == "?":
        comps = extract_components(course_config)
        print(f"Available components for course '{args.course}':")
        for comp in comps:
            print(f"  - {comp}")
        sys.exit(0)

    selected_components = None
    if args.component:
        selected_components = [comp.strip() for comp in args.component.split(",")]
        available = extract_components(course_config)
        invalid = [comp for comp in selected_components if comp not in available]
        if invalid:
            logger.error("Invalid components specified: %s", ", ".join(invalid))
            sys.exit(1)

    host_details = get_host_by_name(args.host)
    if not host_details:
        logger.error("Host details could not be retrieved for host '%s'.", args.host)
        sys.exit(1)
    
    # --monitor-only branch: Skip build and create monitors only.
    if str(args.monitor_only).lower() == "true":
        logger.info("Monitor-only mode enabled. Creating monitors without building pods.")
        data = {"tag": args.tag, "course_name": args.course, "pod_details": []}
        vendor = course_config.get("vendor_shortcode")
        
        if vendor == "f5":
            if not args.class_number:
                logger.error("Class number is required for f5 courses in monitor-only mode.")
                sys.exit(1)
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["class_number"] = args.class_number
                pod_config["pod_number"] = str(pod)
                update_monitor_and_database(pod_config, args, data, extra_details={"class_number": args.class_number})
        else:
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                update_monitor_and_database(pod_config, args, data)
        logger.info("Monitor-only process complete.")
        sys.exit(0)

    logger.info("Connecting to vCenter for host '%s'.", args.host)
    service_instance = get_vcenter_instance(host_details)
    data = {"tag": args.tag, "course_name": args.course, "pod_details": []}
    vendor_setup(service_instance, host_details, args, course_config, selected_components, data)


def teardown_environment(args):
    """
    Tear down the lab environment based on provided arguments.
    
    Dispatches the unified vendor‑specific teardown operation and removes
    monitor and database entries.
    
    :param args: Parsed command-line arguments.
    """
    logger.info("Fetching host details for teardown.")
    host_details = get_host_by_name(args.host)
    if not host_details:
        logger.error("Host details could not be retrieved for host '%s'.", args.host)
        sys.exit(1)

    logger.info("Connecting to vCenter for teardown.")
    service_instance = get_vcenter_instance(host_details)
    course_config = fetch_and_prepare_course_config(args.course)
    vendor_teardown(service_instance, host_details, args, course_config)
    logger.info("Teardown process complete.")


def manage_environment(args):
    """
    Manage VM operations (start, stop, reboot) on a range of pods.
    
    :param args: Parsed command-line arguments.
    """
    logger.info("Fetching host details for management operation.")
    host_details = get_host_by_name(args.host)
    if not host_details:
        logger.error("Host details could not be retrieved for host '%s'.", args.host)
        sys.exit(1)

    logger.info("Connecting to vCenter for management operation.")
    service_instance = get_vcenter_instance(host_details)
    course_config = fetch_and_prepare_course_config(args.course)

    data = {"tag": args.tag, "course_name": args.course, "pod_details": []}
    selected_components = None
    if args.component:
        if args.component == "?":
            comps = extract_components(course_config)
            print(f"Available components for course '{args.course}':")
            for comp in comps:
                print(f"  - {comp}")
            sys.exit(0)
        else:
            selected_components = [comp.strip() for comp in args.component.split(",")]
            available = extract_components(course_config)
            invalid = [comp for comp in selected_components if comp not in available]
            if invalid:
                logger.error("Invalid components specified: %s", ", ".join(invalid))
                sys.exit(1)

    futures = []
    logger.info("Dispatching VM management operations (%s) for pods %s to %s.",
                args.operation, args.start_pod, args.end_pod)
    with ThreadPoolExecutor() as executor:
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
            future = executor.submit(
                vm_operations.perform_vm_operations,
                service_instance,
                pod_config,
                args.operation,
                selected_components
            )
            future.pod_number = pod  # Attach pod number to the future
            futures.append(future)
            power_status = "True" if args.operation == "start" else "False"
            pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": power_status}
            data["pod_details"].append(pod_details)
            update_database(data)
        wait_for_futures(futures)
    logger.info("VM management operations complete.")


# -----------------------------------------------------------------------------
# Main Entry Point and Argument Parsing
# -----------------------------------------------------------------------------
def main():
    """
    Main entry point. Parses arguments and dispatches to setup, manage, or teardown.
    """
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    argcomplete.autocomplete(parser)
    subparsers = parser.add_subparsers(dest='command', title='commands', help='Available commands')

    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up the lab environment')
    # New vendor argument
    setup_parser.add_argument('-v', '--vendor', help='Vendor code (e.g., pa, cp, f5, etc.)')
    setup_parser.add_argument('-g', '--course', help='Course configuration name.')
    setup_parser.add_argument('-cn', '--class_number', help='Required if vendor is f5 or course contains "f5".')
    setup_parser.add_argument('-s', '--start-pod', help='Starting pod number.')
    setup_parser.add_argument('-e', '--end-pod', help='Ending pod number.')
    setup_parser.add_argument('-c', '--component', help='Comma-separated components or "?" for a list.')
    setup_parser.add_argument('--host', help='Host name for pod creation.')
    setup_parser.add_argument('-ds', '--datastore', default="vms", help='Folder for pod storage.')
    setup_parser.add_argument('-th', '--thread', type=int, default=4, help='Number of cloning threads.')
    setup_parser.add_argument('-r', '--re-build', action='store_true', help='Rebuild existing resources.')
    setup_parser.add_argument('--verbose', action='store_true', help='Enable verbose output.')
    setup_parser.add_argument('-q', '--quiet', action='store_true', help='Suppress output except warnings/errors.')
    setup_parser.add_argument('-mem', '--memory', type=int, default=None, help='Memory for F5 bigip component.')
    setup_parser.add_argument('--full', action='store_true', help='Create full clones.')
    setup_parser.add_argument('--clonefrom', action='store_true', help='Clone from an existing pod.')
    setup_parser.add_argument('-t', '--tag', default="untagged", help='Tag for the pod range.')
    setup_parser.add_argument('--monitor-only', action='store_true', help='Create only monitors for the pod range.')

    # Manage command
    manage_parser = subparsers.add_parser('manage', help='Manage the lab environment.')
    manage_parser.add_argument('-v', '--vendor', help='Vendor code (e.g., pa, cp, f5, etc.)')
    manage_parser.add_argument('-cn', '--class_number', help='Class number for f5 courses.')
    manage_parser.add_argument('-g', '--course', help='Course configuration name.')
    manage_parser.add_argument('-c', '--component', help='Comma-separated components or "?" for a list.')
    manage_parser.add_argument('-s', '--start-pod', help='Starting pod number.')
    manage_parser.add_argument('-e', '--end-pod', help='Ending pod number.')
    manage_parser.add_argument('--host', help='Host name where pods reside.')
    manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True,
                               help='VM operation to perform.')
    manage_parser.add_argument('-t', '--tag', default="untagged", help='Tag for the pod range.')
    manage_parser.add_argument('--verbose', action='store_true', help='Enable verbose output.')

    # Teardown command
    teardown_parser = subparsers.add_parser('teardown', help='Tear down the lab environment.')
    teardown_parser.add_argument('-v', '--vendor', help='Vendor code (e.g., pa, cp, f5, etc.)')
    teardown_parser.add_argument('-g', '--course', help='Course configuration name.')
    teardown_parser.add_argument('-cn', '--class_number', help='Required if vendor is f5 or course contains "f5".')
    teardown_parser.add_argument('-s', '--start-pod', help='Starting pod number.')
    teardown_parser.add_argument('-e', '--end-pod', help='Ending pod number.')
    teardown_parser.add_argument('--host', help='Host name where pods reside.')
    teardown_parser.add_argument('-t', '--tag', default="untagged", help='Tag for the pod range.')
    teardown_parser.add_argument('--verbose', action='store_true', help='Enable verbose output.')

    args = parser.parse_args()

    # Ensure vendor is provided.
    if not args.vendor:
        print("Error: --vendor is required.")
        sys.exit(1)

    # Check that if vendor is "f5" or the course name contains "f5",
    # then --class_number is required unless the course or component is in lookup mode ("?").
    if (args.vendor.lower() == "f5" or (args.course and "f5" in args.course.lower())) \
       and args.course != "?" and args.component != "?" and not args.class_number:
        print("Error: --class_number is required when vendor is 'f5' or the course name contains 'f5'.")
        sys.exit(1)

    # When listing components, ensure a valid course (not "?") is provided.
    if getattr(args, "component", None) == "?" and (not args.course or args.course == "?"):
        print("Error: When listing components, a valid --course value must be provided (cannot be '?').")
        sys.exit(1)

    # For commands where we're not just listing courses or components,
    # if the course is not "?" and not listing components (i.e. --component != "?"),
    # then validate that host, start-pod, and end-pod are provided.
    if args.command in ['setup', 'manage', 'teardown']:
        if args.course != "?" and not (args.command == 'setup' and args.component == "?"):
            missing = []
            if not args.host:
                missing.append("--host")
            if not args.start_pod:
                missing.append("--start-pod")
            if not args.end_pod:
                missing.append("--end-pod")
            if missing:
                print("Error: The following arguments are required for the '{}' command: {}".format(
                    args.command, ", ".join(missing)
                ))
                sys.exit(1)

            # Check that start-pod is not greater than end-pod.
            try:
                start_pod = int(args.start_pod)
                end_pod = int(args.end_pod)
                if start_pod > end_pod:
                    print("Error: --start-pod value must be less than or equal to --end-pod value.")
                    sys.exit(1)
            except ValueError:
                print("Error: Pod numbers must be integers.")
                sys.exit(1)

    # Set logging level.
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled.")
    else:
        logger.setLevel(logging.INFO)

    # Only log the initiating message when all required build arguments are provided.
    if args.command == 'setup' and args.course != "?" and args.component != "?":
        logger.info("Initiating setup for course '%s' on host '%s' (pods %s-%s); Rebuild=%s",
                    args.course, args.host, args.start_pod, args.end_pod, args.re_build)
        setup_environment(args)
        logger.info("Setup complete for course '%s'.", args.course)
    elif args.command == 'setup':
        # When listing courses/components, just call setup_environment() without the build logs.
        setup_environment(args)
    elif args.command == 'manage':
        logger.info("Initiating management operations for course '%s' on host '%s'.", args.course, args.host)
        manage_environment(args)
    elif args.command == 'teardown':
        logger.info("Initiating teardown for course '%s' on host '%s'.", args.course, args.host)
        teardown_environment(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    duration_minutes = (end_time - start_time) / 60
    logger.info("Program completed in %.2f minutes.", duration_minutes)
