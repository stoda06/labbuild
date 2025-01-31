#!/usr/bin/env python3

import argparse
import argcomplete
import json
import logging
import os
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote_plus

import pymongo

import labs.manage.vm_operations as manage
import labs.setup.avaya as avaya
import labs.setup.checkpoint as checkpoint
import labs.setup.f5 as f5
import labs.setup.palo as palo
import labs.setup.pr as pr
import labs.setup.nu as nu
from logger.log_config import setup_logger
from managers.vcenter import VCenter

load_dotenv()

logger = setup_logger()

MONGO_USER = quote_plus("MONGO_USER")
MONGO_PASSWORD = quote_plus("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONG_HOST")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/{MONGO_DATABASE}"

def fetch_and_prepare_course_config(setup_name, pod=None, f5_class=None):
    """
    Fetch the setup configuration from MongoDB and prepare it for a specific pod.

    This function connects to the MongoDB instance using a provided URI, retrieves
    the configuration for the specified setup name, and optionally replaces
    placeholders (e.g., {X}) with a specific pod number.

    :param setup_name: Name of the setup to fetch.
    :param pod: Optional pod number to replace placeholders in the configuration.
    :return: Prepared setup configuration or None if not found.
    """
    client = None
    try:
        # Establish connection to the MongoDB instance
        client = pymongo.MongoClient(MONGO_URI)
        db = client["labbuild_db"]
        collection = db["courseconfig"]

        # Retrieve the configuration for the given setup name
        setup_config = collection.find_one({"course_name": setup_name})
        if setup_config is None:
            logger.error(f"Setup {setup_name} not found in the database.")
            return None

        # Remove MongoDB's default _id field from the result to avoid serialization issues
        setup_config.pop("_id", None)

        # Replace placeholders (e.g., {X}) with the provided pod number
        setup_config_str = json.dumps(setup_config)
        if pod is not None:
            setup_config_str = setup_config_str.replace("{X}", str(pod))
        if f5_class is not None:
            setup_config_str = setup_config_str.replace("{Y}", str(f5_class))
        setup_config = json.loads(setup_config_str)

        return setup_config

    except pymongo.errors.PyMongoError as e:
        # Log an error if there is an issue connecting to MongoDB
        logger.error(f"Error connecting to MongoDB: {e}")
        return None

    finally:
        # Ensure the MongoDB connection is closed
        if client:
            client.close()


def extract_components(course_config):
    """
    Extract all unique component names from a course configuration.

    This function parses the "components" and "group" keys in the configuration
    to extract and deduplicate component names.

    :param course_config: Dictionary containing course configuration.
    :return: List of unique component names.
    """
    components = []

    # Extract components from the top-level "components" key
    if "components" in course_config:
        components.extend([comp["component_name"] for comp in course_config["components"]])

    # Extract components from nested "group" structures
    if "groups" in course_config:
        for group in course_config["groups"]:
            if "component" in group:
                components.extend([comp["component_name"] for comp in group["component"]])

    # Return unique components
    return list(set(components))


def wait_for_futures(futures):
    """
    Wait for all futures in the provided list to complete.

    This function iterates over a list of concurrent futures, logs the result of
    each future if successful, and logs any errors encountered during execution.

    :param futures: List of futures to wait for.
    """
    for future in futures:
        try:
            result = future.result()  # This will re-raise any exceptions from the task
            logger.debug(result)
        except Exception as e:
            logger.error(f"Task failed: {e}")


def get_host_by_name(hostname):
    client = None
    try:
        # Establish connection to the MongoDB instance
        client = pymongo.MongoClient(MONGO_URI)
        db = client["labbuild_db"]
        collection = db["host"]

        host_details = collection.find_one({"host_name": hostname})
        if host_details is None:
            logger.error(f"Host {hostname} not found in the database.")
            return None
        
        host_details.pop("_id", None)
        return host_details
    
    except pymongo.errors.PyMongoError as e:
        # Log an error if there is an issue connecting to MongoDB
        logger.error(f"Error connecting to MongoDB: {e}")
        return None

    finally:
        # Ensure the MongoDB connection is closed
        if client:
            client.close()


def update_database(data):
    """
    Update or insert data into the 'currentallocation' collection.
    
    :param data: Dictionary containing the tag, course, and pod details.
    :param mongo_uri: MongoDB connection URI.
    :param db_name: Name of the MongoDB database.
    """
    client = pymongo.MongoClient(MONGO_URI)
    db = client["labbuild_db"]
    collection = db["currentallocation"]

    tag = data["tag"]
    course_name = data["course_name"]
    pod_details = data["pod_details"]

    # Check if the tag exists
    tag_entry = collection.find_one({"tag": tag})
    
    if tag_entry:
        # Tag exists, check if the course exists under this tag
        existing_course = next(
            (course for course in tag_entry.get("courses", []) if course["course_name"] == course_name),
            None
        )
        
        if existing_course:
            # Course exists, update or insert pod details
            for new_pod in pod_details:
                existing_pod = next(
                    (pod for pod in existing_course["pod_details"] if pod["pod_number"] == new_pod["pod_number"]),
                    None
                )
                if existing_pod:
                    # Pod exists, update its details
                    existing_pod.update(new_pod)
                else:
                    # Pod does not exist, add it
                    existing_course["pod_details"].append(new_pod)
        else:
            # Course does not exist, add a new course
            new_course = {
                "course_name": course_name,
                "pod_details": pod_details
            }
            tag_entry.setdefault("courses", []).append(new_course)
        
        # Update the document in MongoDB
        collection.update_one({"tag": tag}, {"$set": {"courses": tag_entry["courses"]}})
    else:
        # Tag does not exist, create a new entry
        new_entry = {
            "tag": tag,
            "courses": [
                {
                    "course_name": course_name,
                    "pod_details": pod_details
                }
            ]
        }
        collection.insert_one(new_entry)
    
    client.close()
    logger.info("Data successfully updated or inserted.")


def delete_from_database(tag, course_name=None, pod_number=None, class_number=None):
    """
    Delete a tag, course, or pod from the 'currentallocation' collection.

    :param tag: The tag to delete or to search under.
    :param course_name: (Optional) The course name to delete under the tag.
    :param pod_number: (Optional) The pod number to delete under the course.
    """
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client["labbuild_db"]
        collection = db["currentallocation"]

        # Case 1: Delete the entire tag
        if not course_name and not pod_number:
            collection.delete_one({"tag": tag})
            logger.info(f"Deleted tag: {tag}")
            return
        
        # Find the tag entry
        tag_entry = collection.find_one({"tag": tag})
        if not tag_entry:
            logger.warning(f"Tag {tag} not found.")
            return
        
        # Case 2: Delete a course under the tag
        if course_name and not pod_number:
            updated_courses = [
                course for course in tag_entry.get("courses", [])
                if course["course_name"] != course_name
            ]
            # Update or delete the tag if no courses remain
            if updated_courses:
                collection.update_one({"tag": tag}, {"$set": {"courses": updated_courses}})
            else:
                collection.delete_one({"tag": tag})
            logger.info(f"Deleted course '{course_name}' under tag '{tag}'.")
            return
        
        # Case 3: Delete a pod under the course
        if course_name and pod_number:
            for course in tag_entry.get("courses", []):
                if course["course_name"] == course_name:
                    updated_pods = [
                        pod for pod in course.get("pod_details", [])
                        if pod["pod_number"] != pod_number or ("class_number" in pod and pod["class_number"] != class_number)
                    ]
                    course["pod_details"] = updated_pods
                    break
            
            # Remove the course if no pods remain
            tag_entry["courses"] = [
                course for course in tag_entry.get("courses", [])
                if course["pod_details"]
            ]
            
            # Update or delete the tag if no courses remain
            if tag_entry["courses"]:
                collection.update_one({"tag": tag}, {"$set": {"courses": tag_entry["courses"]}})
            else:
                collection.delete_one({"tag": tag})
            logger.info(f"Deleted pod '{pod_number}' from course '{course_name}' under tag '{tag}'.")
            return

    except Exception as e:
        logger.error(f"Error occurred during deletion: {str(e)}")
    finally:
        client.close()


def setup_environment(args):
    """
    Set up the lab environment based on the provided arguments.

    This function retrieves the course configuration, validates the components,
    and initiates the appropriate build process for the specified vendor.

    :param args: Parsed command-line arguments containing setup details.
    """
    course_config = fetch_and_prepare_course_config(args.course)

    # If the user requested available components, display them and exit
    if args.component == "?":
        if course_config:
            available_components = extract_components(course_config)
            print(f"Available components for course '{args.course}':")
            for component in available_components:
                print(f"  - {component}")
        else:
            print(f"No components found for course '{args.course}'.")
        sys.exit(0)

    if course_config:
        # Retrieve host details and initialize the vCenter connection
        host_details = get_host_by_name(args.host)
        vc_host = host_details["vcenter"]
        vc_user = os.getenv("VC_USER")
        vc_password = os.getenv("VC_PASS")
        vc_port = 443

        service_instance = VCenter(vc_host, vc_user, vc_password, vc_port)
        service_instance.connect()
        futures = []

        data = {
            "tag": args.tag,
            "course_name": args.course,
            "pod_details": []
        }

        # Parse and validate selected components
        selected_components = None
        if args.component:
            selected_components = [comp.strip() for comp in args.component.split(",")]
            available_components = extract_components(course_config)
            invalid_components = [comp for comp in selected_components if comp not in available_components]
            if invalid_components:
                logger.error(f"Invalid components specified: {', '.join(invalid_components)}")
                sys.exit(1)

        # Execute build processes based on vendor
        if course_config["vendor_shortcode"] == "cp":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["pod_number"] = pod
                    pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "True"}
                    data["pod_details"].append(pod_details)
                    update_database(data)
                    deploy_futures = executor.submit(
                        checkpoint.build_cp_pod,
                        service_instance,
                        pod_config,
                        rebuild=args.re_build,
                        thread=args.thread,
                        full=args.full,
                        selected_components=selected_components
                    )
                    futures.append(deploy_futures)
                wait_for_futures(futures)

        if course_config["vendor_shortcode"] == "pa":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["pod_number"] = pod
                    if  "cortex" in course_config["course_name"]:
                        build_future = executor.submit(palo.build_cortex_pod,
                                                        service_instance, pod_config, 
                                                        rebuild=args.re_build,
                                                        full=args.full,
                                                        selected_components=selected_components)
                    elif "1100-210" in course_config["course_name"]:
                        build_future = executor.submit(palo.build_1100_210_pod,
                                                        service_instance, pod_config, 
                                                        rebuild=args.re_build,
                                                        full=args.full,
                                                        selected_components=selected_components)
                    elif "1110" in course_config["course_name"]:
                        build_future = executor.submit(palo.build_1110_pod,
                                                        service_instance, pod_config, 
                                                        rebuild=args.re_build,
                                                        full=args.full,
                                                        selected_components=selected_components)
                    elif "1100-220" in course_config["course_name"]:
                        build_future = executor.submit(palo.build_1100_220_pod,
                                                        service_instance, host_details, 
                                                        pod_config, 
                                                        rebuild=args.re_build,
                                                        full=args.full,
                                                        selected_components=selected_components)
                    futures.append(build_future)
                    pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "True"}
                    data["pod_details"].append(pod_details)
                    update_database(data)
                wait_for_futures(futures)

        if course_config["vendor_shortcode"] == "av":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["pod_number"] = pod
                    if  "aura" in pod_config["course_name"]:
                        build_future = executor.submit(avaya.build_aura_pod, service_instance, pod_config)
                    if "ipo" in pod_config["course_name"]:
                        build_future = executor.submit(avaya.build_ipo_pod, service_instance, 
                                                       pod_config, rebuild=args.re_build, 
                                                       selected_components=selected_components)
                    futures.append(build_future)
                    pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "True"}
                    data["pod_details"].append(pod_details)
                    update_database(data)
                wait_for_futures(futures)

        if course_config["vendor_shortcode"] == "f5":
            class_config = fetch_and_prepare_course_config(args.course, f5_class=args.class_number)
            class_config["host_fqdn"] = host_details["fqdn"]
            class_config["class_number"] = args.class_number
            class_config["class_name"] = f'f5-class{args.class_number}'
            f5.build_new_class(service_instance, class_config, rebuild=args.re_build, 
                               full=args.full, selected_components=selected_components)
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod, f5_class=args.class_number)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["class_number"] = args.class_number
                pod_config["pod_number"] = str(pod)
                f5.build_new_pod(service_instance, pod_config, mem=args.memory, rebuild=args.re_build, 
                                 full=args.full, selected_components=selected_components)
            pod_details = {"pod_number": pod, "pod_host": args.host, "class_number":args.class_number, "poweron": "True"}
            data["pod_details"].append(pod_details)
            update_database(data)
            data["pod_details"].append(pod_details)

        if course_config["vendor_shortcode"] == "prtg":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["class_number"] = args.class_number
                    build_future = executor.submit(pr.build_pr_pod, service_instance, pod_config, 
                                                   rebuild=args.re_build,
                                                   full=args.full, 
                                                   selected_components=selected_components)
                    futures.append(build_future)
                    pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "True"}
                    data["pod_details"].append(pod_details)
                    update_database(data)
                wait_for_futures(futures)

        if course_config["vendor_shortcode"] == "nu":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                    pod_config["host_fqdn"] = host_details["fqdn"]
                    pod_config["class_number"] = args.class_number
                    build_future = executor.submit(nu.build_nu_pod, service_instance, pod_config, 
                                                   rebuild=args.re_build,
                                                   full=args.full, 
                                                   selected_components=selected_components)
                    futures.append(build_future)
                    pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "True"}
                    data["pod_details"].append(pod_details)
                    update_database(data)
                wait_for_futures(futures)

def teardown_environment(args):
    host_details = get_host_by_name(args.host)
    vc_host = host_details["vcenter"]
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443

    service_instance = VCenter(vc_host, vc_user, vc_password, vc_port)
    service_instance.connect()

    course_config = fetch_and_prepare_course_config(args.course)

    if course_config["vendor_shortcode"] == "cp":
        with ThreadPoolExecutor() as executor:
            futures = []
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                group_name = f'cp-pod{pod}'
                logger.info(f"Deleting {group_name}")
                teardown_future = executor.submit(
                    checkpoint.teardown_pod,
                    service_instance,
                    pod_config
                )
                futures.append(teardown_future)
                delete_from_database(args.tag, course_name=pod_config["course_name"], pod_number=pod)
            wait_for_futures(futures)

    if course_config["vendor_shortcode"] == "f5":
        class_number = args.class_number
        class_name = f"f5-class{class_number}"
        class_config = fetch_and_prepare_course_config(args.course, f5_class=class_number)
        class_config["class_name"] = class_name
        class_config["host_fqdn"] = host_details["fqdn"]
        f5.teardown_class(service_instance, class_config)
        delete_from_database(args.tag, course_name=class_config["course_name"], class_number=class_number)

    if course_config["vendor_shortcode"] == "pa":
        with ThreadPoolExecutor() as executor:
            futures = []
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                if "1110" in course_config["course_name"]:
                    teardown_future = executor.submit(
                        palo.teardown_1110, service_instance, pod_config
                    )
                elif "1100" in course_config["course_name"]:
                    teardown_future = executor.submit(
                        palo.teardown_1100, service_instance, pod_config
                    )
                elif course_config["course_name"] == "cortex":
                    teardown_future = executor.submit(
                        palo.teardown_cortex, service_instance, pod_config
                    )
                futures.append(teardown_future)
                delete_from_database(args.tag, course_name=pod_config["course_name"], pod_number=pod)
            wait_for_futures(futures)

    if course_config["vendor_shortcode"] == "av":
        with ThreadPoolExecutor() as executor:
            futures = []
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                if "ipo" in course_config["course_name"]:
                    teardown_future = executor.submit(avaya.teardown_ipo, service_instance, pod_config)
                elif "aura" in course_config["course_name"]:
                    teardown_future = executor.submit(avaya.teardown_aura, service_instance, pod_config)
                futures.append(teardown_future)
                delete_from_database(args.tag, course_name=pod_config["course_name"], pod_number=pod)
            wait_for_futures(futures)

    if course_config["vendor_shortcode"] == "prtg":
        with ThreadPoolExecutor() as executor:
            futures = []
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
                pod_config["host_fqdn"] = host_details["fqdn"]
                pod_config["pod_number"] = pod
                teardown_future = executor.submit(pr.teardown_pr_pod, service_instance, pod_config)
                futures.append(teardown_future)
                delete_from_database(args.tag, course_name=pod_config["course_name"], pod_number=pod)
            wait_for_futures(futures)

    logger.info(f"Teardown process complete.")

def manage_environment(args):
    host_details = get_host_by_name(args.host)
    vc_host = host_details["vcenter"]
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443

    service_instance = VCenter(vc_host, vc_user, vc_password, vc_port)
    service_instance.connect()
    course_config = fetch_and_prepare_course_config(args.course)

    data = {
            "tag": args.tag,
            "course_name": args.course,
            "pod_details": []
        }

    selected_components = None
    if args.component:
        if args.component == "?":
            available_components = extract_components(course_config)
            print(f"Available components for course '{args.course}':")
            for component in available_components:
                print(f"  - {component}")
            sys.exit(0)
        else:
            selected_components = [comp.strip() for comp in args.component.split(",")]
            available_components = extract_components(course_config)
            invalid_components = [comp for comp in selected_components if comp not in available_components]
            if invalid_components:
                print(f"Invalid components specified: {', '.join(invalid_components)}")
                sys.exit(1)

    futures = []
    with ThreadPoolExecutor() as executor:
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            pod_config = fetch_and_prepare_course_config(args.course, pod=pod)
            operation_future = executor.submit(
                manage.perform_vm_operations,
                service_instance,
                pod_config,
                args.operation,
                selected_components
            )
            futures.append(operation_future)
            if args.operation == 'start':
                pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "True"}
                data["pod_details"].append(pod_details)
                update_database(data)
            if args.operation == 'stop':
                pod_details = {"pod_number": pod, "pod_host": args.host, "poweron": "False"}
                data["pod_details"].append(pod_details)
                update_database(data)
        wait_for_futures(futures)

def main():
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    argcomplete.autocomplete(parser)
    subparsers = parser.add_subparsers(dest='command', title='commands', help='Available commands')

    # Subparser for the 'setup' command
    setup_parser = subparsers.add_parser('setup', help='Set-up the lab environment')
    setup_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    setup_parser.add_argument('-cn', '--class_number', help='Class argument only valid if --course contains a f5')
    setup_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    setup_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    setup_parser.add_argument(
        '-c','--component', 
        help="Comma-separated list of components to build (e.g., R81.20-vr,A-GUI-CCAS-R81.20)"
    )
    setup_parser.add_argument('--host', required=True, help='Name of the host to create the pods.')
    setup_parser.add_argument('-ds','--datastore', required=False, default="vms", help='Name of the folder in which the pod folder has to be created.')
    setup_parser.add_argument('-th','--thread', type=int, required=False, default=4, help='Number of worker for the cloning process.')
    setup_parser.add_argument('-r','--re-build', action='store_true', help='Enable re-build to clear any existing resources for the given build and proceed with the build')
    setup_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')
    setup_parser.add_argument('-q','--quiet', action='store_true', help='Suppress output to display only warnings or errors')
    setup_parser.add_argument('-mem','--memory', type=int, default=None, required=False, help='Specify memory for f5 bigip component.')
    setup_parser.add_argument('--full', action='store_true', help='Create full clones to conserve storage space')
    setup_parser.add_argument('--clonefrom', action='store_true', help='Create clones from an existing pod.')
    setup_parser.add_argument('-t', '--tag', required=False, default="untagged", help='Used to tag a pod range with SF Job code.')

    manage_parser = subparsers.add_parser('manage', help='Manage the lab environment.')
    manage_parser.add_argument('-cn', '--class_number', help='Class argument only valid if --course contains a f5')
    # manage_parser.add_argument('-cs','--create-snapshot', required=True, help='Name of the snapshot.')
    # manage_parser.add_argument('-mem','--memory', required=True, choices=['True','False'], help='''Include the virtual machine's memory in the snapshot.''')
    # manage_parser.add_argument('-qs','--quiesce', required=True, choices=['True','False'], default='False', help='Quiesce the file system in the virtual machine.')
    manage_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    manage_parser.add_argument(
        '-c','--component', 
        help="Comma-separated list of components to build (e.g., R81.20-vr,A-GUI-CCAS-R81.20)"
    )
    manage_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    manage_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    manage_parser.add_argument('--host', required=True, help='Name of the host where the pods reside.')
    manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True, help='Perform VM operations.')
    manage_parser.add_argument('-t', '--tag', required=False, default="untagged", help='Used to tag a pod range with SF Job code.')
    manage_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')

    # Subparser for the 'teardown' command
    teardown_parser = subparsers.add_parser('teardown', help='Teardown the lab environment')
    teardown_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    teardown_parser.add_argument('-cn', '--class_number', help='Class argument only valid if --course contains a f5')
    teardown_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    teardown_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    teardown_parser.add_argument('--host', required=True, help='Name of the host where the pod can be found.')
    teardown_parser.add_argument('-t', '--tag', required=False, default="untagged", help='Used to tag a pod range with SF Job code.')
    teardown_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')

    # Parse arguments
    args = parser.parse_args()
    # Check the course condition after parsing
    if 'f5' in args.course:
        if not args.class_number:
            print("Error: --class_number is required when --course contains 'f5'.")
            sys.exit(1)

    if args.verbose:
        logger.setLevel(logging.DEBUG)  # Set to DEBUG to enable verbose output
    else:
        logger.setLevel(logging.INFO)  # Set to INFO or higher to reduce output

    # Execute based on switches
    if args.command == 'setup':
        logger.info(f"Start of building {args.course} pod range {args.start_pod}->{args.end_pod} on {args.host}. Rebuild: {args.re_build}")
        setup_environment(args)
        logger.info(f"End of building {args.course} pod range {args.start_pod}->{args.end_pod} on {args.host}")
    elif args.command == 'manage':
        if args.operation:
            manage_environment(args)
    elif args.command == 'teardown':
        logger.info("teardown environment.")
        teardown_environment(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    # Capture the start time with higher precision
    start_time = time.perf_counter()
    main()
    # Capture the end time with higher precision
    end_time = time.perf_counter()

    # Calculate the duration in seconds
    duration_seconds = end_time - start_time

    # Convert seconds to minutes
    duration_minutes = duration_seconds / 60
    logger.name = 'labbuild'
    logger.info(f"The program took {duration_minutes:.2f} minutes to run.")
