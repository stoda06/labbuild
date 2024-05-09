#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from logger.log_config import setup_logger
from hosts.host import get_host_by_name
from managers.vcenter import VCenter
from dotenv import load_dotenv
import labs.manage.vm_operations as manage
import labs.setup.checkpoint as checkpoint
import labs.setup.avaya as avaya
import labs.setup.palo as palo
import argparse
import logging
import json
import time
import os

load_dotenv()

logger = setup_logger()

def load_setup_template(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def get_setup_config(setup_name):
    setup_template = load_setup_template('courses/'+ setup_name+'.json')
    setup_config = setup_template.get(setup_name)
    if setup_config is None:
        logger.error(f"Setup {setup_name} not found.")
        return None
    return setup_config

def replace_placeholder(setup_config, value):
    # Convert the network map dictionary to a JSON string
    setup_config_str = json.dumps(setup_config)

    # Replace the placeholder in the string
    updated_setup_config_str = setup_config_str.replace("{X}", str(value))

    # Convert the updated string back to a dictionary
    updated_setup_config = json.loads(updated_setup_config_str)

    return updated_setup_config

def wait_for_futures(futures):
        
        # Optionally, wait for all cloning tasks to complete and handle their results
        for future in futures:
            try:
                result = future.result()  # This will re-raise any exceptions caught in the task
                # Handle successful cloning result
                logger.debug(result)
            except Exception as e:
                # Handle cloning failure
                logger.error(f"Task failed: {e}")


def setup_environment(args):

    course_config = get_setup_config(args.course)

    if course_config:
        host_details = get_host_by_name(args.host)

        # Step-1: Connect to vcenter
        vc_host = host_details.vcenter
        vc_user = os.getenv("VC_USER")
        vc_password = os.getenv("VC_PASS")
        vc_port = 443  # Default port for vCenter connection

        service_instance = VCenter(vc_host, vc_user, vc_password, vc_port)
        service_instance.connect()
        futures = []

        if course_config["vendor"] == "cp":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = replace_placeholder(course_config, pod)
                    deploy_futures = executor.submit(
                        checkpoint.build_cp_pod,
                        service_instance,
                        pod_config,
                        args.host,
                        pod,
                        rebuild=args.re_build,
                        thread=args.thread,
                        datastore=args.datastore
                    )
                    futures.append(deploy_futures)
                wait_for_futures(futures)
        if course_config["vendor"] == "pa":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = replace_placeholder(course_config, pod)
                    if course_config["version"] == "cortex":
                        build_future = executor.submit(palo.build_cortex_pod,
                                                        service_instance, host_details, 
                                                        pod_config, 
                                                        rebuild=args.re_build,
                                                        linked=args.link)
                    elif course_config["version"] == "1100-210":
                        build_future = executor.submit(palo.build_1100_210_pod,
                                                        service_instance, host_details, 
                                                        pod_config, 
                                                        rebuild=args.re_build,
                                                        linked=args.link)
                    futures.append(build_future)
                wait_for_futures(futures)
        
        if course_config["vendor"] == "av":
            with ThreadPoolExecutor() as executor:
                for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                    pod_config = replace_placeholder(course_config, pod)
                    if pod_config["version"] == "aura":
                        build_futures = executor.submit(avaya.build_aura_pod,
                                            service_instance, pod_config)
                    if pod_config["version"] == "ipo":
                        build_futures = executor.submit(avaya.build_ipo_pod,
                                            service_instance, pod_config, pod, rebuild=args.re_build)
                    futures.append(build_futures)
                wait_for_futures(futures)

def teardown_environment(args):

    host_details = get_host_by_name(args.host)

    # Step-1: Connect to vcenter
    vc_host = host_details.vcenter
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    service_instance = VCenter(vc_host, vc_user, vc_password, vc_port)
    service_instance.connect()

    course_config = get_setup_config(args.course)
    with ThreadPoolExecutor() as executor:
        futures = []
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            pod_config = replace_placeholder(course_config, pod)
            logger.info(f"Deleting {pod_config['group_name']}")
            if course_config["vendor"] == "cp":
                teardown_future = executor.submit(
                    checkpoint.teardown_pod,
                    service_instance,
                    pod_config,
                    args.host
                )
            futures.append(teardown_future)
        wait_for_futures(futures)
    
    logger.info(f"Teardown process complete.")

def manage_environment(args):
    host_details = get_host_by_name(args.host)

    # Step-1: Connect to vcenter
    vc_host = host_details.vcenter
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    service_instance = VCenter(vc_host, vc_user, vc_password, vc_port)
    service_instance.connect()
    course_config = get_setup_config(args.course)
    futures = []
    
    with ThreadPoolExecutor() as executor:
        for pod in range(int(args.start_pod), int(args.end_pod) + 1):
            pod_config = replace_placeholder(course_config, pod)
            operation_future = executor.submit(manage.vm_operations.perform_vm_operations,
                                service_instance, pod_config, args.operation)
            futures.append(operation_future)
        wait_for_futures(futures)

def main():
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    subparsers = parser.add_subparsers(dest='command', title='commands', help='Available commands')

    # Subparser for the 'setup' command
    setup_parser = subparsers.add_parser('setup', help='Set-up the lab environment')
    setup_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    setup_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    setup_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    setup_parser.add_argument('--host', required=True, help='Name of the host to create the pods.')
    setup_parser.add_argument('-ds','--datastore', required=False, default="vms", help='Name of the folder in which the pod folder has to be created.')
    setup_parser.add_argument('-t','--thread', type=int, required=False, default=4, help='Number of worker for the cloning process.')
    setup_parser.add_argument('-r','--re-build', action='store_true', help='Enable re-build to clear any existing resources for the given build and proceed with the build')
    setup_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')
    setup_parser.add_argument('-q','--quiet', action='store_true', help='Suppress output to display only warnings or errors')
    setup_parser.add_argument('-c','--component', help='Build a specific component for a course.')
    setup_parser.add_argument('--link', action='store_true', help='Create linked clones to conserve storage space')

    manage_parser = subparsers.add_parser('manage', help='Manage the lab environment.')
    # manage_parser.add_argument('-cs','--create-snapshot', required=True, help='Name of the snapshot.')
    # manage_parser.add_argument('-mem','--memory', required=True, choices=['True','False'], help='''Include the virtual machine's memory in the snapshot.''')
    # manage_parser.add_argument('-qs','--quiesce', required=True, choices=['True','False'], default='False', help='Quiesce the file system in the virtual machine.')
    manage_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    manage_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    manage_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    manage_parser.add_argument('--host', required=True, help='Name of the host where the pods reside.')
    manage_parser.add_argument('-o', '--operation', choices=['start', 'stop', 'reboot'], required=True, help='Perform VM operations.')
    manage_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')

    # Subparser for the 'teardown' command
    teardown_parser = subparsers.add_parser('teardown', help='Teardown the lab environment')
    teardown_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    teardown_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    teardown_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    teardown_parser.add_argument('--host', required=True, help='Name of the host where the pod can be found.')
    teardown_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')

    # Parse arguments
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)  # Set to DEBUG to enable verbose output
    else:
        logger.setLevel(logging.INFO)  # Set to INFO or higher to reduce output

    # Execute based on switches
    if args.command == 'setup':
        logger.info(f"Building setup for {args.course} on {args.host}")
        setup_environment(args)
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

    logger.info(f"The program took {duration_minutes:.2f} minutes to run.")
