from managers.resource_pool_manager import ResourcePoolManager
from managers.folder_manager import FolderManager
from managers.network_manager import NetworkManager
from managers.vm_manager import VmManager
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from logger.log_config import setup_logger
import logging
import argparse
import json
import time
import os

load_dotenv()

logger = setup_logger()
vc_host = "vcenter-appliance-2.rededucation.com"
vc_user = os.getenv("VC_USER")
vc_password = os.getenv("VC_PASS")
vc_port = 443  # Default port for vCenter connection

def load_setup_template(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def get_setup_config(setup_name):
    setup_config = load_setup_template(setup_name)
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
                print(result)
            except Exception as e:
                # Handle cloning failure
                print(f"Cloning task failed: {e}")


def setup_environment(args):
    logger.info("Gathering user info.")
    # Add your setup environment logic here

    vc = VCenter(vc_host, vc_user, vc_password, vc_port)
    vc.connect()

    course_config = get_setup_config("courses/" + args.course + ".json")

    # Capture the start time with higher precision
    start_time = time.perf_counter()

    if course_config:
        with ThreadPoolExecutor() as executor:
            futures = []
            for pod in range(args.start_pod, args.end_pod+1):
                pod_config = replace_placeholder(course_config, pod)

                deploy_futures = executor.submit(
                    deploy_lab,
                    vc,
                    args,
                    pod_config
                )
                futures.append(deploy_futures)
            wait_for_futures(futures)


def deploy_lab(vc, args, pod_config):

    # Create resource pool for the pod.
    logger.info(f"Creating resource pool for pod: {pod_config["group_name"]}")
    resource_pool_manager = ResourcePoolManager(vc)
    cpu_allocation = {
        'limit': -1,
        'reservation': 0,
        'expandable_reservation': True,
        'shares': 4000
    }
    memory_allocation = {
        'limit': -1,
        'reservation': 0,
        'expandable_reservation': True,
        'shares': 163840
    }
    resource_pool_manager.create_resource_pool(args.parent_resource_pool, 
                                               pod_config["group_name"], 
                                                cpu_allocation, 
                                                memory_allocation)
    logger.info(f"Resource pool created for {pod_config["group_name"]}.")
    logger.info(f"Assigning user and role for resource pool {pod_config["group_name"]}.")
    resource_pool_manager.assign_role_to_resource_pool(pod_config["group_name"], 
                                                       pod_config["domain"]+"\\"+pod_config["user"], 
                                                       pod_config["role"])
    logger.info(f"Creating folder {pod_config["folder_name"]}")

def teardown_lab():
    print("Tearing down lab...")
    # Add your teardown lab logic here

def main():
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    subparsers = parser.add_subparsers(dest='command', title='commands', help='Available commands')

    # Subparser for the 'setup' command
    setup_parser = subparsers.add_parser('setup', help='Set-up the lab environment')
    setup_parser.add_argument('--course', required=True, help='Path to the configuration file.')
    setup_parser.add_argument('-s', '--start-pod', required=True, help='Starting value for the range of the pods.')
    setup_parser.add_argument('-e', '--end-pod', required=True, help='Ending value for the range of the pods.')
    setup_parser.add_argument('--host', required=True, help='Name of the host to create the pods.')
    setup_parser.add_argument('-rp','--parent-resource-pool', required=True, help='Name of the resource pool in which the pods has to be created.')
    setup_parser.add_argument('-fd','--parent-folder', required=True, help='Name of the folder in which the pod folder has to be created.')
    setup_parser.add_argument('-v','--verbose', action='store_true', help='Enable verbose output')

    setup_parser = subparsers.add_parser('manage', help='Manage the lab environment.')
    setup_parser.add_argument('-cs','--create-snapshot', required=True, help='Name of the snapshot.')
    setup_parser.add_argument('-mem','--memory', required=True, choices=['True','False'], help='''Include the virtual machine's memory in the snapshot.''')
    setup_parser.add_argument('-qs','--quiesce', required=True, choices=['True','False'], default='False', help='Quiesce the file system in the virtual machine.')

    # Subparser for the 'teardown' command
    teardown_parser = subparsers.add_parser('teardown', help='Teardown the lab environment')
    teardown_parser.add_argument('-rp', '--resource-pool',required=True, help='Name of the resource pool to be deleted.')
    teardown_parser.add_argument('-fd', '--resource-folder',required=True, help='Name of the folder to be deleted.')
    teardown_parser.add_argument('-nt', '--network',required=True, help='Name of the network to be delete.')
    teardown_parser.add_argument('--force', action='store_true', help='Force teardown without confirmation')


    # Parse arguments
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)  # Set to DEBUG to enable verbose output
    else:
        logger.setLevel(logging.INFO)  # Set to INFO or higher to reduce output

    # Execute based on switches
    if args.command == 'setup':
        print(f"Setting up environment with config: {args.verbose}")
        setup_environment(args)
        if args.verbose:
            print("Verbose mode enabled.")
    elif args.command == 'teardown':
        if args.force:
            print("Forcing teardown without confirmation.")
        else:
            print("Teardown initiated with confirmation.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
