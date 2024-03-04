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
                print(result)
            except Exception as e:
                # Handle cloning failure
                print(f"Cloning task failed: {e}")


def setup_environment(args):
    vc_host = args.vcenter
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection
    logger.info("Gathering user info.")
    # Add your setup environment logic here

    vc = VCenter(vc_host, vc_user, vc_password, vc_port)
    vc.connect()

    course_config = get_setup_config(args.course)

    # Capture the start time with higher precision
    start_time = time.perf_counter()

    if course_config:
        with ThreadPoolExecutor() as executor:
            futures = []
            for pod in range(int(args.start_pod), int(args.end_pod) + 1):
                pod_config = replace_placeholder(course_config, pod)
                deploy_futures = executor.submit(
                    deploy_lab,
                    vc,
                    args,
                    pod_config,
                    pod
                )
                futures.append(deploy_futures)
            wait_for_futures(futures)

    # Capture the end time with higher precision
    end_time = time.perf_counter()

    # Calculate the duration in seconds
    duration_seconds = end_time - start_time

    # Convert seconds to minutes
    duration_minutes = duration_seconds / 60

    logger.info(f"The program took {duration_minutes:.2f} minutes to run.")


def deploy_lab(vc, args, pod_config, pod):

    host = args.host + ".rededucation.com"

    # Create resource pool for the pod.
    logger.info(f"Creating resource pool for pod: {pod_config['group_name']}")
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
    logger.info(f"Resource pool created for {pod_config['group_name']}.")
    logger.info(f"Assigning user: {pod_config['user']} and role: {pod_config['role']} for resource pool {pod_config['group_name']}.")
    resource_pool_manager.assign_role_to_resource_pool(pod_config["group_name"], 
                                                       pod_config["domain"]+"\\"+pod_config["user"], 
                                                       pod_config["role"])
    
    logger.info(f"Creating folder {pod_config['folder_name']}")
    folder_manager = FolderManager(vc)
    folder_manager.create_folder(args.parent_folder, pod_config['folder_name'])
    logger.info(f"Created folder {pod_config['folder_name']}")
    logger.info(f"Assigning user: {pod_config['user']} and role: {pod_config['role']} for resource pool {pod_config['folder_name']}")
    folder_manager.assign_user_to_folder(pod_config["folder_name"],
                                         pod_config["domain"]+"\\"+pod_config["user"],
                                         pod_config["role"])
    
    logger.info(f"Creating vswitch {pod_config} on host {host}")
    network_manager = NetworkManager(vc)
    network_manager.create_vswitch(host, pod_config['network']['switch_name'])
    logger.info(f"Created vswitch {pod_config['network']['switch_name']} on host {host}")
    logger.info(f"Creating port groups {pod_config['network']['port_groups']} on host {host}")
    network_manager.create_vm_port_groups(host, pod_config["network"]["switch_name"],
                                          pod_config["network"]["port_groups"])
    logger.info(f"Created port groups {pod_config['network']['port_groups']} on host {pod_config}")
    logger.info(f"Assigning user: {pod_config['user']} and role: {pod_config['role']} for networks {pod_config['network']['port_groups']}")
    network_names = [pg["port_group_name"] for pg in pod_config["network"]["port_groups"]]
    network_manager.apply_user_role_to_networks(pod_config["domain"]+"\\"+pod_config["user"],
                                                pod_config["role"], network_names)
    logger.info(f"Assigned user: {pod_config['user']} and role: {pod_config['role']} for networks {pod_config['network']['port_groups']}")
    if pod_config['network']['promiscious_mode']:
        logger.info(f"Enable promiscuous mode for {pod_config['network']['promiscious_mode']}")
        network_manager.enable_promiscuous_mode(host, pod_config['network']['promiscious_mode'])
    
    logger.info(f"Cloning VMs in to {pod_config['folder_name']}")
    vm_manager = VmManager(vc)
    with ThreadPoolExecutor() as executor:
        futures = []
        for component in pod_config["components"]:
            # Schedule the VM cloning task
            logger.info(f"Cloning VM {component['clone_name']}")
            clone_future = executor.submit(
                vm_manager.clone_vm,
                component["base_vm"], 
                component["clone_name"], 
                pod_config["group_name"], 
                pod_config["folder_name"], 
                datastore_name="vms"  # Assuming "vms" is a fixed datastore name for all clones
            )
            futures.append(clone_future)
        wait_for_futures(futures)
        futures.clear()

        for component in pod_config["components"]:
            logger.info(f"Updating networks for VMs {component['clone_name']}")
            # Schedule the VM cloning task
            update_future = executor.submit(
                vm_manager.update_vm_networks,
                component["clone_name"],
                pod_config["folder_name"],
                pod
            )
            futures.append(update_future)
            if component["base_vm"] == "cp-R81.20-vr":
                vm_manager.update_mac_address(component["clone_name"], 
                                              "Network adapter 1", 
                                              "00:50:56:04:00:" + "{:02x}".format(pod))
        wait_for_futures(futures)
        futures.clear()

        for component in pod_config["components"]:
            # Schedule the VM cloning task
            poweron_future = executor.submit(
                vm_manager.poweron_vm,
                component["clone_name"]
            )
            futures.append(poweron_future)
        wait_for_futures(futures)


def teardown_lab():
    print("Tearing down lab...")
    # Add your teardown lab logic here

def main():
    parser = argparse.ArgumentParser(prog='labbuild', description="Lab Build Management Tool")
    subparsers = parser.add_subparsers(dest='command', title='commands', help='Available commands')

    # Subparser for the 'setup' command
    setup_parser = subparsers.add_parser('setup', help='Set-up the lab environment')
    setup_parser.add_argument('--vcenter', required=True, help='Specify the vcenter for conenction.')
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
        print(f"Setting up environment with config...")
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
