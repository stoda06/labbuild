from managers.resource_pool_manager import ResourcePoolManager
from managers.folder_manager import FolderManager
from managers.network_manager import NetworkManager
from managers.vm_manager import VmManager
from managers.host_manager import HostManager
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from logger.log_config import setup_logger
import logging
import argparse
import json
import time
import sys
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
                logger.error(f"Cloning task failed: {e}")


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

    host_manager = HostManager(vc)
    host = args.host + ".rededucation.com"
    try:
        host_manager.get_host(host)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

    # Create resource pool for the pod.
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
    try:
        resource_pool_manager.create_resource_pool(args.parent_resource_pool, 
                                                pod_config["group_name"], 
                                                    cpu_allocation, 
                                                    memory_allocation)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    # Assign user and role to the created resource pool.
    resource_pool_manager.assign_role_to_resource_pool(pod_config["group_name"], 
                                                       pod_config["domain"]+"\\"+pod_config["user"], 
                                                       pod_config["role"])
    
    # Create pod folder
    folder_manager = FolderManager(vc)
    folder_manager.create_folder(args.parent_folder, pod_config['folder_name'])
    # Assign user and role to the created folder.
    folder_manager.assign_user_to_folder(pod_config["folder_name"],
                                         pod_config["domain"]+"\\"+pod_config["user"],
                                         pod_config["role"])
    
    # Create vSwitches
    network_manager = NetworkManager(vc)
    for network in pod_config['network']:
        network_manager.create_vswitch(host, network['switch_name'])
        # Create necessary port groups/networks.
        network_manager.create_vm_port_groups(host, network["switch_name"], network["port_groups"])
        # Assign user and role to created port groups/networks.
        network_names = [pg["port_group_name"] for pg in network["port_groups"]]
        network_manager.apply_user_role_to_networks(pod_config["domain"]+"\\"+pod_config["user"],
                                                    pod_config["role"], network_names)
        # Check if any of the created networks need to be set to promisci
        if network['promiscuous_mode']:
            network_manager.enable_promiscuous_mode(host, network['promiscuous_mode'])
    
    # Start cloning the required VMs simultaneously.
    vm_manager = VmManager(vc)
    with ThreadPoolExecutor(max_workers=args.thread) as executor:
        futures = []
        for component in pod_config["components"]:
            clone_future = executor.submit(
                vm_manager.clone_vm,
                component["base_vm"], 
                component["clone_name"], 
                pod_config["group_name"], 
                pod_config["folder_name"], 
                datastore_name=args.datastore
            )
            futures.append(clone_future)
        wait_for_futures(futures)
        futures.clear()

        for component in pod_config["components"]:
            # Update cloned VMs with the created network(s).
            update_future = executor.submit(
                vm_manager.update_vm_networks,
                component["clone_name"],
                pod_config["folder_name"],
                pod
            )
            futures.append(update_future)
            # Update MAC address on the VR with the pod number with HEX base.
            if "cp-R81-vr" in component["clone_name"] or "cpvr" in component["clone_name"]:
                vm_manager.update_mac_address(component["clone_name"], 
                                              "Network adapter 1", 
                                              "00:50:56:04:00:" + "{:02x}".format(pod))
        wait_for_futures(futures)
        futures.clear()

        snapshot_name = "base"
        for component in pod_config["components"]:
            # Create a snapshot of all the cloned VMs to save base config.
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                snapshot_futures = executor.submit(
                    vm_manager.create_snapshot,
                    component["clone_name"],
                    snapshot_name,
                    description=f"Snapshot of {component['clone_name']}"
                )
                futures.append(snapshot_futures)
        wait_for_futures(futures)
        futures.clear()

        for component in pod_config["components"]:
            # Schedule the VM cloning task
            if "state" in component:
                if "poweroff" in component["state"]:
                    continue
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
    setup_parser.add_argument('-ds','--datastore', required=False, default="vms", help='Name of the folder in which the pod folder has to be created.')
    setup_parser.add_argument('-t','--thread', type=int, required=False, default=4, help='Number of worker for the cloning process.')
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
    elif args.command == 'manage':
        pass
    elif args.command == 'teardown':
        if args.force:
            print("Forcing teardown without confirmation.")
        else:
            print("Teardown initiated with confirmation.")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
