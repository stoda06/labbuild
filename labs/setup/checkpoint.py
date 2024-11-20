from hosts.host import get_host_by_name
from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
from monitor.prtg import PRTGManager
from monitor.prtg import checkpoint_server_info
from logger.log_config import setup_logger
import sys
import re


def wait_for_futures(futures):
    # Optionally, wait for all cloning tasks to complete and handle their results
    for future in futures:
        try:
            result = future.result()  # This will re-raise any exceptions caught in the task
            # Handle successful cloning result
        except Exception as e:
            # Handle cloning failure
            print(f"Task failed: {e}")


def update_network_dict(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to hex format

    def update_mac_address(mac_address):
        # Split the MAC address into octets
        mac_octets = mac_address.split(':')
        # Update the last octet with the hex value of the pod number
        mac_octets[-1] = pod_hex
        # Join the octets back into a MAC address
        return ':'.join(mac_octets)

    def update_network_name(network_name, pod_number):
        # Use regex to find and replace any "vs" followed by a number in the network name
        return re.sub(r'vs\d+', f'vs{pod_number}', network_name)

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']

        # Update the network name if it contains "vsX" (where X is any number)
        network_name = update_network_name(network_name, pod_number)

        # Update the MAC address if the network name contains "rdp"
        if 'rdp' in network_name:
            mac_address = update_mac_address(mac_address)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address
        }

    return updated_network_dict


def build_cp_pod(service_instance, pod_config, hostname, pod, rebuild=False, thread=4, full=False, selected_components=None):
    host = get_host_by_name(hostname)
    vm_manager = VmManager(service_instance)
    folder_manager = FolderManager(service_instance)
    network_manager = NetworkManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)
    permission_manager = PermissionManager(service_instance)

    if rebuild:
        handle_rebuild(vm_manager, network_manager, resource_pool_manager, pod, pod_config, host)

    create_resource_pool(resource_pool_manager, host, pod, pod_config)
    assign_role_to_resource_pool(resource_pool_manager, pod_config)

    create_folder(folder_manager, host, pod, pod_config)
    assign_role_to_folder(folder_manager, pod_config)
    add_permissions_to_datastore(permission_manager, pod_config)

    create_networks(network_manager, host, pod, pod_config)
    clone_and_configure_vms(vm_manager, network_manager, pod, pod_config, full, selected_components)

    power_on_components(vm_manager, pod_config, thread, selected_components)

    # add_to_prtg(pod_config, pod)


def handle_rebuild(vm_manager, network_manager, resource_pool_manager, pod, pod_config, host):
    """Handles the rebuilding of resources if rebuild flag is enabled."""
    vm_manager.logger.info(f'P{pod} - Rebuild flag is enabled.')
    vm_manager.logger.info(f'P{pod} - Deleting folder {pod_config["folder_name"]}')
    vm_manager.delete_folder(pod_config["folder_name"], force=True)

    for network in pod_config['network']:
        network_manager.logger.info(f'P{pod} - Deleting vswitch {network["switch_name"]}')
        network_manager.delete_vswitch(host.fqdn, network['switch_name'])

    resource_pool_manager.logger.info(f'P{pod} - Deleting resource pool {pod_config["group_name"]}')
    resource_pool_manager.delete_resource_pool(pod_config["group_name"])


def create_resource_pool(resource_pool_manager, host, pod, pod_config):
    """Creates a resource pool for the pod."""
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
        resource_pool_manager.logger.info(f'P{pod} - Creating resource pool {pod_config["group_name"]}')
        resource_pool_manager.create_resource_pool(host.resource_pool,
                                                   pod_config["group_name"],
                                                   cpu_allocation,
                                                   memory_allocation)
    except Exception as e:
        resource_pool_manager.logger.error(f"An error occurred: {e}")
        sys.exit(1)


def assign_role_to_resource_pool(resource_pool_manager, pod_config):
    """Assigns a user and role to the created resource pool."""
    resource_pool_manager.assign_role_to_resource_pool(pod_config["group_name"],
                                                       f'{pod_config["domain"]}\\{pod_config["user"]}',
                                                       pod_config["role"])


def create_folder(folder_manager, host, pod, pod_config):
    """Creates a folder for the pod."""
    folder_manager.logger.info(f'P{pod} - Creating folder {pod_config["folder_name"]}')
    folder_manager.create_folder(host.folder, pod_config['folder_name'])


def assign_role_to_folder(folder_manager, pod_config):
    """Assigns a user and role to the created folder."""
    folder_manager.assign_user_to_folder(pod_config["folder_name"],
                                         f'{pod_config["domain"]}\\{pod_config["user"]}',
                                         pod_config["role"])

def add_permissions_to_datastore(permission_manager, pod_config):
    """Adds permissions for a specified user or group to a datastore."""
    permission_manager.add_permissions_to_datastore("checkpoint", 
                                                    f'{pod_config["domain"]}\\{pod_config["user"]}', 
                                                    pod_config["role"])

def create_networks(network_manager, host, pod, pod_config):
    """Creates vSwitches and their associated port groups for the pod."""
    network_manager.logger.info(f'P{pod} - Creating network')
    for network in pod_config['network']:
        network_manager.create_vswitch(host.fqdn, network['switch_name'])
        network_manager.create_vm_port_groups(host.fqdn, network["switch_name"], network["port_groups"])

        network_names = [pg["port_group_name"] for pg in network["port_groups"]]
        network_manager.apply_user_role_to_networks(f'{pod_config["domain"]}\\{pod_config["user"]}',
                                                    pod_config["role"], network_names)

        if network['promiscuous_mode']:
            network_manager.enable_promiscuous_mode(host.fqdn, network['promiscuous_mode'])


def clone_and_configure_vms(vm_manager, network_manager, pod, pod_config, full, selected_components=None):
    """Clones the required VMs, updates their networks, and creates snapshots."""
    components_to_clone = pod_config["components"]
    if selected_components:
        # Filter components based on selected_components
        components_to_clone = [
            component for component in pod_config["components"]
            if component["component_name"] in selected_components
        ]

    for component in components_to_clone:
        vm_manager.logger.name = f'P{pod}'
        clone_vm(vm_manager, pod_config, component, full)
        configure_vm_network(vm_manager, component, pod)
        create_vm_snapshot(vm_manager, component)
        if "maestro" in component["component_name"]:
            drive_name = "CD/DVD drive 1"
            iso_type = "Datastore ISO file"
            datastore_name = "vms"
            iso_path = f"podiso/pod-{pod}-a.iso"
            vm_manager.modify_cd_drive(component["clone_name"], drive_name, iso_type, datastore_name, iso_path, connected=True)

def clone_vm(vm_manager, pod_config, component, full):
    """Clones a VM based on the component configuration."""
    if not full:
        vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
        if not vm_manager.snapshot_exists(component["base_vm"], "base"):
            vm_manager.create_snapshot(component["base_vm"], "base",
                                       description="Snapshot used for creating linked clones.")
        vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], "base",
                                       pod_config["group_name"],
                                       directory_name=pod_config["folder_name"])
    else:
        vm_manager.logger.info(f'Cloning component {component["clone_name"]}.')
        vm_manager.clone_vm(component["base_vm"], component["clone_name"],
                            pod_config["group_name"], directory_name=pod_config["folder_name"])


def configure_vm_network(vm_manager, component, pod):
    """Updates the VM network settings for a cloned component."""
    vm_manager.logger.info(f'Updating VM networks for {component["clone_name"]}.')
    vm_network = vm_manager.get_vm_network(component["base_vm"])
    updated_vm_network = update_network_dict(vm_network, int(pod))
    vm_manager.update_vm_network(component["clone_name"], updated_vm_network)


def create_vm_snapshot(vm_manager, component):
    """Creates a snapshot on the cloned VM."""
    snapshot_name = "base"
    if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
        vm_manager.logger.info(f'Creating "base" snapshot on {component["clone_name"]}.')
        vm_manager.create_snapshot(component["clone_name"], snapshot_name,
                                   description=f"Snapshot of {component['clone_name']}")


def power_on_components(vm_manager, pod_config, thread, selected_components=None):
    """Powers on all components of the pod in parallel using threading."""
    components_to_clone = pod_config["components"]
    if selected_components:
        # Filter components based on selected_components
        components_to_clone = [
            component for component in pod_config["components"]
            if component["component_name"] in selected_components
        ]
    with ThreadPoolExecutor(max_workers=thread) as executor:
        futures = []
        vm_manager.logger.info(f'Power on all components.')
        for component in components_to_clone:
            if "state" in component and "poweroff" in component["state"]:
                continue
            poweron_future = executor.submit(vm_manager.poweron_vm, component["clone_name"])
            futures.append(poweron_future)
        wait_for_futures(futures)


def add_to_last_octet(ip_address, number_to_add):
    """
    Adds a specified number to the last octet of an IP address.

    :param ip_address: The original IP address in string format (e.g., "192.168.1.10").
    :param number_to_add: The integer value to add to the last octet.
    :return: A new IP address with the updated last octet.
    """
    # Split the IP address into octets
    octets = ip_address.split('.')

    if len(octets) != 4:
        raise ValueError("Invalid IP address format. Ensure it contains four octets.")

    try:
        # Convert the last octet to an integer and add the given number
        last_octet = int(octets[-1])
        new_last_octet = last_octet + number_to_add - 1

        # Ensure the new last octet is within the valid range (0-255)
        if not 0 <= new_last_octet <= 255:
            raise ValueError("Resulting last octet is out of range (0-255).")

        # Form the new IP address with the updated last octet
        new_ip_address = '.'.join(octets[:-1] + [str(new_last_octet)])
        return new_ip_address

    except ValueError as e:
        raise ValueError(f"Error processing IP address: {e}")


def teardown_pod(service_instance, pod_config, hostname):

    host = get_host_by_name(hostname)
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)

    vm_manager.delete_folder(pod_config["folder_name"], force=True)
    for network in pod_config['network']:
        network_manager.delete_vswitch(host.fqdn, network['switch_name'])
    resource_pool_manager.delete_resource_pool(pod_config["group_name"])


def add_to_prtg(pod_config, pod):
    logger = setup_logger()
    
    for server in checkpoint_server_info["servers"]:
        prtg_obj = PRTGManager(server["url"], server["apitoken"])
        if prtg_obj.get_up_sensor_count() >= 500:
            continue
        device_id = prtg_obj.search_device(pod_config["prtg_container"], pod_config["prtg_object_name"])
        if device_id:
            if not prtg_obj.get_device_status(device_id):
                base_device_ip = prtg_obj.get_device_ip(pod_config["prtg_object"])
                new_deviceip = add_to_last_octet(base_device_ip, pod)
                prtg_obj.set_device_ip(device_id, new_deviceip)
                prtg_obj.enable_device(device_id)
        else:
            device_id = prtg_obj.clone_device(pod_config["prtg_object"], pod_config["prtg_container"], pod_config["prtg_object_name"])
            base_device_ip = prtg_obj.get_device_ip(pod_config["prtg_object"])
            new_deviceip = add_to_last_octet(base_device_ip, pod)
            prtg_obj.set_device_ip(device_id, new_deviceip)
            status = prtg_obj.enable_device(device_id)
            if status:
                logger.info(f"Successfully enabled monitor {device_id}.")
                break
            else: 
                logger.error(f"Failed to enable monitor {device_id}.")