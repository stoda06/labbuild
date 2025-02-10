from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
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
        connected_at_power_on = details['connected_at_power_on']

        # Update the network name if it contains "vsX" (where X is any number)
        network_name = update_network_name(network_name, pod_number)

        # Update the MAC address if the network name contains "rdp"
        if 'rdp' in network_name:
            mac_address = update_mac_address(mac_address)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address,
            'connected_at_power_on': connected_at_power_on
        }

    return updated_network_dict


def build_cp_pod(service_instance, pod_config, rebuild=False, thread=4, full=False, selected_components=None):

    vm_manager = VmManager(service_instance)
    folder_manager = FolderManager(service_instance)
    network_manager = NetworkManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)
    permission_manager = PermissionManager(service_instance)

    create_resource_pool(resource_pool_manager, pod_config)

    create_folder(folder_manager, pod_config)
    # add_permissions_to_datastore(permission_manager, pod_config)

    create_networks(network_manager, pod_config)
    clone_and_configure_vms(vm_manager, pod_config, full, rebuild, selected_components)

    power_on_components(vm_manager, pod_config, thread, selected_components)


def create_resource_pool(resource_pool_manager, pod_config):
    """Creates a resource pool for the pod."""
    try:
        parent_resource_pool = pod_config["vendor_shortcode"] + "-" + pod_config["host_fqdn"].split(".")[0]
        pod_resource_pool =  f'{pod_config["vendor_shortcode"]}-pod{pod_config["pod_number"]}'
        user = f"labcp-{pod_config['pod_number']}"
        domain = "vcenter.rededucation.com"
        role = "labcp-0-role"
        resource_pool_manager.logger.info(f'Creating resource pool {pod_resource_pool}')
        resource_pool_manager.create_resource_pool(parent_resource_pool, pod_resource_pool)
        resource_pool_manager.assign_role_to_resource_pool(pod_resource_pool, f'{domain}\\{user}', role)
    except Exception as e:
        resource_pool_manager.logger.error(f"An error occurred: {e}")
        sys.exit(1)  


def create_folder(folder_manager, pod_config):
    """Creates a folder for the pod."""
    try:
        if "maestro" in pod_config["course_name"]:
            folder_name = f'cp-maestro-{pod_config["pod_number"]}-folder'
        else:
            folder_name = f'cp-pod{pod_config["pod_number"]}-folder'
        user = f"labcp-{pod_config['pod_number']}"
        domain = "vcenter.rededucation.com"
        role = "labcp-0-role"
        folder_manager.logger.info(f'Creating folder {folder_name}')
        folder_manager.create_folder(pod_config["vendor_shortcode"], folder_name)
        folder_manager.assign_user_to_folder(folder_name, f'{domain}\\{user}', role)
    except Exception as e:
        folder_manager.logger.error(f"An error occurred: {e}")
    

def add_permissions_to_datastore(permission_manager, pod_config):
    """Adds permissions for a specified user or group to a datastore."""
    user = f"labcp-{pod_config['pod_number']}"
    domain = "vcenter.rededucation.com"
    role = "labcp-0-role"
    permission_manager.add_permissions_to_datastore("checkpoint", f'{domain}\\{user}', role)

def create_networks(network_manager, pod_config):
    """Creates vSwitches and their associated port groups for the pod."""
    user = f"labcp-{pod_config['pod_number']}"
    domain = "vcenter.rededucation.com"
    role = "labcp-0-role"
    network_manager.logger.info(f'Creating network')
    for network in pod_config['networks']:
        network_manager.create_vswitch(pod_config["host_fqdn"], network['switch_name'])
        network_manager.create_vm_port_groups(pod_config["host_fqdn"], network["switch_name"], network["port_groups"])

        network_names = [pg["port_group_name"] for pg in network["port_groups"]]
        network_manager.apply_user_role_to_networks(f'{domain}\\{user}', role, network_names)

        if network['promiscuous_mode']:
            network_manager.enable_promiscuous_mode(pod_config["host_fqdn"], network['promiscuous_mode'])


def clone_and_configure_vms(vm_manager, pod_config, full, rebuild, selected_components=None):
    """Clones the required VMs, updates their networks, and creates snapshots."""
    pod = pod_config["pod_number"]
    components_to_clone = pod_config["components"]
    if selected_components:
        # Filter components based on selected_components
        components_to_clone = [
            component for component in pod_config["components"]
            if component["component_name"] in selected_components
        ]

    for component in components_to_clone:
        if rebuild:
            vm_manager.delete_vm(component['clone_name'])
        clone_vm(vm_manager, pod_config, component, full)
        configure_vm_network(vm_manager, component, pod)
        create_vm_snapshot(vm_manager, component)
        if "maestro" in component["component_name"]:
            drive_name = "CD/DVD drive 1"
            iso_type = "Datastore ISO file"
            datastore_name = "keg2"
            iso_path = f"podiso/pod-{pod}-a.iso"
            vm_manager.modify_cd_drive(component["clone_name"], drive_name, iso_type, datastore_name, iso_path, connected=True)

def clone_vm(vm_manager, pod_config, component, full):
    """Clones a VM based on the component configuration."""
    if "maestro" in pod_config["course_name"]:
        group_name = f'cp-maestro-pod{pod_config["pod_number"]}'
        folder_name = f'cp-maestro-{pod_config["pod_number"]}-folder'
    else:
        group_name = f'cp-pod{pod_config["pod_number"]}'
        folder_name = f'cp-pod{pod_config["pod_number"]}-folder'
    if not full:
        vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
        if not vm_manager.snapshot_exists(component["base_vm"], "base"):
            vm_manager.create_snapshot(component["base_vm"], "base",
                                       description="Snapshot used for creating linked clones.")
        vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], "base", 
                                       group_name, directory_name=folder_name)
    else:
        vm_manager.logger.info(f'Cloning component {component["clone_name"]}.')
        vm_manager.clone_vm(component["base_vm"], component["clone_name"],
                            group_name, directory_name=folder_name)


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
            if component.get("state") == "poweroff":
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


def teardown_pod(service_instance, pod_config):

    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)

    if "maestro" in pod_config["course_name"]:
        group_name = f'cp-maestro-pod{pod_config["pod_number"]}'
        folder_name = f'cp-maestro-{pod_config["pod_number"]}-folder'
    else:
        group_name = f'cp-pod{pod_config["pod_number"]}'
        folder_name = f'cp-pod{pod_config["pod_number"]}-folder'

    vm_manager.delete_folder(folder_name, force=True)
    for network in pod_config['networks']:
        network_manager.delete_vswitch(pod_config["host_fqdn"], network['switch_name'])
    resource_pool_manager.delete_resource_pool(group_name)
