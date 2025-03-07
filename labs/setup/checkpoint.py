from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
import sys
import re
from monitor.prtg import PRTGManager
from logger.log_config import setup_logger


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
        if "maestro" in pod_config["course_name"]:
            pod_resource_pool = f'cp-maestro-pod{pod_config["pod_number"]}'
        else:
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
            cd_drive_info = vm_manager.get_cd_drive(component['clone_name'])
            drive_name = "CD/DVD drive 1"
            iso_type = "Datastore ISO file"
            if "datastore" in cd_drive_info['CD/DVD drive 1']['datastore']:
                datastore_name = "datastore2-ho"
            else:
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
    

def perm_only_cp_pod(service_instance, pod_config):
    """
    For a given pod configuration, only apply permission functions:
      1. Assign user to folder.
      2. Assign role to resource pool.
      3. Apply user role to networks.
    """
    from managers.folder_manager import FolderManager
    from managers.resource_pool_manager import ResourcePoolManager
    from managers.network_manager import NetworkManager

    folder_manager = FolderManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)
    network_manager = NetworkManager(service_instance)

    # Define user, domain, and role.
    user = f"labcp-{pod_config['pod_number']}"
    domain = "vcenter.rededucation.com"
    role = "labcp-0-role"

    # Determine folder name and resource pool name based on course name.
    if "maestro" in pod_config["course_name"]:
        folder_name = f'cp-maestro-{pod_config["pod_number"]}-folder'
        pod_resource_pool = f'cp-maestro-pod{pod_config["pod_number"]}'
    else:
        folder_name = f'cp-pod{pod_config["pod_number"]}-folder'
        pod_resource_pool = f'cp-pod{pod_config["pod_number"]}'

    # Gather network names from pod_config.
    network_names = []
    for network in pod_config.get('networks', []):
        network_names.extend([pg["port_group_name"] for pg in network.get("port_groups", [])])

    # Call the permission functions.
    folder_manager.logger.info("Assigning user to folder '%s'.", folder_name)
    folder_manager.assign_user_to_folder(folder_name, f'{domain}\\{user}', role)

    resource_pool_manager.logger.info("Assigning role to resource pool '%s'.", pod_resource_pool)
    resource_pool_manager.assign_role_to_resource_pool(pod_resource_pool, f'{domain}\\{user}', role)

    network_manager.logger.info("Applying user role to networks: %s.", network_names)
    network_manager.apply_user_role_to_networks(f'{domain}\\{user}', role, network_names)



def add_monitor(pod_config, db_client, prtg_server=None):
    """
    Adds or updates a PRTG monitor for a given pod configuration.

    This function performs the following steps:
      1. Extracts and validates the pod number from the pod_config.
      2. Determines a base IP address based on the course name and host details:
         - If the course name contains "maestro", a specific base IP is used.
         - Otherwise, a default base IP is used.
      3. Computes a new IP address by adding the pod number to the last octet of the base IP.
         - If the computed last octet exceeds 255, the function returns None.
      4. Retrieves the PRTG server configuration for vendor "cp" from the MongoDB database.
         - If a specific prtg_server name is provided, it filters the servers to use only the matching one.
      5. Iterates over the filtered servers:
         - Retrieves the current number of "up" sensors and the sensor count required by the template.
         - Skips any server that would exceed the sensor limit (499) if the new monitor is added.
         - Extracts required PRTG configuration details (container ID and monitor name) from pod_config.
         - Searches for an existing monitor:
             * If found and inactive, updates its IP and enables it.
             * Otherwise, clones a new monitor from the template, sets its IP, and enables it.
         - Constructs and returns the monitor URL if the operation is successful.
      6. If no server could add or update the monitor, logs an error and returns None.

    Args:
        pod_config (dict): Dictionary containing the pod configuration. Expected keys include:
            - "host_fqdn": The fully qualified domain name of the host.
            - "pod_number": The pod number (should be convertible to an integer).
            - "course_name": The course name, used to determine the base IP.
            - "prtg": A sub-dictionary with keys:
                - "object": The template device object ID.
                - "container": The PRTG container (group) ID.
                - "name": The monitor name to be used.
        db_client (MongoClient): An active MongoDB client used to access the "labbuild_db" database.
        prtg_server (str, optional): Specific PRTG server name to use. If provided, only the server
            with a matching "name" field in the configuration will be used. Defaults to None.

    Returns:
        str or None: The URL of the added or updated PRTG monitor if successful, or None if the operation fails.
    """
    # Set up logging for the monitor addition process.
    logger = setup_logger()

    # Extract the short host name from the fully qualified domain name.
    host = pod_config["host_fqdn"].split(".")[0]

    # Validate and convert the pod number to an integer.
    try:
        pod_number = int(pod_config.get("pod_number"))
    except (TypeError, ValueError):
        logger.error("Invalid or missing pod number in pod_config.")
        return None

    # Determine the base IP address based on the course name.
    course_name = pod_config.get("course_name", "")
    if "maestro" in course_name.lower():
        # Use a specific base IP for courses containing "maestro".
        base_ip = "172.26.4.200" if host.lower() == "hotshot" else "172.30.4.200"
    else:
        # Default base IP for non-maestro courses.
        base_ip = "172.26.4.100" if host.lower() == "hotshot" else "172.30.4.100"

    # Split the base IP into its constituent parts.
    base_ip_parts = base_ip.split('.')
    try:
        # Convert the last octet of the base IP to an integer.
        base_last_octet = int(base_ip_parts[3])
    except ValueError:
        logger.error("Invalid base IP: %s", base_ip)
        return None

    # Compute the new last octet by adding the pod number.
    new_last_octet = base_last_octet + pod_number
    if new_last_octet > 255:
        logger.error("Resulting IP's last octet (%s) exceeds 255.", new_last_octet)
        return None

    # Reconstruct the new IP address using the first three octets and the new last octet.
    new_ip = ".".join(base_ip_parts[:3] + [str(new_last_octet)])
    logger.debug("Computed new IP for PRTG monitor: %s", new_ip)

    # Access the PRTG configuration from the "labbuild_db" database.
    db = db_client["labbuild_db"]
    collection = db["prtg"]
    server_data = collection.find_one({"vendor_shortcode": "cp"})
    if not server_data or "servers" not in server_data:
        logger.error("No PRTG server configuration found for vendor 'cp'.")
        return None

    # If a specific PRTG server is specified, filter the servers to match that name.
    if prtg_server:
        servers = [server for server in server_data["servers"] if server.get("name") == prtg_server]
        if not servers:
            logger.error("PRTG server '%s' not found in configuration.", prtg_server)
            return None
    else:
        servers = server_data["servers"]

    # Iterate over the selected servers to attempt monitor creation or update.
    for server in servers:
        # Create a PRTGManager instance for interacting with the server.
        prtg_obj = PRTGManager(server["url"], server["apitoken"])
        # Retrieve the current count of sensors that are up.
        current_sensor_count = prtg_obj.get_up_sensor_count()
        # Get the sensor count required by the template device.
        template_obj_id = pod_config.get("prtg", {}).get("object")
        template_sensor_count = prtg_obj.get_template_sensor_count(template_obj_id)
        # Skip this server if adding the new sensor would exceed the sensor limit.
        if (current_sensor_count + template_sensor_count) >= 499:
            logger.info("Server %s would exceed sensor limits (current: %s, template: %s); skipping.",
                        server["url"], current_sensor_count, template_sensor_count)
            continue

        # Extract necessary configuration details for the monitor from pod_config.
        container_id = pod_config.get("prtg", {}).get("container")
        clone_name = pod_config.get("prtg", {}).get("name")
        if not container_id or not clone_name or not template_obj_id:
            logger.error("Missing required PRTG configuration in pod_config (container, name, or object).")
            continue

        # Search for an existing monitor device by container and monitor name.
        device_id = prtg_obj.search_device(container_id, clone_name)
        if device_id:
            # If the device exists but is not enabled, update its IP and enable it.
            if not prtg_obj.get_device_status(device_id):
                prtg_obj.set_device_ip(device_id, new_ip)
                prtg_obj.enable_device(device_id)
        else:
            # If no device exists, clone a new device from the template.
            device_id = prtg_obj.clone_device(template_obj_id, container_id, clone_name)
            if not device_id:
                logger.error("Failed to clone device for %s.", clone_name)
                continue
            # Set the new IP address for the cloned device.
            prtg_obj.set_device_ip(device_id, new_ip)
            # Enable the newly cloned device; if it fails, log an error.
            if not prtg_obj.enable_device(device_id):
                logger.error("Failed to enable monitor %s.", device_id)
                continue

        # Construct the monitor URL based on the server URL and device ID.
        monitor_url = f"{server['url']}/device.htm?id={device_id}"
        logger.info("PRTG monitor added successfully: %s", monitor_url)
        return monitor_url

    # If the loop completes without returning, no server was able to add/update the monitor.
    logger.error("Failed to add/update monitor on any available PRTG server.")
    return None