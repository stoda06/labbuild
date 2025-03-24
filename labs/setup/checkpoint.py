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


def build_cp_pod(service_instance, pod_config, rebuild=False, thread=4, full=False, selected_components=None) -> tuple:
    """
    Build and configure a control plane pod for a lab environment.

    This function performs several sequential steps:
      1. Create network resources (vSwitches, port groups, roles, and promiscuous mode if needed)
      2. Create and assign a resource pool
      3. Create and assign a folder for the pod
      4. Clone and configure each component's VM (linked or full clone based on the 'full' flag)
      5. Power on VMs concurrently

    Each step returns a tuple (success, failed_step, error_message). In case of failure, the function stops 
    and returns immediately with the step name and error message. On complete success, it returns (True, None, None).

    Parameters:
        service_instance : object
            The vCenter service instance used to initialize managers.
        pod_config : dict
            Dictionary with configuration details for the pod (networks, components, course name, etc.).
        rebuild : bool, optional
            If True, deletes existing VMs before cloning. Defaults to False.
        thread : int, optional
            Maximum number of threads to use when powering on VMs concurrently. Defaults to 4.
        full : bool, optional
            If True, performs a full clone; otherwise, a linked clone is performed. Defaults to False.
        selected_components : list, optional
            A list of component names to process. If None, all components are processed.

    Returns:
        tuple: A tuple (success, failed_step, error_message) where:
            - success (bool): True if all steps succeed, otherwise False.
            - failed_step (str): Name of the step where the failure occurred.
            - error_message (str): Description of the failure.
    """
    # Initialize managers for VM, folder, network, and resource pool operations.
    vm_mgr = VmManager(service_instance)
    folder_mgr = FolderManager(service_instance)
    network_mgr = NetworkManager(service_instance)
    rp_mgr = ResourcePoolManager(service_instance)

    # Global variables based on pod configuration.
    pod_number = pod_config["pod_number"]
    domain_user = f"vcenter.rededucation.com\\labcp-{pod_number}"
    role = "labcp-0-role"

    # Determine naming convention based on course type.
    if "maestro" in pod_config["course_name"]:
        folder_name = f"cp-maestro-{pod_number}-folder"
        group_name = f"cp-maestro-pod{pod_number}"
        rp_name = f"cp-maestro-pod{pod_number}"
    else:
        folder_name = f"cp-pod{pod_number}-folder"
        group_name = f"cp-pod{pod_number}"
        rp_name = f"{pod_config['vendor_shortcode']}-pod{pod_number}"

    # ==============================
    # STEP 1: Create Network Resources
    # ==============================
    for net in pod_config["networks"]:
        # Create a vSwitch on the specified host.
        if not network_mgr.create_vswitch(pod_config["host_fqdn"], net["switch_name"]):
            return False, "create_vswitch", f"Failed creating vswitch {net['switch_name']}"

        # Create VM port groups on the newly created vSwitch.
        if not network_mgr.create_vm_port_groups(pod_config["host_fqdn"], net["switch_name"], net["port_groups"]):
            return False, "create_vm_port_groups", f"Failed creating port groups on {net['switch_name']}"

        # Extract port group names to apply the user role.
        pg_names = [pg["port_group_name"] for pg in net["port_groups"]]
        if not network_mgr.apply_user_role_to_networks(domain_user, role, pg_names):
            return False, "apply_user_role_to_networks", f"Failed applying role to networks {pg_names}"

        # Enable promiscuous mode if it is specified in the network config.
        if net.get("promiscuous_mode") and not network_mgr.enable_promiscuous_mode(pod_config["host_fqdn"], net["promiscuous_mode"]):
            return False, "enable_promiscuous_mode", f"Failed enabling promiscuous mode on {net['switch_name']}"

    # ==============================
    # STEP 2: Create Resource Pool
    # ==============================
    parent_rp = f"{pod_config['vendor_shortcode']}-{pod_config['host_fqdn'].split('.')[0]}"
    if not rp_mgr.create_resource_pool(parent_rp, rp_name):
        return False, "create_resource_pool", f"Failed creating resource pool {rp_name}"
    if not rp_mgr.assign_role_to_resource_pool(rp_name, domain_user, role):
        return False, "assign_role_to_resource_pool", f"Failed assigning role to resource pool {rp_name}"

    # ==============================
    # STEP 3: Create Folder
    # ==============================
    if not folder_mgr.create_folder(pod_config["vendor_shortcode"], folder_name):
        return False, "create_folder", f"Failed creating folder {folder_name}"
    if not folder_mgr.assign_user_to_folder(folder_name, domain_user, role):
        return False, "assign_user_to_folder", f"Failed assigning user to folder {folder_name}"

    # ==============================
    # STEP 4: Clone and Configure Components
    # ==============================
    components = pod_config["components"]
    # Filter components if selected_components is provided.
    if selected_components:
        components = [c for c in components if c["component_name"] in selected_components]

    for comp in components:
        clone = comp["clone_name"]
        base = comp["base_vm"]

        # If rebuild is True, delete the existing VM.
        if rebuild and not vm_mgr.delete_vm(clone):
            return False, "delete_vm", f"Failed deleting existing VM {clone}"

        # Clone operation: perform a linked clone if 'full' is False.
        if not full:
            # Create a snapshot if it does not exist.
            if not vm_mgr.snapshot_exists(base, "base") and not vm_mgr.create_snapshot(base, "base", "Base snapshot"):
                return False, "create_snapshot", f"Failed creating snapshot on {base}"
            if not vm_mgr.create_linked_clone(base, clone, "base", group_name, directory_name=folder_name):
                return False, "create_linked_clone", f"Failed linked cloning {clone}"
        # Perform a full clone if 'full' is True.
        else:
            if not vm_mgr.clone_vm(base, clone, group_name, directory_name=folder_name):
                return False, "clone_vm", f"Failed full cloning {clone}"

        # Update the network configuration of the cloned VM.
        updated_network = update_network_dict(vm_mgr.get_vm_network(base), int(pod_number))
        if not vm_mgr.update_vm_network(clone, updated_network):
            return False, "update_vm_network", f"Failed updating network on {clone}"

        # Create a base snapshot for the newly cloned VM if it doesn't exist.
        if not vm_mgr.snapshot_exists(clone, "base") and not vm_mgr.create_snapshot(clone, "base", f"Snapshot of {clone}"):
            return False, "create_snapshot", f"Failed creating base snapshot on {clone}"

        # For maestro-specific components, modify the CD/DVD drive settings.
        if "maestro" in comp["component_name"]:
            # Choose the datastore based on the host FQDN.
            datastore = "datastore2-ho" if "hotshot" in pod_config["host_fqdn"] else "keg2"
            if not vm_mgr.modify_cd_drive(
                clone,
                "CD/DVD drive 1",
                "Datastore ISO file",
                datastore,
                f"podiso/pod-{pod_number}-a.iso",
                connected=True
            ):
                return False, "modify_cd_drive", f"Failed modifying CD drive for {clone}"

    # ==============================
    # STEP 5: Power On Components
    # ==============================
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=thread) as executor:
        # Submit power-on tasks concurrently for VMs that are not marked as "poweroff".
        futures = {executor.submit(vm_mgr.poweron_vm, comp["clone_name"]): comp["clone_name"]
                   for comp in components if comp.get("state") != "poweroff"}
        for future in futures:
            if not future.result():
                return False, "poweron_vm", f"Failed powering on {futures[future]}"

    # If all steps complete successfully, return True.
    return True, None, None



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


