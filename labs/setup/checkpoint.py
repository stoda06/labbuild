from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
from tqdm import tqdm
import re
from monitor.prtg import PRTGManager
from logger.log_config import setup_logger
from tqdm import tqdm


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
    vm_mgr = VmManager(service_instance)
    folder_mgr = FolderManager(service_instance)
    network_mgr = NetworkManager(service_instance)
    rp_mgr = ResourcePoolManager(service_instance)

    pod_number = pod_config["pod_number"]
    domain_user = f"vcenter.rededucation.com\\labcp-{pod_number}"
    role = "labcp-0-role"

    if "maestro" in pod_config["course_name"]:
        folder_name = f"cp-maestro-{pod_number}-folder"
        group_name = f"cp-maestro-pod{pod_number}"
        rp_name = f"cp-maestro-pod{pod_number}"
    else:
        folder_name = f"cp-pod{pod_number}-folder"
        group_name = f"cp-pod{pod_number}"
        rp_name = f"{pod_config['vendor_shortcode']}-pod{pod_number}"

    # STEP 1: Network resources (with progress bar)
    for net in tqdm(pod_config["networks"], desc=f"Pod {pod_number} → network setup", unit="net"):
        if not network_mgr.create_vswitch(pod_config["host_fqdn"], net["switch_name"]):
            return False, "create_vswitch", f"Failed creating vswitch {net['switch_name']}"
        if not network_mgr.create_vm_port_groups(pod_config["host_fqdn"], net["switch_name"], net["port_groups"]):
            return False, "create_vm_port_groups", f"Failed creating port groups on {net['switch_name']}"
        pg_names = [pg["port_group_name"] for pg in net["port_groups"]]
        if not network_mgr.apply_user_role_to_networks(domain_user, role, pg_names):
            return False, "apply_user_role_to_networks", f"Failed applying role to networks {pg_names}"
        if net.get("promiscuous_mode") and not network_mgr.enable_promiscuous_mode(pod_config["host_fqdn"], net["promiscuous_mode"]):
            return False, "enable_promiscuous_mode", f"Failed enabling promiscuous mode on {net['switch_name']}"

    # STEP 2: Resource pool
    parent_rp = f"{pod_config['vendor_shortcode']}-{pod_config['host_fqdn'].split('.')[0]}"
    if not rp_mgr.create_resource_pool(parent_rp, rp_name):
        return False, "create_resource_pool", f"Failed creating resource pool {rp_name}"
    if not rp_mgr.assign_role_to_resource_pool(rp_name, domain_user, role):
        return False, "assign_role_to_resource_pool", f"Failed assigning role to resource pool {rp_name}"

    # STEP 3: Folder
    if not folder_mgr.create_folder(pod_config["vendor_shortcode"], folder_name):
        return False, "create_folder", f"Failed creating folder {folder_name}"
    if not folder_mgr.assign_user_to_folder(folder_name, domain_user, role):
        return False, "assign_user_to_folder", f"Failed assigning user to folder {folder_name}"

    # STEP 4: Clone & configure
    components = pod_config["components"]
    if selected_components:
        components = [c for c in components if c["component_name"] in selected_components]

    for comp in tqdm(components, desc=f"Pod {pod_number} → cloning/configuring", unit="vm"):
        clone = comp["clone_name"]
        base = comp["base_vm"]

        if rebuild and not vm_mgr.delete_vm(clone):
            return False, "delete_vm", f"Failed deleting existing VM {clone}"

        if not full:
            if not vm_mgr.snapshot_exists(base, "base") and not vm_mgr.create_snapshot(base, "base", "Base snapshot"):
                return False, "create_snapshot", f"Failed creating snapshot on {base}"
            if not vm_mgr.create_linked_clone(base, clone, "base", group_name, directory_name=folder_name):
                return False, "create_linked_clone", f"Failed linked cloning {clone}"
        else:
            if not vm_mgr.clone_vm(base, clone, group_name, directory_name=folder_name):
                return False, "clone_vm", f"Failed full cloning {clone}"

        updated_network = update_network_dict(vm_mgr.get_vm_network(base), int(pod_number))
        if not vm_mgr.update_vm_network(clone, updated_network):
            return False, "update_vm_network", f"Failed updating network on {clone}"
        if not vm_mgr.snapshot_exists(clone, "base") and not vm_mgr.create_snapshot(clone, "base", f"Snapshot of {clone}"):
            return False, "create_snapshot", f"Failed creating base snapshot on {clone}"

        if "maestro" in comp["component_name"]:
            datastore = "datastore2-ho" if "hotshot" in pod_config["host_fqdn"] else "keg2"
            if not vm_mgr.modify_cd_drive(clone, "CD/DVD drive 1", "Datastore ISO file", datastore, f"podiso/pod-{pod_number}-a.iso", connected=True):
                return False, "modify_cd_drive", f"Failed modifying CD drive for {clone}"

    # STEP 5: Power on
    to_power = [comp["clone_name"] for comp in components if comp.get("state") != "poweroff"]
    with ThreadPoolExecutor(max_workers=thread) as executor:
        futures = {executor.submit(vm_mgr.poweron_vm, name): name for name in to_power}
        for future in tqdm(futures, desc=f"Pod {pod_number} → powering on", unit="vm"):
            if not future.result():
                return False, "poweron_vm", f"Failed powering on {futures[future]}"

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
    Add or update a PRTG device (monitor) for a specific pod, ensuring existing monitors are reused.

    This function performs the following steps:
      1. Validate pod_config and compute the target IP address by adding the pod number 
         to a base IP (which differs for “maestro” vs non‑maestro courses, and “hotshot” hosts).
      2. Retrieve PRTG server configuration from MongoDB (vendor_shortcode="cp"), optionally filtering
         to a single server if prtg_server is specified.
      3. Search for an existing device in the specified container with the desired monitor name:
           • If found:
               – Update its IP address.
               – If currently disabled, re-enable it.
               – Return its URL.
      4. If no existing device is found:
           – Verify adding a new device won’t exceed the 499‑sensor limit on that server.
           – Clone a new device from the specified template.
           – Set its IP address and enable it.
           – Return its URL.
      5. If all servers either exceed capacity or cloning/enabling fails, log an error and return None.

    Args:
        pod_config (dict): 
            Required keys:
              • host_fqdn (str): Fully‑qualified domain name of the host.
              • pod_number (int or str): Pod index (will be converted to int).
              • course_name (str): Used to select the base IP block.
              • prtg (dict): Contains:
                   – object (int): Template device object ID.
                   – container (int): PRTG group/container ID.
                   – name (str): Desired monitor name.
        db_client (pymongo.MongoClient): Active MongoDB client for labbuild_db.
        prtg_server (str, optional): Specific PRTG server name to use; if omitted, all configured servers are tried.

    Returns:
        str | None: URL of the created or updated device on success; None on failure.
    """
    logger = setup_logger()

    # Extract short hostname and convert pod_number to integer
    host = pod_config.get("host_fqdn", "").split(".")[0]
    try:
        pod_number = int(pod_config.get("pod_number"))
    except (TypeError, ValueError):
        logger.error("Invalid or missing pod number in pod_config.")
        return None

    # Select base IP based on course name and host
    course_name = pod_config.get("course_name", "").lower()
    if "maestro" in course_name:
        base_ip = "172.26.4.200" if host.lower() == "hotshot" else "172.30.4.200"
    else:
        base_ip = "172.26.4.100" if host.lower() == "hotshot" else "172.30.4.100"

    parts = base_ip.split(".")
    try:
        base_last_octet = int(parts[3])
    except ValueError:
        logger.error("Invalid base IP: %s", base_ip)
        return None

    new_last_octet = base_last_octet + pod_number
    if new_last_octet > 255:
        logger.error("Computed IP last octet %d exceeds 255", new_last_octet)
        return None

    new_ip = ".".join(parts[:3] + [str(new_last_octet)])
    logger.debug("Computed new PRTG IP: %s", new_ip)

    # Retrieve PRTG server configurations from MongoDB
    db = db_client["labbuild_db"]
    prtg_conf = db["prtg"].find_one({"vendor_shortcode": "cp"})
    if not prtg_conf or "servers" not in prtg_conf:
        logger.error("No PRTG server configuration found for vendor 'cp'.")
        return None

    servers = prtg_conf["servers"]
    if prtg_server:
        servers = [s for s in servers if s.get("name") == prtg_server]
        if not servers:
            logger.error("Specified PRTG server '%s' not found", prtg_server)
            return None

    # Validate required PRTG parameters in pod_config
    container_id = pod_config.get("prtg", {}).get("container")
    monitor_name = pod_config.get("prtg", {}).get("name")
    template_id = pod_config.get("prtg", {}).get("object")
    if not all([container_id, monitor_name, template_id]):
        logger.error("Missing PRTG container/name/object in pod_config.")
        return None

    # Attempt update or creation on each configured server
    for server in servers:
        prtg = PRTGManager(server["url"], server["apitoken"])

        # Check for existing device
        existing_id = prtg.search_device(container_id, monitor_name)
        if existing_id:
            prtg.set_device_ip(existing_id, new_ip)
            if not prtg.get_device_status(existing_id):
                prtg.enable_device(existing_id)
                logger.info("Re-enabled existing monitor '%s' (ID=%s)", monitor_name, existing_id)
            else:
                logger.info("Monitor '%s' (ID=%s) already active", monitor_name, existing_id)
            return f"{server['url']}/device.htm?id={existing_id}"

        # Ensure sensor limit not exceeded before cloning
        if (prtg.get_up_sensor_count() + prtg.get_template_sensor_count(template_id)) >= 499:
            logger.info("Skipping server %s: sensor limit would be exceeded", server["url"])
            continue

        # Clone new device from template
        new_id = prtg.clone_device(template_id, container_id, monitor_name)
        if not new_id:
            logger.error("Failed to clone device '%s'", monitor_name)
            continue

        prtg.set_device_ip(new_id, new_ip)
        if not prtg.enable_device(new_id):
            logger.error("Failed to enable new monitor ID=%s", new_id)
            continue

        monitor_url = f"{server['url']}/device.htm?id={new_id}"
        logger.info("Created new PRTG monitor: %s", monitor_url)
        return monitor_url

    logger.error("Unable to add or update monitor on any PRTG server.")
    return None
