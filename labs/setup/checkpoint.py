from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
from tqdm import tqdm
import re
from monitor.prtg import PRTGManager
from tqdm import tqdm

import logging
logger = logging.getLogger(__name__) # Or logging.getLogger('VmManager')

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
        vswitch_result = network_mgr.create_vswitch(pod_config["host_fqdn"], net["switch_name"])
        if vswitch_result is False:
            return False, "create_vswitch", f"Failed creating vswitch {net['switch_name']}"
        elif vswitch_result == "RESOURCE_LIMIT":
            if not network_mgr.create_vswitch_portgroups(pod_config["host_fqdn"], net["switch_name"], net["port_groups"], resource_limit=vswitch_result):
                return False, "create_vswitch_portgroups", f"Failed creating port groups"
        if vswitch_result is True:
            if not network_mgr.create_vswitch_portgroups(pod_config["host_fqdn"], net["switch_name"], net["port_groups"]):
                return False, "create_vswitch_portgroups", f"Failed creating port groups on {net['switch_name']}"
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
    Adds or updates a PRTG monitor for a Checkpoint pod.

    Ensures that only one monitor with the target name exists across all configured
    Checkpoint PRTG servers by first searching and deleting any duplicates found,
    then creating the new monitor on an appropriate server.

    Args:
        pod_config (dict): Pod configuration containing PRTG details and pod info.
        db_client (pymongo.MongoClient): Active MongoDB client.
        prtg_server (str, optional): Specific PRTG server name to target for creation.
                                     If None, selects the first available server.

    Returns:
        str or None: The URL of the created PRTG monitor, or None on failure.
    """

    # --- 1. Extract Monitor Details & Calculate IP ---
    try:
        pod_number = int(pod_config.get("pod_number"))
        prtg_details = pod_config.get("prtg", {})
        monitor_name = prtg_details.get("name")
        container_id = prtg_details.get("container")
        template_id = prtg_details.get("object")

        if not all([monitor_name, container_id, template_id]):
            logger.error("Missing required PRTG config (name, container, object) in pod_config.")
            return None

        # Determine base IP based on host (example logic, adjust if needed)
        host_short = pod_config.get("host_fqdn", "").split(".")[0]
        if "maestro" in pod_config.get("course_name", "").lower():
            base_ip = "172.26.4.200" if host_short.lower() == "hotshot" else "172.30.4.200"
        else:
            base_ip = "172.26.4.100" if host_short.lower() == "hotshot" else "172.30.4.100"

        parts = base_ip.split(".")
        base_last_octet = int(parts[3])
        new_last_octet = base_last_octet + pod_number
        if new_last_octet > 255:
            logger.error(f"Computed IP last octet {new_last_octet} exceeds 255 for pod {pod_number}.")
            return None
        new_ip = ".".join(parts[:3] + [str(new_last_octet)])
        logger.debug(f"Target IP for monitor '{monitor_name}': {new_ip}")

    except (TypeError, ValueError, KeyError) as e:
        logger.error(f"Error processing pod_config for PRTG details: {e}", exc_info=True)
        return None

    # --- 2. Get Configured Checkpoint PRTG Servers ---
    try:
        db = db_client["labbuild_db"]
        prtg_conf = db["prtg"].find_one({"vendor_shortcode": "cp"})
        if not prtg_conf or not prtg_conf.get("servers"):
            logger.error("No PRTG server configuration found for vendor 'cp' in database.")
            return None
        all_cp_servers = prtg_conf["servers"]
    except Exception as e:
        logger.error(f"Failed to retrieve PRTG server configuration from DB: {e}", exc_info=True)
        return None

    # --- 3. Search All Servers and Delete Existing Monitors ---
    logger.info(f"Searching for existing monitor '{monitor_name}' on all CP PRTG servers...")
    deleted_count = 0
    for server in all_cp_servers:
        server_url = server.get("url")
        api_token = server.get("apitoken")
        server_name = server.get("name", server_url) # Use name for logging if available
        if not server_url or not api_token:
            logger.warning(f"Skipping server {server_name}: Missing URL or API token.")
            continue

        try:
            prtg_mgr = PRTGManager(server_url, api_token)
            existing_id = prtg_mgr.search_device(container_id, monitor_name)
            if existing_id:
                logger.warning(f"Found existing monitor '{monitor_name}' (ID: {existing_id}) on server {server_name}. Deleting...")
                if prtg_mgr.delete_monitor_by_id(existing_id):
                    logger.info(f"Successfully deleted monitor ID {existing_id} from {server_name}.")
                    deleted_count += 1
                else:
                    logger.error(f"Failed to delete monitor ID {existing_id} from {server_name}.")
                    # Decide whether to proceed or fail here. Let's proceed but log error.
            # else: logger.debug(f"Monitor '{monitor_name}' not found on server {server_name}.") # Optional debug
        except Exception as e:
            logger.error(f"Error checking/deleting monitor on server {server_name}: {e}", exc_info=True)
            # Continue checking other servers even if one fails

    if deleted_count > 0:
        logger.info(f"Finished deleting {deleted_count} pre-existing monitor(s) named '{monitor_name}'.")

    # --- 4. Select Target Server for Creation ---
    target_server_info = None
    if prtg_server: # Specific server requested
        target_server_info = next((s for s in all_cp_servers if s.get("name") == prtg_server), None)
        if not target_server_info:
            logger.error(f"Specified target PRTG server '{prtg_server}' not found in 'cp' configuration.")
            return None
        logger.info(f"Target server specified: {prtg_server}")
    else: # No specific server requested, find first available
        logger.info("No target server specified, finding first available based on capacity...")
        for server in all_cp_servers:
            server_url = server.get("url")
            api_token = server.get("apitoken")
            server_name = server.get("name", server_url)
            if not server_url or not api_token: continue # Skip incomplete configs

            try:
                prtg_mgr = PRTGManager(server_url, api_token)
                current_sensor_count = prtg_mgr.get_up_sensor_count()
                template_sensor_count = prtg_mgr.get_template_sensor_count(template_id)

                if (current_sensor_count + template_sensor_count) < 499: # Example limit
                    logger.info(f"Selected server {server_name} (Sensors: {current_sensor_count}+{template_sensor_count} < 499)")
                    target_server_info = server
                    break # Found a suitable server
                else:
                    logger.warning(f"Server {server_name} skipped: capacity limit would be exceeded ({current_sensor_count}+{template_sensor_count} >= 499).")
            except Exception as e:
                logger.error(f"Error checking capacity on server {server_name}: {e}")
                # Continue to check next server

    if not target_server_info:
        logger.error(f"Could not find any suitable target PRTG server for monitor '{monitor_name}'.")
        return None

    # --- 5. Create New Monitor on Target Server ---
    target_url = target_server_info.get("url")
    target_token = target_server_info.get("apitoken")
    target_name = target_server_info.get("name", target_url)

    if not target_url or not target_token:
         logger.error(f"Selected target server {target_name} has incomplete configuration (URL/Token).")
         return None

    logger.info(f"Attempting to create monitor '{monitor_name}' on target server: {target_name}")
    try:
        prtg_target_mgr = PRTGManager(target_url, target_token)

        # Optional: Double-check capacity again right before cloning
        current_count = prtg_target_mgr.get_up_sensor_count()
        template_count = prtg_target_mgr.get_template_sensor_count(template_id)
        if (current_count + template_count) >= 499:
            logger.error(f"Target server {target_name} capacity check failed just before creation.")
            return None # Capacity filled between selection and creation

        new_device_id = prtg_target_mgr.clone_device(template_id, container_id, monitor_name)
        if not new_device_id:
            logger.error(f"Failed to clone device '{monitor_name}' on {target_name}.")
            return None

        logger.info(f"Cloned device ID {new_device_id} on {target_name}. Setting IP...")
        if not prtg_target_mgr.set_device_ip(new_device_id, new_ip):
            logger.error(f"Failed to set IP '{new_ip}' for device ID {new_device_id} on {target_name}.")
            # Consider deleting the partially created device here? Or leave it paused?
            return None # Fail if IP set fails

        logger.info(f"Set IP for device ID {new_device_id}. Enabling...")
        if not prtg_target_mgr.enable_device(new_device_id):
            logger.error(f"Failed to enable monitor ID {new_device_id} on {target_name}.")
            # Consider deleting?
            return None # Fail if enable fails

        monitor_url = f"{target_url}/device.htm?id={new_device_id}"
        logger.info(f"Successfully created and enabled PRTG monitor: {monitor_url}")
        return monitor_url

    except Exception as e:
        logger.error(f"Error during monitor creation on target server {target_name}: {e}", exc_info=True)
        return None