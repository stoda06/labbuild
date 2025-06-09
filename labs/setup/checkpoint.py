from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
from tqdm import tqdm
import re
from typing import Optional, Dict, List, Tuple
from monitor.prtg import PRTGManager
from tqdm import tqdm
from pyVmomi import vim

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


def increment_string(s: Optional[str]) -> str:
    """
    Increment the trailing numeric part of a string.
    If the string ends with '-<number>', increments that number.
    Otherwise, appends '-1'.
    Handles None or empty string by returning 'snapshot-1' (or a more generic default).
    """
    if not s:
        return "snapshot-1" # Default for empty or None, can be adjusted

    # Regex to find a hyphen followed by digits at the end of the string
    match = re.search(r'^(.*)-(\d+)$', s)

    if match:
        prefix = match.group(1) # The part before "-<number>"
        number = int(match.group(2))
        return f"{prefix}-{number + 1}"
    else:
        # If no "-<number>" suffix is found, append "-1" to the original string
        return f"{s}-1"


# --- Main Build Function ---
def build_cp_pod(service_instance, pod_config: Dict, rebuild: bool = False, thread: int = 4,
                 full: bool = False, selected_components: Optional[List[str]] = None,
                 clonefrom: Optional[int] = None,
                 source_pod_vms: Optional[Dict[str, str]] = None) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Builds a Checkpoint pod, with optional --clonefrom functionality.
    """
    vm_mgr = VmManager(service_instance)
    folder_mgr = FolderManager(service_instance)
    network_mgr = NetworkManager(service_instance)
    rp_mgr = ResourcePoolManager(service_instance)
    # permission_mgr = PermissionManager(service_instance) # If needed for permissions

    target_pod_number = pod_config["pod_number"]
    domain_user = f"vcenter.rededucation.com\\labcp-{target_pod_number}" # For the target pod
    role_name = "labcp-0-role" # Consistent role name

    # --- Determine Target Resource Pool and Folder Names ---
    if "maestro" in pod_config["course_name"].lower():
        target_group_name = f'cp-maestro-pod{target_pod_number}'
        target_folder_name = f'cp-maestro-{target_pod_number}-folder'
    else:
        target_group_name = f'cp-pod{target_pod_number}'
        target_folder_name = f'cp-pod{target_pod_number}-folder'
    
    target_resource_pool_name = target_group_name # CP RPs are often named same as group

    # STEP 1: Resource Pool for the TARGET pod
    # Parent RP based on target pod's host
    parent_rp_name = f"{pod_config['vendor_shortcode']}-{pod_config['host_fqdn'].split('.')[0]}"
    logger.info(f"Ensuring resource pool '{target_resource_pool_name}' under '{parent_rp_name}' for pod {target_pod_number}.")
    if not rp_mgr.create_resource_pool(parent_rp_name, target_resource_pool_name):
        return False, "create_resource_pool", f"Failed creating resource pool '{target_resource_pool_name}'"
    if not rp_mgr.assign_role_to_resource_pool(target_resource_pool_name, domain_user, role_name):
        return False, "assign_role_to_rp", f"Failed assigning role to '{target_resource_pool_name}'"

    # STEP 2: Folder for the TARGET pod
    logger.info(f"Ensuring folder '{target_folder_name}' for pod {target_pod_number}.")
    if not folder_mgr.create_folder(pod_config["vendor_shortcode"], target_folder_name): # Parent folder is vendor code
        return False, "create_folder", f"Failed creating folder '{target_folder_name}'"
    if not folder_mgr.assign_user_to_folder(target_folder_name, domain_user, role_name):
        return False, "assign_role_to_folder", f"Failed assigning role to '{target_folder_name}'"

    # STEP 3: Network resources for the TARGET pod
    host_fqdn_target = pod_config["host_fqdn"]
    for net_config in tqdm(pod_config.get("networks", []), desc=f"Pod {target_pod_number} → network setup", unit="net"):
        switch_name = net_config["switch_name"]
        vswitch_result = network_mgr.create_vswitch(host_fqdn_target, switch_name)

        if vswitch_result is False:
            return False, "create_vswitch_cp", f"Failed creating vSwitch '{switch_name}'"
        elif vswitch_result == "RESOURCE_LIMIT":
            logger.warning(f"vSwitch '{switch_name}' hit resource limit. Attempting port group creation on alternate...")
            # Assuming create_vswitch_portgroups handles the resource_limit by finding alternate vSwitch
            if not network_mgr.create_vswitch_portgroups(host_fqdn_target, switch_name, net_config["port_groups"], pod_number=target_pod_number, resource_limit=vswitch_result):
                 return False, "create_pg_resource_limit_cp", f"Failed creating port groups (resource limit)"
        elif vswitch_result is True: # vSwitch created or already existed
            if not network_mgr.create_vm_port_groups(host_fqdn_target, switch_name, net_config["port_groups"], pod_number=target_pod_number):
                 return False, "create_vm_port_groups_cp", f"Failed creating port groups on '{switch_name}'"
        
        pg_names = [pg["port_group_name"] for pg in net_config["port_groups"]]
        # Apply permissions for the target pod's user to these networks
        if not network_mgr.apply_user_role_to_networks(domain_user, role_name, pg_names):
            return False, "apply_role_networks_cp", f"Failed applying role to networks: {pg_names}"
        
        if net_config.get("promiscuous_mode"):
            # Pass the list of port group names that need promiscuous mode
            promiscuous_pg_names = net_config.get("promiscuous_mode")
            if isinstance(promiscuous_pg_names, list):
                if not network_mgr.enable_promiscuous_mode(host_fqdn_target, promiscuous_pg_names):
                    return False, "enable_promiscuous_cp", f"Failed enabling promiscuous mode on {switch_name} for {promiscuous_pg_names}"
            else:
                 logger.warning(f"Promiscuous mode configuration for switch {switch_name} is not a list. Skipping.")


    # STEP 4: Clone & Configure VMs for the TARGET pod
    components_to_process = pod_config.get("components", [])
    if selected_components:
        components_to_process = [c for c in components_to_process if c.get("component_name") in selected_components]

    for component in tqdm(components_to_process, desc=f"Pod {target_pod_number} → cloning/configuring", unit="vm"):
        target_vm_name = component["clone_name"].replace("{X}", str(target_pod_number))

        if rebuild: # Use the rebuild flag passed into the function
            logger.info(f"Rebuild: Deleting existing VM '{target_vm_name}' for pod {target_pod_number}.")
            if not vm_mgr.delete_vm(target_vm_name):
                return False, "delete_vm_rebuild_cp", f"Failed deleting existing VM '{target_vm_name}'"

        base_vm_for_clone = component["base_vm"]  # Default: clone from template
        snapshot_for_clone = "base"              # Default: use "base" snapshot of template

        if clonefrom is not None and source_pod_vms:
            component_original_name = component.get("component_name")
            if component_original_name in source_pod_vms:
                source_vm_name_actual = source_pod_vms[component_original_name]
                base_vm_for_clone = source_vm_name_actual # Switch base to the source pod's VM
                logger.info(f"CloneFrom: Target '{target_vm_name}' will be cloned from source VM '{base_vm_for_clone}' (Pod {clonefrom}).")

                source_vm_obj = vm_mgr.get_obj([vim.VirtualMachine], base_vm_for_clone) # type: ignore
                if not source_vm_obj:
                    return False, "source_vm_not_found_cf", f"CloneFrom: Source VM '{base_vm_for_clone}' not found."

                latest_snap_on_source = vm_mgr.get_latest_snapshot_name(source_vm_obj) # Use your new method
                snapshot_for_clone = increment_string(latest_snap_on_source) # New snapshot name

                logger.info(f"CloneFrom: Creating new snapshot '{snapshot_for_clone}' on source VM '{base_vm_for_clone}'.")
                if not vm_mgr.create_snapshot(base_vm_for_clone, snapshot_for_clone,
                                              description=f"For cloning to Pod {target_pod_number} from Pod {clonefrom}"):
                    return False, "create_source_snap_cf", f"Failed creating snapshot '{snapshot_for_clone}' on source '{base_vm_for_clone}'"
            else:
                logger.warning(f"CloneFrom: Component '{component_original_name}' not in source VM map. Defaulting to template '{base_vm_for_clone}'.")
        
        # Perform Cloning
        if not full:
            logger.info(f"Creating linked clone '{target_vm_name}' from '{base_vm_for_clone}' (Snapshot: '{snapshot_for_clone}').")
            if not vm_mgr.create_linked_clone(base_vm_for_clone, target_vm_name, snapshot_for_clone,
                                              target_resource_pool_name, directory_name=target_folder_name):
                return False, "create_linked_clone_cp", f"Failed linked clone for '{target_vm_name}'"
        else:
            logger.info(f"Creating full clone '{target_vm_name}' from '{base_vm_for_clone}'.")
            if not vm_mgr.clone_vm(base_vm_for_clone, target_vm_name,
                                   target_resource_pool_name, directory_name=target_folder_name):
                return False, "clone_vm_cp", f"Failed full clone for '{target_vm_name}'"

        # Network Configuration for the NEWLY CLONED VM (target_vm_name)
        # Always get network layout from the original base template specified in config
        original_template_for_net = component["base_vm"]
        base_vm_network_layout = vm_mgr.get_vm_network(original_template_for_net)
        if base_vm_network_layout is None:
            return False, "get_base_net_layout_cp", f"Could not get network layout from template '{original_template_for_net}'"
        
        # Update network dict using the target pod number
        target_pod_network_config = update_network_dict(base_vm_network_layout, target_pod_number)
        logger.info(f"Updating network for '{target_vm_name}' for pod {target_pod_number}.")
        if not vm_mgr.update_vm_network(target_vm_name, target_pod_network_config):
            return False, "update_vm_network_cp", f"Failed updating network for '{target_vm_name}'"
        # (Optional) connect_networks_to_vm if your update_vm_network doesn't handle connect state.
        if not vm_mgr.connect_networks_to_vm(target_vm_name, target_pod_network_config):
            return False, "connect_networks_to_vm_cp", f"Failed connecting networks for '{target_vm_name}'"


        # Create "base" snapshot on the newly cloned and configured VM
        target_vm_base_snapshot_name = "base"
        if not vm_mgr.snapshot_exists(target_vm_name, target_vm_base_snapshot_name):
            logger.info(f"Creating '{target_vm_base_snapshot_name}' snapshot on target VM '{target_vm_name}'.")
            if not vm_mgr.create_snapshot(target_vm_name, target_vm_base_snapshot_name,
                                          description=f"Base snapshot of '{target_vm_name}' for Pod {target_pod_number}"):
                return False, "create_target_vm_snapshot_cp", f"Failed creating snapshot on '{target_vm_name}'"

        # Maestro CD drive logic for the TARGET VM
        if "maestro" in component["component_name"].lower() and "maestro" in pod_config["course_name"].lower():
            logger.info(f"Configuring CD drive for Maestro component '{target_vm_name}'.")
            cd_drive_info = vm_mgr.get_cd_drive(target_vm_name)
            drive_name = "CD/DVD drive 1"
            iso_type = "Datastore ISO file"
            # Determine datastore based on target host_fqdn
            datastore_name = "datastore2-ho" if "hotshot" in host_fqdn_target.lower() else "keg2"
            iso_path = f"podiso/pod-{target_pod_number}-a.iso" # ISO specific to target pod
            if not vm_mgr.modify_cd_drive(target_vm_name, drive_name, iso_type, datastore_name, iso_path, connected=True):
                return False, "modify_cd_drive_maestro_cp", f"Failed modifying CD drive for Maestro VM '{target_vm_name}'"

    # STEP 5: Power on components for the TARGET pod
    # Ensure names used for power-on are the final target VM names
    vm_names_to_power_on = [
        component["clone_name"].replace("{X}", str(target_pod_number))
        for component in components_to_process if component.get("state") != "poweroff"
    ]
    if vm_names_to_power_on:
        with ThreadPoolExecutor(max_workers=thread) as executor:
            futures = {executor.submit(vm_mgr.poweron_vm, vm_name): vm_name for vm_name in vm_names_to_power_on}
            for future in tqdm(futures, desc=f"Pod {target_pod_number} → powering on", unit="vm"):
                vm_name_powered = futures[future]
                try:
                    if not future.result():
                        return False, "poweron_vm_cp", f"Failed powering on VM '{vm_name_powered}'"
                except Exception as e:
                    return False, "poweron_vm_exception_cp", f"Exception powering on VM '{vm_name_powered}': {e}"
    
    logger.info(f"Successfully built Checkpoint pod {target_pod_number}.")
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