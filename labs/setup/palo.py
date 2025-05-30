from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from monitor.prtg import PRTGManager
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

import logging
logger = logging.getLogger(__name__) # Or logging.getLogger('VmManager')

def update_network_dict_1110(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to a two-digit hexadecimal string

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = details['network_name'].replace('pa-', 'pan-')
            if 'pan-internal-1' in details['network_name']:
                details['network_name'] = details['network_name'].replace('1', str(pod_number))
            else:
                details['network_name'] = details['network_name'].replace('0', str(pod_number))

    return network_dict

def update_network_dict_1100(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to a two-digit hexadecimal string

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = details['network_name'].replace('1', str(pod_number))

    return network_dict

def update_network_dict_cortex(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to hex with at least two digits

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = f"pa-internal-cortex-{pod_number}"

    return network_dict

def solve_vlan_id(port_groups):
    for group in port_groups:
        # Evaluate the expression in the vlan_id and update it as an integer
        group["vlan_id"] = eval(group["vlan_id"])
    return port_groups


def build_1100_220_pod(service_instance, host_details, pod_config, rebuild=False, full=False):
    vm_manager = VmManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"

    # STEP 1: Pre-clone actions for each component.
    for component in tqdm(pod_config["components"], desc=f"Pod {pod} → Pre-clone actions", unit="comp"):
        if rebuild:
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
                if not vm_manager.delete_vm(component["clone_name"]):
                    return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"
            else:
                vm_manager.logger.info(f'Powering off VM {component["vm_name"]}')
                if not vm_manager.poweroff_vm(component["vm_name"]):
                    return False, "poweroff_vm", f"Failed powering off VM {component['vm_name']}"
        
        # Determine resource pool based on host name.
        if host_details.name == "cliffjumper":
            resource_pool = component["group_name"] + "-cl"
        elif host_details.name == "apollo":
            resource_pool = component["group_name"] + "-ap"
        elif host_details.name == "nightbird":
            resource_pool = component["group_name"] + "-ni"
        elif host_details.name == "ultramagnus":
            resource_pool = component["group_name"] + "-ul"
        else:
            resource_pool = component["group_name"]

        # STEP 2: Clone or revert snapshot.
        if not pod % 2:
            if "firewall" in component["component_name"]:
                vm_manager.logger.info(f"Reverting {component['vm_name']} to snapshot {component['snapshot']}")
                if not vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"]):
                    return False, "revert_to_snapshot", f"Failed reverting snapshot on {component['vm_name']}"
                continue
        else:
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                if not full:
                    vm_manager.logger.info(f'Creating linked clone for {component["clone_name"]}.')
                    if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                        if not vm_manager.create_snapshot(component["base_vm"], "base", 
                                                          description="Snapshot used for creating linked clones."):
                            return False, "create_snapshot", f"Failed creating snapshot on {component['base_vm']}"
                    if not vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], 
                                                          "base", resource_pool):
                        return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
                else:
                    if not vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool):
                        return False, "clone_vm", f"Failed cloning VM for {component['clone_name']}"
            elif "firewall" in component["component_name"]:
                if not vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"]):
                    return False, "revert_to_snapshot", f"Failed reverting snapshot on {component['vm_name']}"
            elif "panorama" in component["component_name"]:
                if not vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"]):
                    return False, "revert_to_snapshot", f"Failed reverting snapshot on {component['vm_name']}"

        # STEP 3: Update VM network and create snapshot if needed.
        if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
            vm_network = vm_manager.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict_1100(vm_network, pod)
            if not vm_manager.update_vm_network(component["clone_name"], updated_vm_network):
                return False, "update_vm_network", f"Failed updating network for {component['clone_name']}"
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                if not vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                                  description=f"Snapshot of {component['clone_name']}"):
                    return False, "create_snapshot", f"Failed creating snapshot on {component['clone_name']}"

    # STEP 4: Power on VMs in parallel.
    to_power = []
    for component in pod_config["components"]:
        if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
            to_power.append(component["clone_name"])
        else:
            to_power.append(component["vm_name"])
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(vm_manager.poweron_vm, name): name for name in to_power}
        for future in tqdm(futures, desc=f"Pod {pod} → Powering on", unit="vm"):
            if not future.result():
                failed_vm = futures[future]
                vm_manager.logger.error(f"Failed powering on {failed_vm}")
                return False, "poweron_vm", f"Failed powering on {failed_vm}"
    return True, None, None


def build_1100_210_pod(service_instance, host_details, pod_config, rebuild=False, full=False):
    vm_manager = VmManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    
    # STEP 1: Clone or revert each component.
    for component in tqdm(pod_config["components"], desc=f"Pod {pod} → Clone/Revert", unit="comp"):
        if rebuild:
            if "firewall" not in component["component_name"]:
                vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
                if not vm_manager.delete_vm(component["clone_name"]):
                    return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"
            else:
                vm_manager.logger.info(f'Powering off VM {component["vm_name"]}')
                if not vm_manager.poweroff_vm(component["vm_name"]):
                    return False, "poweroff_vm", f"Failed powering off VM {component['vm_name']}"
        
        # Determine resource pool based on host name.
        if host_details.name == "cliffjumper":
            resource_pool = component["group_name"] + "-cl"
        elif host_details.name == "apollo":
            resource_pool = component["group_name"] + "-ap"
        elif host_details.name == "nightbird":
            resource_pool = component["group_name"] + "-ni"
        elif host_details.name == "ultramagnus":
            resource_pool = component["group_name"] + "-ul"
        else:
            resource_pool = component["group_name"]

        if "firewall" not in component["component_name"]:
            if not full:
                vm_manager.logger.info(f'Creating linked clone for {component["clone_name"]}.')
                if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                    if not vm_manager.create_snapshot(component["base_vm"], "base", 
                                                      description="Snapshot used for creating linked clones."):
                        return False, "create_snapshot", f"Failed creating snapshot on {component['base_vm']}"
                if not vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], 
                                                      "base", resource_pool):
                    return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
            else:
                if not vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool):
                    return False, "clone_vm", f"Failed cloning VM for {component['clone_name']}"
        else:
            if not vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"]):
                return False, "revert_to_snapshot", f"Failed reverting snapshot on {component['vm_name']}"
        
        # STEP 2: Update network and create snapshot if needed.
        if "firewall" not in component["component_name"]:
            vm_network = vm_manager.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict_1100(vm_network, pod)
            if not vm_manager.update_vm_network(component["clone_name"], updated_vm_network):
                return False, "update_vm_network", f"Failed updating network for {component['clone_name']}"
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                if not vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                                  description=f"Snapshot of {component['clone_name']}"):
                    return False, "create_snapshot", f"Failed creating snapshot on {component['clone_name']}"

    # STEP 3: Power on VMs in parallel.
    to_power = []
    for component in pod_config["components"]:
        if "firewall" not in component["component_name"]:
            to_power.append(component["clone_name"])
        else:
            to_power.append(component["vm_name"])
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(vm_manager.poweron_vm, name): name for name in to_power}
        for future in tqdm(futures, desc=f"Pod {pod} → Powering on", unit="vm"):
            if not future.result():
                failed_vm = futures[future]
                vm_manager.logger.error(f"Failed powering on {failed_vm}")
                return False, "poweron_vm", f"Failed powering on {failed_vm}"
    return True, None, None


def build_1110_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):
    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    folder_mgr = FolderManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    target_folder_name = f'pa-pod{pod}-folder'
    
    
    # Optionally filter components.
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]
    
    # STEP 1: Create networks.
    for network in tqdm(pod_config['networks'], desc=f"Pod {pod} → Setting up networks", unit="net"):
        solved_port_groups = solve_vlan_id(network["port_groups"])
        # If network creation fails, return immediately.
        if not nm.create_vswitch_portgroups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups):
            return False, "create_vswitch_portgroups", f"Failed creating port groups on {network['switch_name']}"
    
    # STEP 2: Create resource pool.
    if "cliffjumper" in pod_config["host_fqdn"]:
        parent_resource_pool = "pa-cl"
    elif "unicron" in pod_config["host_fqdn"]:
        parent_resource_pool = "pa-un"
    elif "nightbird" in pod_config["host_fqdn"]:
        parent_resource_pool = "pa-ni"
    elif "ultramagnus" in pod_config["host_fqdn"]:
        parent_resource_pool = "pa-ul"
    elif "hotshot" in pod_config["host_fqdn"]:
        parent_resource_pool = "pa"
        
    group_name = f'pa-pod{pod_config["pod_number"]}'
    if not ResourcePoolManager(service_instance).create_resource_pool(parent_resource_pool, group_name, host_fqdn=pod_config["host_fqdn"]):
        return False, "create_resource_pool", f"Failed creating resource pool {group_name}"
    vmm.logger.info(f'Created resource pool {group_name}')

    logger.info(f"Ensuring folder '{target_folder_name}' for pod {pod}.")
    if not folder_mgr.create_folder(pod_config["vendor_shortcode"], target_folder_name): # Parent folder is vendor code
        return False, "create_folder", f"Failed creating folder '{target_folder_name}'"
    # if not folder_mgr.assign_user_to_folder(target_folder_name, domain_user, role_name):
    #     return False, "assign_role_to_folder", f"Failed assigning role to '{target_folder_name}'"
    
    # STEP 3: Clone/configure each component.
    for component in tqdm(components_to_build, desc=f"Pod {pod} → Cloning/Configuring", unit="vm"):
        if rebuild:
            vmm.logger.info(f'Deleting VM {component["clone_name"]}')
            if not vmm.delete_vm(component["clone_name"]):
                return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"
        if not full:
            if not vmm.snapshot_exists(component["base_vm"], "base") and not vmm.create_snapshot(component["base_vm"], "base", "Base snapshot"):
                return False, "create_snapshot", f"Failed creating snapshot on {component['base_vm']}"
            vmm.logger.info(f'Creating linked clone for {component["clone_name"]}.')
            if not vmm.create_linked_clone(component["base_vm"], component["clone_name"], "base", 
                                           group_name, directory_name=target_folder_name):
                return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
        else:
            if not vmm.clone_vm(component["base_vm"], component["clone_name"], 
                                group_name, directory_name=target_folder_name):
                return False, "clone_vm", f"Failed cloning VM for {component['clone_name']}"
        
        # Update VM network settings.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_1110(vm_network, pod)
        if not vmm.update_vm_network(component["clone_name"], updated_vm_network):
            return False, "update_vm_network", f"Failed updating network for {component['clone_name']}"
        
        if "firewall" in component["component_name"]:
            if not vmm.download_vmx_file(component["clone_name"], f"/tmp/{component['clone_name']}.vmx"):
                return False, "download_vmx_file", f"Failed downloading vmx file for {component['clone_name']}"
            if not vmm.update_vm_uuid(f"/tmp/{component['clone_name']}.vmx", component["uuid"]):
                return False, "update_vm_uuid", f"Failed updating uuid for {component['clone_name']}"
            if not vmm.upload_vmx_file(component["clone_name"], f"/tmp/{component['clone_name']}.vmx"):
                return False, "upload_vmx_file", f"Failed uploading vmx file for {component['clone_name']}"
            if not vmm.verify_uuid(component["clone_name"], component["uuid"]):
                return False, "verify_uuid", f"UUID verification failed for {component['clone_name']}"
        
        if not vmm.snapshot_exists(component["clone_name"], snapshot_name):
            if not vmm.create_snapshot(component["clone_name"], snapshot_name, description=f"Snapshot of {component['clone_name']}"):
                return False, "create_snapshot", f"Failed creating snapshot for {component['clone_name']}"
    
    # STEP 4: Power on VMs in parallel.
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(vmm.poweron_vm, comp["clone_name"]): comp["clone_name"] for comp in components_to_build}
        for future in tqdm(futures, desc=f"Pod {pod} → Powering on", unit="vm"):
            if not future.result():
                failed_vm = futures[future]
                vmm.logger.error(f"Failed powering on {failed_vm}")
                return False, "poweron_vm", f"Failed powering on {failed_vm}"
    vmm.logger.info('Power-on all VMs.')
    return True, None, None


def build_cortex_pod(service_instance, host_details, pod_config, rebuild=False, full=False, selected_components=None):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    
    # Optionally filter components.
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]
    
    # STEP 1: Network setup.
    for network in tqdm(pod_config["networks"], desc=f"Pod {pod} → Network setup", unit="net"):
        solved_port_groups = solve_vlan_id(network["port_groups"])
        if not network_manager.create_vswitch_portgroups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups):
            return False, "create_vswitch_portgroups", f"Failed creating port groups on {network['switch_name']}"
    
    # STEP 2: Clone/configure each component.
    for component in tqdm(components_to_build, desc=f"Pod {pod} → Cloning/Configuring", unit="vm"):
        host_prefix = pod_config["host_fqdn"].split(".")[0]
        if host_prefix == "cliffjumper":
            resource_pool = component["component_name"] + "-cl"
        elif host_prefix == "apollo":
            resource_pool = component["component_name"] + "-ap"
        elif host_prefix == "nightbird":
            resource_pool = component["component_name"] + "-ni"
        elif host_prefix == "ultramagnus":
            resource_pool = component["component_name"] + "-ul"
        elif host_prefix == "unicron":
            resource_pool = component["component_name"] + "-un"
        else:
            resource_pool = component["component_name"]

        if rebuild:
            if not vm_manager.delete_vm(component["clone_name"]):
                return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"
        
        if not full:
            if not vm_manager.snapshot_exists(component["base_vm"], "base") and not vm_manager.create_snapshot(component["base_vm"], "base", "Base snapshot"):
                return False, "create_snapshot", f"Failed creating snapshot on {component['base_vm']}"
            if not vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], "base", resource_pool):
                return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
        else:
            if not vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool):
                return False, "clone_vm", f"Failed cloning VM for {component['clone_name']}"
        
        # STEP 3: Update VM network, connect networks and create snapshot.
        vm_network = vm_manager.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_cortex(vm_network, pod)
        if not vm_manager.update_vm_network(component["clone_name"], updated_vm_network):
            return False, "update_vm_network", f"Failed updating network for {component['clone_name']}"
        if not vm_manager.connect_networks_to_vm(component["clone_name"], updated_vm_network):
            return False, "connect_networks", f"Failed connecting networks for {component['clone_name']}"
        if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
            if not vm_manager.create_snapshot(component["clone_name"], snapshot_name, description=f"Snapshot of {component['clone_name']}"):
                return False, "create_snapshot", f"Failed creating snapshot for {component['clone_name']}"
    
    # STEP 4: Power on VMs in parallel (if not marked as "poweroff").
    to_power = []
    for component in components_to_build:
        if component.get("state") != "poweroff":
            to_power.append(component["clone_name"])
    
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(vm_manager.poweron_vm, name): name for name in to_power}
        for future in tqdm(futures, desc=f"Pod {pod} → Powering on", unit="vm"):
            if not future.result():
                failed_vm = futures[future]
                vm_manager.logger.error(f"Failed powering on {failed_vm}")
                return False, "poweron_vm", f"Failed powering on {failed_vm}"
    return True, None, None



def teardown_cortex(service_instance, pod_config):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)

    for component in pod_config["components"]:
        vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
        vm_manager.delete_vm(component["clone_name"])

    for network in pod_config["networks"]:
        network_manager.logger.info(f'Deleting port-groups from {network["switch_name"]} vswitch.')
        solved_port_groups = solve_vlan_id(network["port_groups"])
        network_manager.delete_port_groups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups)

def teardown_1100(service_instance, pod_config):
    vm_manager = VmManager(service_instance)

    for component in pod_config["components"]:
        if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
            vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
            vm_manager.delete_vm(component["clone_name"])
        else:
            vm_manager.logger.info(f'Power-off VM {component["vm_name"]}')
            vm_manager.poweroff_vm(component["vm_name"])

def teardown_1110(service_instance, pod_config):
    
    vm_manager = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    group_name = f'pa-pod{pod_config["pod_number"]}'
    folder_name = f'pa-pod{pod_config["pod_number"]}-folder'

    vm_manager.delete_folder(folder_name, force=True)
    rpm.delete_resource_pool(group_name)
    rpm.logger.info(f'Removed resource pool {group_name} and all its VMs.')

    for network in pod_config['networks']:
        solved_port_groups = solve_vlan_id(network["port_groups"])
        nm.delete_port_groups(pod_config['host_fqdn'], network["switch_name"], solved_port_groups)
        nm.logger.info(f'Deleted associated port groups from vswitch {network["switch_name"]}')


def add_monitor(pod_config, db_client, prtg_server=None):
    """
    Adds or updates a PRTG monitor for a Palo Alto pod.

    Ensures that only one monitor with the target name exists across all configured
    Palo Alto PRTG servers by first searching and deleting any duplicates found,
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
        # Palo Alto uses the VR name, which often includes the pod number
        # Construct the expected name based on the pattern in config
        monitor_name_pattern = prtg_details.get("name") # e.g., "11.1-vr-{X}"
        container_id = prtg_details.get("container")
        template_id = prtg_details.get("object")

        if not all([monitor_name_pattern, container_id, template_id]):
            logger.error("Missing required PRTG config (name pattern, container, object) in pod_config.")
            return None

        # Construct the specific monitor name for this pod
        monitor_name = monitor_name_pattern.replace("{X}", str(pod_number))

        # Determine base IP based on host (example logic, adjust if needed)
        host_short = pod_config.get("host_fqdn", "").split(".")[0]
        # Default base IP for all palo courses (adjust if needed)
        base_ip = "172.26.7.100" if host_short.lower() == "hotshot" else "172.30.7.100"

        parts = base_ip.split('.')
        base_last_octet = int(parts[3])
        new_last_octet = base_last_octet + pod_number # Palo might be direct addition? Verify this logic.
        # If direct addition: new_last_octet = base_last_octet + pod_number

        if new_last_octet > 255:
            logger.error(f"Computed IP last octet {new_last_octet} exceeds 255 for pod {pod_number}.")
            return None
        new_ip = ".".join(parts[:3] + [str(new_last_octet)])
        logger.debug(f"Target IP for monitor '{monitor_name}': {new_ip}")

    except (TypeError, ValueError, KeyError) as e:
        logger.error(f"Error processing pod_config for PRTG details: {e}", exc_info=True)
        return None

    # --- 2. Get Configured Palo Alto PRTG Servers ---
    try:
        db = db_client["labbuild_db"]
        prtg_conf = db["prtg"].find_one({"vendor_shortcode": "pa"}) # Query for 'pa' vendor
        if not prtg_conf or not prtg_conf.get("servers"):
            logger.error("No PRTG server configuration found for vendor 'pa' in database.")
            return None
        all_pa_servers = prtg_conf["servers"]
    except Exception as e:
        logger.error(f"Failed to retrieve PRTG server configuration from DB: {e}", exc_info=True)
        return None

    # --- 3. Search All Servers and Delete Existing Monitors ---
    logger.info(f"Searching for existing monitor '{monitor_name}' on all PA PRTG servers...")
    deleted_count = 0
    for server in all_pa_servers:
        server_url = server.get("url")
        api_token = server.get("apitoken")
        server_name = server.get("name", server_url)
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
            # else: logger.debug(f"Monitor '{monitor_name}' not found on server {server_name}.")
        except Exception as e:
            logger.error(f"Error checking/deleting monitor on server {server_name}: {e}", exc_info=True)

    if deleted_count > 0:
        logger.info(f"Finished deleting {deleted_count} pre-existing monitor(s) named '{monitor_name}'.")

    # --- 4. Select Target Server for Creation ---
    target_server_info = None
    if prtg_server: # Specific server requested
        target_server_info = next((s for s in all_pa_servers if s.get("name") == prtg_server), None)
        if not target_server_info:
            logger.error(f"Specified target PRTG server '{prtg_server}' not found in 'pa' configuration.")
            return None
        logger.info(f"Target server specified: {prtg_server}")
    else: # No specific server requested, find first available based on capacity
        logger.info("No target server specified, finding first available based on capacity...")
        for server in all_pa_servers:
            server_url = server.get("url")
            api_token = server.get("apitoken")
            server_name = server.get("name", server_url)
            if not server_url or not api_token: continue

            try:
                prtg_mgr = PRTGManager(server_url, api_token)
                current_sensor_count = prtg_mgr.get_up_sensor_count()
                template_sensor_count = prtg_mgr.get_template_sensor_count(template_id)

                if (current_sensor_count + template_sensor_count) < 499: # Example limit
                    logger.info(f"Selected server {server_name} (Sensors: {current_sensor_count}+{template_sensor_count} < 499)")
                    target_server_info = server
                    break # Found a suitable server
                else:
                    logger.warning(f"Server {server_name} skipped: capacity limit ({current_sensor_count}+{template_sensor_count} >= 499).")
            except Exception as e:
                logger.error(f"Error checking capacity on server {server_name}: {e}")

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

        # Optional: Double-check capacity again
        current_count = prtg_target_mgr.get_up_sensor_count()
        template_count = prtg_target_mgr.get_template_sensor_count(template_id)
        if (current_count + template_count) >= 499:
            logger.error(f"Target server {target_name} capacity check failed just before creation.")
            return None

        new_device_id = prtg_target_mgr.clone_device(template_id, container_id, monitor_name)
        if not new_device_id:
            logger.error(f"Failed to clone device '{monitor_name}' on {target_name}.")
            return None

        logger.info(f"Cloned device ID {new_device_id} on {target_name}. Setting IP...")
        if not prtg_target_mgr.set_device_ip(new_device_id, new_ip):
            logger.error(f"Failed to set IP '{new_ip}' for device ID {new_device_id} on {target_name}.")
            return None

        logger.info(f"Set IP for device ID {new_device_id}. Enabling...")
        if not prtg_target_mgr.enable_device(new_device_id):
            logger.error(f"Failed to enable monitor ID {new_device_id} on {target_name}.")
            return None

        monitor_url = f"{target_url}/device.htm?id={new_device_id}"
        logger.info(f"Successfully created and enabled PRTG monitor: {monitor_url}")
        return monitor_url

    except Exception as e:
        logger.error(f"Error during monitor creation on target server {target_name}: {e}", exc_info=True)
        return None