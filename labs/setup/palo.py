from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from monitor.prtg import PRTGManager
from logger.log_config import setup_logger
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

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
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    
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
    group_name = f'pa-pod{pod_config["pod_number"]}'
    if not ResourcePoolManager(service_instance).create_resource_pool("pa", group_name, host_fqdn=pod_config["host_fqdn"]):
        return False, "create_resource_pool", f"Failed creating resource pool {group_name}"
    vmm.logger.info(f'Created resource pool {group_name}')
    
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
            if not vmm.create_linked_clone(component["base_vm"], component["clone_name"], "base", group_name):
                return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
        else:
            if not vmm.clone_vm(component["base_vm"], component["clone_name"], group_name):
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


def build_cortex_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):
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
        if rebuild:
            if not vm_manager.delete_vm(component["clone_name"]):
                return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"
            if not network_manager.delete_port_groups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups):
                return False, "delete_port_groups", f"Failed deleting port groups on {network['switch_name']}"
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
    
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    group_name = f'pa-pod{pod_config["pod_number"]}'

    rpm.poweroff_all_vms(group_name)
    rpm.logger.info(f'Power-off all VMs in {group_name}')
    rpm.delete_resource_pool(group_name)
    rpm.logger.info(f'Removed resource pool {group_name} and all its VMs.')

    for network in pod_config['networks']:
        solved_port_groups = solve_vlan_id(network["port_groups"])
        nm.delete_port_groups(pod_config['host_fqdn'], network["switch_name"], solved_port_groups)
        nm.logger.info(f'Deleted associated port groups from vswitch {network["switch_name"]}')


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

    # Default base IP for all palo courses.
    base_ip = "172.26.7.100" if host.lower() == "hotshot" else "172.30.7.100"

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
    server_data = collection.find_one({"vendor_shortcode": "pa"})
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