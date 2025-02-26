from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from monitor.prtg import PRTGManager
from logger.log_config import setup_logger

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
    for component in pod_config["components"]:
        if rebuild:
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                vm_manager.delete_vm(component["clone_name"])
            else:
                vm_manager.poweroff_vm(component["vm_name"])

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

        if not pod % 2:
            if "firewall" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
                continue
        else:
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                if not full:
                    vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
                    if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                        vm_manager.create_snapshot(component["base_vm"], "base", 
                                                description="Snapshot used for creating linked clones.")
                    vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], 
                                                "base", resource_pool)
                else:
                    vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)
            elif "firewall" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
            elif "panorama" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
            
            # Step-4: Update VM Network
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                vm_network = vm_manager.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict_1100(vm_network, int(pod))
                vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
                # Create a snapshot of all the cloned VMs to save base config.
                if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                    vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                                description=f"Snapshot of {component['clone_name']}")
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
            vm_manager.poweron_vm(component["clone_name"])
        else:
            vm_manager.poweron_vm(component["vm_name"])

def build_1100_210_pod(service_instance, host_details, pod_config, rebuild=False, full=False):
    vm_manager = VmManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    # Step-3: Clone VMs
    
    for component in pod_config["components"]:
        if rebuild:
            if "firewall" not in component["component_name"]:
                vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
                vm_manager.delete_vm(component["clone_name"])
            else:
                vm_manager.logger.info(f'Power-off VM {component["vm_name"]}')
                vm_manager.poweroff_vm(component["vm_name"])

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
                vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
                if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                    vm_manager.create_snapshot(component["base_vm"], "base", 
                                            description="Snapshot used for creating linked clones.")
                vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], 
                                               "base", resource_pool)
            else:
                vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)
        else:
            vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
        
        # Step-4: Update VM Network
        if "firewall" not in component["component_name"]:
            # Update VM networks and MAC address.
            vm_network = vm_manager.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict_1100(vm_network, int(pod))
            vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
            # Create a snapshot of all the cloned VMs to save base config.
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                            description=f"Snapshot of {component['clone_name']}")
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        if "firewall" not in component["component_name"]:
            vm_manager.poweron_vm(component["clone_name"])
        else:
            vm_manager.poweron_vm(component["vm_name"])

def build_1110_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):

    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]

    for network in pod_config['networks']:
        nm.create_vswitch(pod_config["host_fqdn"], network["switch_name"])
        solved_port_groups = solve_vlan_id(network["port_groups"])
        nm.create_vswitch_portgroups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups)
        nm.logger.info(f'Created portgoups on {network["switch_name"]}.')

    group_name = f'pa-pod{pod_config["pod_number"]}'
    rpm.create_resource_pool("pa", group_name, host_fqdn=pod_config["host_fqdn"])
    rpm.logger.info(f'Created resource pool {group_name}')

    for component in components_to_build:
        if rebuild:
            vmm.logger.info(f'Deleting VM {component["clone_name"]}.')
            vmm.delete_vm(component["clone_name"])
        if not full:
            vmm.logger.info(f'Cloning linked component {component["clone_name"]}.')
            vmm.create_linked_clone(component["base_vm"], component["clone_name"], 
                                    "base", group_name)
        else:
            vmm.clone_vm(component["base_vm"], component["clone_name"], group_name)
        
        # Update VM networks and MAC address.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_1110(vm_network, int(pod))
        vmm.update_vm_network(component["clone_name"], updated_vm_network)
        if "firewall" in component["component_name"]:
                vmm.download_vmx_file(component["clone_name"],f"/tmp/{component['clone_name']}.vmx")
                vmm.update_vm_uuid(f"/tmp/{component['clone_name']}.vmx", component["uuid"])
                vmm.upload_vmx_file(component["clone_name"],f"/tmp/{component['clone_name']}.vmx")
                vmm.verify_uuid(component["clone_name"], component["uuid"])
        # Create a snapshot of all the cloned VMs to save base config.
        if not vmm.snapshot_exists(component["clone_name"], snapshot_name):
            vmm.create_snapshot(component["clone_name"], snapshot_name, 
                                description=f"Snapshot of {component['clone_name']}")
        # Step-5: Poweron VMs
    for component in components_to_build:
        vmm.poweron_vm(component["clone_name"])
    vmm.logger.info('Power-on all VMs.')

def build_cortex_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    # Step-2: Create Network
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]
    if rebuild:
        for component in components_to_build:
            vm_manager.delete_vm(component["clone_name"])
        for network in pod_config["networks"]:
            solved_port_groups = solve_vlan_id(network["port_groups"])
            network_manager.delete_port_groups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups)

    for network in pod_config["networks"]:
        if not rebuild:
            solved_port_groups = solve_vlan_id(network["port_groups"])
        network_manager.create_vswitch_portgroups(pod_config["host_fqdn"],
                                                network["switch_name"],
                                                solved_port_groups)
        
    # Step-3: Clone VMs
    for component in components_to_build:
        if pod_config["host_fqdn"].split(".")[0] == "cliffjumper":
            resource_pool = component["component_name"] + "-cl"
        elif pod_config["host_fqdn"].split(".")[0] == "apollo":
            resource_pool = component["component_name"] + "-ap"
        elif pod_config["host_fqdn"].split(".")[0] == "nightbird":
            resource_pool = component["component_name"] + "-ni"
        elif pod_config["host_fqdn"].split(".")[0] == "ultramagnus":
            resource_pool = component["component_name"] + "-ul"
        elif pod_config["host_fqdn"].split(".")[0] == "unicron":
            resource_pool = component["component_name"] + "-un"
        else:
            resource_pool = component["component_name"]
        if not full:
            vm_manager.create_linked_clone(component["base_vm"], component["clone_name"],
                                            "base", resource_pool)
        else:
            vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)

        # Step-4: Update VM Network
        vm_network = vm_manager.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_cortex(vm_network, pod)
        vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
        vm_manager.connect_networks_to_vm(component["clone_name"], updated_vm_network)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
            vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                        description=f"Snapshot of {component['clone_name']}")
    
    # Step-5: Poweron VMs
    for component in components_to_build:
        if component.get("state") != "poweroff":
            vm_manager.poweron_vm(component["clone_name"])


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