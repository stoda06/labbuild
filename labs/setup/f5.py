from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager
from managers.resource_pool_manager import ResourcePoolManager
from pyVmomi import vim
from monitor.prtg import PRTGManager
from logger.log_config import setup_logger

def update_network_dict(vm_name, network_dict, class_number, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')  # This ensures zero-padded two-digit hex
        return ':'.join(mac_parts)

    class_vs = f"vs{class_number}"

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']
        connected_at_power_on = details['connected_at_power_on']

        if 'rdp' in network_name and 'apm' not in vm_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        if 'mgt' in network_name and 'srv-bigip' not in vm_name:
            mac_address = replace_mac_octet(mac_address, pod_number)
        
        if 'vs0' in network_name:
            network_name = network_name.replace('vs0', class_vs)
        
        if 'bigip' in vm_name and not 'srv-bigip' in vm_name and ('ext' in network_name or 'int' in network_name):
            mac_address = replace_mac_octet(mac_address, pod_number)

        if 'w10' in vm_name and 'ext' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        if 'li' in vm_name and 'int' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address,
            'connected_at_power_on': connected_at_power_on
        }

    return updated_network_dict
            

def build_class(service_instance, class_config, rebuild=False, full=False, selected_components=None):
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    vmm = VmManager(service_instance)

    # STEP 1: Setup networks.
    for network in class_config["networks"]:
        switch_name = network['switch']
        if not nm.create_vswitch(class_config["host_fqdn"], switch_name):
            return False, "create_vswitch", f"Failed creating vswitch {switch_name} on host {class_config['host_fqdn']}"
        nm.logger.info(f"Created vswitch {switch_name}.")
        if not nm.create_vswitch_portgroups(class_config["host_fqdn"], network["switch"], network["port_groups"]):
            return False, "create_vswitch_portgroups", f"Failed creating portgroups on vswitch {switch_name}"
        nm.logger.info(f"Created portgroups on vswitch {switch_name}.")

    # STEP 2: Create class resource pool.
    class_pool = f'f5-class{class_config["class_number"]}'
    if not rpm.create_resource_pool(f'f5-{class_config["host_fqdn"][0:2]}', class_pool):
        return False, "create_resource_pool", f"Failed creating class resource pool {class_pool}"
    rpm.logger.info(f'Created class resource pool {class_pool}.')

    # STEP 3: Process each group.
    for group in class_config["groups"]:
        group_pool = f'{class_pool}-{group["group_name"]}'
        if not rpm.create_resource_pool(class_pool, group_pool):
            return False, "create_resource_pool", f"Failed creating group resource pool {group_pool}"
        rpm.logger.info(f'Created group resource pool {group_pool}.')

        # Process only groups with "srv" in group_pool.
        if "srv" in group_pool:
            components_to_clone = group["component"]
            if selected_components:
                components_to_clone = [
                    component for component in group["component"]
                    if component["component_name"] in selected_components
                ]

            for component in components_to_clone:
                clone_name = component["clone_vm"]
                # STEP 3a: Rebuild deletion.
                if rebuild:
                    if not vmm.delete_vm(clone_name):
                        return False, "delete_vm", f"Failed deleting VM {clone_name}"
                    vmm.logger.info(f"Deleted VM {clone_name}.")

                # STEP 3b: Clone operation.
                if not full:
                    if not vmm.snapshot_exists(component["base_vm"], "base"):
                        if not vmm.create_snapshot(component["base_vm"], "base", 
                                                   description="Snapshot used for creating linked clones."):
                            return False, "create_snapshot", f"Failed creating snapshot on base VM {component['base_vm']}"
                    if not vmm.create_linked_clone(component["base_vm"], clone_name, "base", group_pool):
                        return False, "create_linked_clone", f"Failed creating linked clone {clone_name}"
                    vmm.logger.info(f'Created linked clone {clone_name}.')
                else:
                    if not vmm.clone_vm(component["base_vm"], clone_name, group_pool):
                        return False, "clone_vm", f"Failed cloning VM {clone_name}"
                    vmm.logger.info(f'Created direct clone {clone_name}.')

                # STEP 3c: Update VM network.
                vm_network = vmm.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict(clone_name, vm_network, int(class_config["class_number"]), int(class_config["class_number"]))
                if not vmm.update_vm_network(clone_name, updated_vm_network):
                    return False, "update_vm_network", f"Failed updating network for {clone_name}"
                if not vmm.connect_networks_to_vm(clone_name, updated_vm_network):
                    return False, "connect_networks_to_vm", f"Failed connecting networks for {clone_name}"
                vmm.logger.info(f'Updated VM {clone_name} networks.')

                # STEP 3d: Update serial port if applicable.
                if "bigip" in component["clone_vm"]:
                    if not vmm.update_serial_port_pipe_name(clone_name, "Serial port 1", r"\\.\pipe\com_" + str(class_config["class_number"])):
                        return False, "update_serial_port_pipe", f"Failed updating serial port pipe on {clone_name}"
                    vmm.logger.info(f'Updated serial port pipe name on {clone_name}.')

                # STEP 3e: Create snapshot on the cloned VM.
                if not vmm.snapshot_exists(clone_name, "base"):
                    if not vmm.create_snapshot(clone_name, "base", description=f"Snapshot of {clone_name}"):
                        return False, "create_snapshot", f"Failed creating snapshot on {clone_name}"

                # STEP 3f: Power on VM if needed.
                if component.get("state") != "poweroff":
                    if not vmm.poweron_vm(component["clone_vm"]):
                        return False, "poweron_vm", f"Failed powering on {component['clone_vm']}"

    return True, None, None


def build_pod(service_instance, pod_config, mem=None, rebuild=False, full=False, selected_components=None):
    vmm = VmManager(service_instance)
    snapshot_name = 'base'

    # Process each group in the pod configuration.
    for group in pod_config["groups"]:
        pod_number = pod_config["pod_number"]
        class_number = pod_config["class_number"]
        pod_pool = f'f5-class{class_number}-{group["group_name"]}'

        # Only process groups that do NOT contain "srv" in the group name.
        if "srv" not in group["group_name"]:
            components_to_clone = group["component"]
            if selected_components:
                components_to_clone = [
                    component for component in group["component"]
                    if component["component_name"] in selected_components
                ]

            # STEP 3.1: Clone components.
            for component in components_to_clone:
                clone_name = component["clone_vm"]
                if rebuild:
                    if not vmm.delete_vm(clone_name):
                        return False, "delete_vm", f"Failed deleting VM {clone_name}"
                    vmm.logger.info(f'Deleted VM {clone_name}.')

                if not full:
                    if not vmm.snapshot_exists(component["base_vm"], "base"):
                        if not vmm.create_snapshot(component["base_vm"], "base", 
                                                   description="Snapshot used for creating linked clones."):
                            return False, "create_snapshot", f"Failed creating snapshot on base VM {component['base_vm']}"
                    if not vmm.create_linked_clone(component["base_vm"], clone_name, "base", pod_pool):
                        return False, "create_linked_clone", f"Failed creating linked clone {clone_name}"
                    vmm.logger.info(f'Created linked clone {clone_name}.')
                else:
                    if not vmm.clone_vm(component["base_vm"], clone_name, pod_pool):
                        return False, "clone_vm", f"Failed cloning VM {clone_name}"
                    vmm.logger.info(f'Created direct clone {clone_name}.')

                # STEP 3.2: Update VM network.
                vm_network = vmm.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict(clone_name, vm_network, int(class_number), int(pod_number))
                if not vmm.update_vm_network(clone_name, updated_vm_network):
                    return False, "update_vm_network", f"Failed updating network for {clone_name}"
                vmm.logger.info(f'Updated VM {clone_name} networks.')

                # STEP 3.3: Update serial port and modify CD drive if applicable.
                if "bigip" in clone_name or "w10" in clone_name:
                    if not vmm.update_serial_port_pipe_name(clone_name, "Serial port 1", r"\\.\pipe\com_" + str(pod_number)):
                        return False, "update_serial_port_pipe", f"Failed updating serial port pipe on {clone_name}"
                    vmm.logger.info(f'Updated serial port pipe name on {clone_name}.')
                    if "w10" in clone_name:
                        drive_name = "CD/DVD drive 1"
                        iso_type = "Datastore ISO file"
                        # Check for datastore object; assume vmm.get_obj returns a truthy value if found.
                        if vmm.get_obj([vim.Datastore], "keg2"):
                            datastore_name = "keg2" 
                        else:
                            datastore_name = "datastore2-ho"
                        iso_path = "podiso/pod-" + pod_number + "-a.iso"
                        if not vmm.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True):
                            return False, "modify_cd_drive", f"Failed modifying CD drive for {clone_name}"
                # STEP 3.4: Reconfigure VM resources and update VM UUID if needed.
                if 'bigip' in clone_name:
                    if mem:
                        if not vmm.reconfigure_vm_resources(clone_name, new_memory_size_mb=mem):
                            return False, "reconfigure_vm_resources", f"Failed reconfiguring memory for {clone_name}"
                        vmm.logger.info(f'Updated {clone_name} with memory {mem} MB.')
                    hex_pod_number = format(int(pod_number), '02x')
                    uuid = component["uuid"].replace('XX', str(hex_pod_number))
                    if not vmm.download_vmx_file(clone_name, f"/tmp/{clone_name}.vmx"):
                        return False, "download_vmx_file", f"Failed downloading vmx file for {clone_name}"
                    if not vmm.update_vm_uuid(f"/tmp/{clone_name}.vmx", uuid):
                        return False, "update_vm_uuid", f"Failed updating UUID for {clone_name}"
                    if not vmm.upload_vmx_file(clone_name, f"/tmp/{clone_name}.vmx"):
                        return False, "upload_vmx_file", f"Failed uploading vmx file for {clone_name}"
                    if not vmm.verify_uuid(clone_name, uuid):
                        return False, "verify_uuid", f"UUID verification failed for {clone_name}"

                # STEP 3.5: Create snapshot on the cloned VM.
                if not vmm.snapshot_exists(clone_name, snapshot_name):
                    if not vmm.create_snapshot(clone_name, snapshot_name, description=f"Snapshot of {clone_name}"):
                        return False, "create_snapshot", f"Failed creating snapshot on {clone_name}"

                # STEP 3.6: Power on the VM if not marked as poweroff.
                if component.get("state") != "poweroff":
                    if not vmm.poweron_vm(component["clone_vm"]):
                        return False, "poweron_vm", f"Failed powering on {component['clone_vm']}"

    return True, None, None


def teardown_class(service_instance, course_config):

    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    
    class_name = course_config["class_name"]

    rpm.poweroff_all_vms(class_name)
    rpm.logger.info(f'Power-off all VMs in {class_name}')

    if rpm.delete_resource_pool(class_name):
        rpm.logger.info(f'Deleted {class_name} successfully.')
    else: 
        rpm.logger.error(f'Failed to delete {class_name}.')

    for network in course_config["networks"]:
        switch_name = network['switch']
        if nm.delete_vswitch(course_config["host_fqdn"], switch_name):
            nm.logger.info(f'Deleted switch {switch_name} successfully.')
        else:
            nm.logger.error(f'Failed to delete switch {switch_name}.')


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
        class_number = int(pod_config.get("class_number"))
    except (TypeError, ValueError):
        logger.error("Invalid or missing class number in pod_config.")
        return None

    # Determine the base IP address based on the course name.
    base_ip = "172.26.2.200" if host.lower() == "hotshot" else "172.30.2.200"

    # Split the base IP into its constituent parts.
    base_ip_parts = base_ip.split('.')
    try:
        # Convert the last octet of the base IP to an integer.
        base_last_octet = int(base_ip_parts[3])
    except ValueError:
        logger.error("Invalid base IP: %s", base_ip)
        return None

    # Compute the new last octet by adding the pod number.
    new_last_octet = base_last_octet + class_number
    if new_last_octet > 255:
        logger.error("Resulting IP's last octet (%s) exceeds 255.", new_last_octet)
        return None

    # Reconstruct the new IP address using the first three octets and the new last octet.
    new_ip = ".".join(base_ip_parts[:3] + [str(new_last_octet)])
    logger.debug("Computed new IP for PRTG monitor: %s", new_ip)

    # Access the PRTG configuration from the "labbuild_db" database.
    db = db_client["labbuild_db"]
    collection = db["prtg"]
    server_data = collection.find_one({"vendor_shortcode": "ot"})
    if not server_data or "servers" not in server_data:
        logger.error("No PRTG server configuration found for vendor 'ot'.")
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