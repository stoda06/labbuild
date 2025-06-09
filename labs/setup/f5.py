from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager
from managers.resource_pool_manager import ResourcePoolManager
from pyVmomi import vim
from monitor.prtg import PRTGManager
from typing import Dict, Any, Optional, List # Added List

import logging
logger = logging.getLogger(__name__) # Or logging.getLogger('VmManager')

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
                        iso_path = f"podiso/pod-{pod_number}-a.iso"
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


def add_monitor(
    entity_config: Dict[str, Any], # Can be class_config or pod_config
    db_client: Any, # pymongo.MongoClient instance
    prtg_server_preference: Optional[str] = None
) -> Optional[str]: # Returns URL of the first successfully added monitor or None
    """
    Adds or updates PRTG monitors for an F5 entity (class or pod).

    Handles multiple PRTG entries defined in the configuration.
    Selects a PRTG server based on availability or preference.

    Args:
        entity_config: Configuration dictionary for the F5 class or pod.
                       Expected to contain "prtg" list, "class_number",
                       and optionally "pod_number".
        db_client: Active MongoDB client.
        prtg_server_preference (str, optional): Specific PRTG server name to target.

    Returns:
        Optional[str]: URL of the first successfully created/updated PRTG monitor,
                       or None if all attempts fail or no PRTG config exists.
    """
    class_number = entity_config.get("class_number")
    pod_number = entity_config.get("pod_number") # Will be None for class-level monitors

    if class_number is None:
        logger.error("F5 add_monitor: class_number missing in entity_config.")
        return None

    prtg_entries = entity_config.get("prtg")
    if not isinstance(prtg_entries, list) or not prtg_entries:
        logger.info(f"No PRTG entries found in config for F5 Class {class_number}" + (f" Pod {pod_number}" if pod_number else "") + ". Skipping monitor setup.")
        return None

    # --- 1. Get Configured F5 PRTG Servers ---
    try:
        db = db_client["labbuild_db"]
        prtg_db_config = db["prtg"].find_one({"vendor_shortcode": "ot"}) # Query for 'f5' vendor
        if not prtg_db_config or not prtg_db_config.get("servers"):
            logger.error("No PRTG server configuration found for vendor 'f5' in database.")
            return None
        all_f5_servers = prtg_db_config["servers"]
    except Exception as e:
        logger.error(f"Failed to retrieve F5 PRTG server configuration from DB: {e}", exc_info=True)
        return None

    first_successful_monitor_url: Optional[str] = None

    for prtg_entry in prtg_entries:
        try:
            monitor_name = prtg_entry.get("name")
            container_id = prtg_entry.get("container")
            template_id = prtg_entry.get("object")


            # --- 2. Calculate Target IP based on monitor_name ---
            target_ip: Optional[str] = None
            host_short = entity_config.get("host_fqdn", "").split(".")[0].lower() # e.g., "hotshot", "k2"

            if "f5vr" in monitor_name: # e.g., f5vr-{Y}
                # IP: 172.30.2.200 + class_number (last octet)
                # This logic implies class_number is used like a pod_number for IP offset
                # base_ip = "172.30.2.200" # Default, adjust if host-dependent
                # Example host-dependent:
                base_ip = "172.26.2.200" if host_short == "hotshot" else "172.30.2.200"
                parts = base_ip.split(".")
                new_last_octet = int(parts[3]) + class_number
                if 0 <= new_last_octet <= 255:
                    target_ip = ".".join(parts[:3] + [str(new_last_octet)])
                else:
                    logger.error(f"IP calc error for '{monitor_name}': last octet {new_last_octet} out of range.")
                    continue
            elif "f5-bigip" in monitor_name: # e.g., f5-bigip{X}
                if pod_number is None: logger.warning(f"Skipping '{monitor_name}': requires pod number for IP calc."); continue
                # IP: 192.168.0.31 + pod_number (second to last octet)
                base_ip_bigip = "192.168.0.31" # Base from requirement
                parts = base_ip_bigip.split(".") # Should be ['192', '168', '0', '31']
                # Add to the third octet (index 2)
                new_third_octet = int(parts[2]) + pod_number
                if 0 <= new_third_octet <= 255:
                    target_ip = ".".join([parts[0], parts[1], str(new_third_octet), parts[3]])
                else:
                    logger.error(f"IP calc error for '{monitor_name}': third octet {new_third_octet} out of range.")
                    continue
            elif "w10" in monitor_name: # e.g., f5-pod{X}-w10
                if pod_number is None: logger.warning(f"Skipping '{monitor_name}': requires pod number for IP calc."); continue
                # IP: 172.30.2.100 + pod_number (last octet)
                # base_ip_w10 = "172.30.2.100" # Default, adjust if host-dependent
                base_ip_w10 = "172.26.2.100" if host_short == "hotshot" else "172.30.2.100"
                parts = base_ip_w10.split(".")
                new_last_octet_w10 = int(parts[3]) + pod_number
                if 0 <= new_last_octet_w10 <= 255:
                    target_ip = ".".join(parts[:3] + [str(new_last_octet_w10)])
                else:
                    logger.error(f"IP calc error for '{monitor_name}': last octet {new_last_octet_w10} out of range.")
                    continue
            else:
                logger.warning(f"Unknown monitor name pattern for IP calculation: '{monitor_name}'. Skipping IP set for this monitor.")
                # We might still create the monitor without an IP if that's desired, or continue to skip it.
                # For now, let's assume an IP is critical.
                continue 
            
            if not target_ip: # Should be caught by continues above, but defensive
                logger.error(f"Target IP could not be calculated for monitor '{monitor_name}'.")
                continue
            
            logger.debug(f"Target IP for monitor '{monitor_name}': {target_ip}")

            # --- 3. Search All F5 Servers and Delete Existing Monitors with this resolved name ---
            deleted_on_any_server = False
            for server_info in all_f5_servers:
                s_url, s_token, s_name = server_info.get("url"), server_info.get("apitoken"), server_info.get("name", server_info.get("url"))
                if not s_url or not s_token: continue
                try:
                    prtg_search_mgr = PRTGManager(s_url, s_token)
                    existing_id = prtg_search_mgr.search_device(container_id, monitor_name)
                    if existing_id:
                        logger.warning(f"Found existing monitor '{monitor_name}' (ID: {existing_id}) on server {s_name}. Deleting...")
                        if prtg_search_mgr.delete_monitor_by_id(existing_id):
                            logger.info(f"Successfully deleted monitor ID {existing_id} from {s_name}.")
                            deleted_on_any_server = True
                        else:
                            logger.error(f"Failed to delete monitor ID {existing_id} from {s_name}.")
                except Exception as e_del:
                    logger.error(f"Error checking/deleting monitor '{monitor_name}' on server {s_name}: {e_del}")
            
            # --- 4. Select Target Server for Creation ---
            target_prtg_server_details = None
            if prtg_server_preference:
                target_prtg_server_details = next((s for s in all_f5_servers if s.get("name") == prtg_server_preference), None)
                if not target_prtg_server_details:
                    logger.error(f"Specified target PRTG server '{prtg_server_preference}' not found in F5 config.")
                    continue # Try next PRTG entry in pod_config
            else:
                for server_info_select in all_f5_servers:
                    s_url, s_token, s_name = server_info_select.get("url"), server_info_select.get("apitoken"), server_info_select.get("name", server_info_select.get("url"))
                    if not s_url or not s_token: continue
                    try:
                        prtg_cap_mgr = PRTGManager(s_url, s_token)
                        current_sensors = prtg_cap_mgr.get_up_sensor_count()
                        template_sensors = prtg_cap_mgr.get_template_sensor_count(template_id)
                        # Example sensor limit, adjust as needed
                        if (current_sensors + template_sensors) < 4990: # F5 might have higher limits or different templates
                            logger.info(f"Selected server {s_name} for '{monitor_name}' (Sensors: {current_sensors}+{template_sensors} < 4990)")
                            target_prtg_server_details = server_info_select
                            break
                        else:
                            logger.warning(f"Server {s_name} for '{monitor_name}' skipped: capacity {current_sensors}+{template_sensors} >= 4990.")
                    except Exception as e_cap:
                        logger.error(f"Error checking capacity for '{monitor_name}' on server {s_name}: {e_cap}")
            
            if not target_prtg_server_details:
                logger.error(f"No suitable PRTG server found for creating monitor '{monitor_name}'.")
                continue # Try next PRTG entry in pod_config

            # --- 5. Create New Monitor on Target Server ---
            final_server_url = target_prtg_server_details.get("url")
            final_api_token = target_prtg_server_details.get("apitoken")
            final_server_name = target_prtg_server_details.get("name", final_server_url)

            if not final_server_url or not final_api_token:
                logger.error(f"Target server {final_server_name} for '{monitor_name}' has incomplete config.")
                continue

            logger.info(f"Attempting to create monitor '{monitor_name}' (IP: {target_ip}) on server: {final_server_name}")
            prtg_create_mgr = PRTGManager(final_server_url, final_api_token)
            
            new_device_id = prtg_create_mgr.clone_device(template_id, container_id, monitor_name)
            if not new_device_id:
                logger.error(f"Failed to clone device '{monitor_name}' on {final_server_name}.")
                continue

            if not prtg_create_mgr.set_device_ip(new_device_id, target_ip):
                logger.error(f"Failed to set IP '{target_ip}' for new device ID {new_device_id} ('{monitor_name}') on {final_server_name}.")
                # Consider deleting the partially created device
                prtg_create_mgr.delete_monitor_by_id(new_device_id)
                continue
            
            if not prtg_create_mgr.enable_device(new_device_id):
                logger.error(f"Failed to enable monitor ID {new_device_id} ('{monitor_name}') on {final_server_name}.")
                # Consider deleting
                prtg_create_mgr.delete_monitor_by_id(new_device_id)
                continue
            
            current_monitor_url = f"{final_server_url}/device.htm?id={new_device_id}"
            logger.info(f"Successfully created and enabled PRTG monitor '{monitor_name}': {current_monitor_url}")
            if first_successful_monitor_url is None:
                first_successful_monitor_url = current_monitor_url # Capture the first success

        except Exception as e_entry:
            logger.error(f"Error processing PRTG entry {prtg_entry.get('name', 'Unknown')}: {e_entry}", exc_info=True)
            # Continue to the next PRTG entry in the list

    return first_successful_monitor_url # Return the URL of the first one that succeeded