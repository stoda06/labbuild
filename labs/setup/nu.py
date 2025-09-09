from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from monitor.prtg import PRTGManager
from managers.network_manager import NetworkManager
from tqdm import tqdm
from pyVmomi import vim
import logging

logger = logging.getLogger(__name__)

def update_network_dict(network_dict, pod_number, course_name="", component_name=""):
    """
    Conditionally updates network configurations based on the course and component.
    - For 'nu-aapm-610', applies special rules for 'aapm', 'vr', and 'w10' components.
    - For all other courses, applies the original default logic.
    """
    pod_hex = format(pod_number, '02x')

    # --- Course-specific logic for nu-aapm-610 ---
    if "aapm-610" in course_name:
        logger.debug(f"Applying 'aapm-610' network rules for component '{component_name}' pod {pod_number}.")
        updated_network_dict = {}
        
        sorted_adapters = sorted(network_dict.keys())

        for adapter_label in sorted_adapters:
            details = network_dict[adapter_label]
            new_details = details.copy() 

            if "vr" in component_name.lower():
                try:
                    adapter_number = int(adapter_label.split()[-1])
                except (ValueError, IndexError):
                    logger.warning(
                        f"Could not parse adapter number from '{adapter_label}' for component '{component_name}'. "
                        "Applying default 'nuvr' network as a fallback."
                    )
                    new_details['network_name'] = f"nuvr-{pod_number}"
                else:
                    if adapter_number == 1:
                        new_details['network_name'] = "nu-rdp"
                    elif adapter_number == 2:
                        new_details['network_name'] = f"vgt-{pod_number}"
                    else:
                        new_details['network_name'] = f"nuvr-{pod_number}"
            
            # --- MODIFICATION START ---
            elif "aapm" in component_name.lower():
                # Requirement: 
                # - All networks connect to 'vgt-X'
                # - Adapters 1 & 2 are DISCONNECTED at power on.
                # - Adapters 3 & 4 (and others) are CONNECTED at power on.
                new_details['network_name'] = f"vgt-{pod_number}"
                
                try:
                    # Parse the adapter number from the label (e.g., "Network adapter 1" -> 1)
                    adapter_number = int(adapter_label.split()[-1])
                    if adapter_number <= 2:
                        new_details['connected_at_power_on'] = False
                        logger.debug(f"For AAPM component '{component_name}', setting {adapter_label} to disconnected.")
                    else:
                        new_details['connected_at_power_on'] = True
                        logger.debug(f"For AAPM component '{component_name}', setting {adapter_label} to connected.")
                except (ValueError, IndexError):
                    logger.warning(
                        f"Could not parse adapter number from '{adapter_label}' for AAPM component. "
                        "Defaulting to 'connected'."
                    )
                    # If we can't parse it, it's safer to leave it connected.
                    new_details['connected_at_power_on'] = True
            # --- MODIFICATION END ---

            elif "w10" in component_name.lower():
                if 'rdp' not in details['network_name']:
                    new_details['network_name'] = f"nu-vr-{pod_number}"

            if 'rdp' in new_details['network_name']:
                mac_parts = '00:50:56:05:00:00'.split(':')
                mac_parts[-1] = pod_hex
                new_details['mac_address'] = ':'.join(mac_parts)

            updated_network_dict[adapter_label] = new_details
        
        return updated_network_dict
    
    # --- Fallback to original logic for all other 'nu' courses ---
    logger.debug(f"Applying default 'nu' network rules for pod {pod_number}.")
    for adapter, details in network_dict.items():
        if 'nu-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:05:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = f'nuvr-{pod_number}'
            
    return network_dict


def build_nu_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):
    rpm = ResourcePoolManager(service_instance)
    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    pod = pod_config["pod_number"]
    snapshot_name = 'base'
    course_name = pod_config.get("course_name", "").lower()

    parent_resource_pool = f'{pod_config["vendor_shortcode"]}-{pod_config["host_fqdn"][0:2]}'
    resource_pool = f'nu-pod{pod}-{pod_config["host_fqdn"][0:2]}'

    if not rpm.create_resource_pool(parent_resource_pool, resource_pool):
        return False, "create_resource_pool", f"Failed creating resource pool {resource_pool}"

    if "aapm-610" in course_name:
        logger.info(f"Course 'aapm-610' detected. Ensuring required networks exist for pod {pod}.")
        
        target_vswitch = nm.find_vswitch_by_name_substring(pod_config["host_fqdn"], "-nt")
        if not target_vswitch:
            return False, "find_vswitch", "Could not find a vSwitch with '-nt' in its name for network creation."

        required_port_groups = [
            {"port_group_name": f"nu-vr-{pod}", "vlan_id": 0},
            {"port_group_name": f"vgt-{pod}", "vlan_id": 4095}
        ]
        
        logger.info(f"Attempting to create port groups on vSwitch '{target_vswitch}'.")
        if not nm.create_vm_port_groups(pod_config["host_fqdn"], target_vswitch, required_port_groups):
            return False, "create_vm_port_groups", f"Failed to create required port groups for aapm-610 on {target_vswitch}."

    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]

    for component in tqdm(components_to_build, desc=f"nu-pod{pod} → Building components", unit="comp"):
        if rebuild:
            if not vmm.delete_vm(component["clone_name"]):
                return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"

        if not full:
            if not vmm.snapshot_exists(component["base_vm"], "base"):
                if not vmm.create_snapshot(component["base_vm"], "base", description="Snapshot for clones"):
                    return False, "create_snapshot", f"Failed creating snapshot on {component['base_vm']}"
            if not vmm.create_linked_clone(component["base_vm"], component["clone_name"], "base", resource_pool):
                return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
        else:
            if not vmm.clone_vm(component["base_vm"], component["clone_name"], resource_pool):
                return False, "clone_vm", f"Failed cloning VM for {component['clone_name']}"

        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict(
            network_dict=vm_network,
            pod_number=int(pod),
            course_name=course_name,
            component_name=component["component_name"]
        )

        if not vmm.update_vm_network(component["clone_name"], updated_vm_network):
            return False, "update_vm_network", f"Failed updating network for {component['clone_name']}"
        if not vmm.connect_networks_to_vm(component["clone_name"], updated_vm_network):
            return False, "connect_networks_to_vm", f"Failed connecting networks for {component['clone_name']}"

        if not vmm.snapshot_exists(component["clone_name"], snapshot_name):
            if not vmm.create_snapshot(component["clone_name"], snapshot_name, description=f"Snapshot of {component['clone_name']}"):
                return False, "create_snapshot", f"Failed creating snapshot on {component['clone_name']}"

        if component.get("state") != "poweroff":
            if not vmm.poweron_vm(component["clone_name"]):
                return False, "poweron_vm", f"Failed powering on {component['clone_name']}"

    return True, None, None


def teardown_nu_pod(service_instance, pod_config):
    rpm = ResourcePoolManager(service_instance)
    group_name = f'nu-pod{pod_config["pod_number"]}-{pod_config["host_fqdn"][0:2]}'

    rpm.poweroff_all_vms(group_name)
    rpm.logger.info(f'Power-off all VMs in {group_name}')
    rpm.delete_resource_pool(group_name)
    rpm.logger.info(f'Removed resource pool {group_name} and all its VMs.')


def add_monitor(pod_config, db_client, prtg_server=None):
    """
    Adds or updates a PRTG monitor for a Nutanix pod.
    """
    try:
        pod_number = int(pod_config.get("pod_number"))
        prtg_details = pod_config.get("prtg", {})
        monitor_name_pattern = prtg_details.get("name")
        container_id = prtg_details.get("container")
        template_id = prtg_details.get("object")

        if not all([monitor_name_pattern, container_id, template_id]):
            logger.error("Missing required PRTG config (name pattern, container, object) in pod_config.")
            return None

        monitor_name = monitor_name_pattern.replace("{X}", str(pod_number))

        host_short = pod_config.get("host_fqdn", "").split(".")[0].lower()
        if host_short in ("hotshot", "trypticon"):
            base_ip = "172.26.5.100"
        else:
            base_ip = "172.30.5.100"

        parts = base_ip.split('.')
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

    try:
        db = db_client["labbuild_db"]
        prtg_conf = db["prtg"].find_one({"vendor_shortcode": "ot"})
        if not prtg_conf or not prtg_conf.get("servers"):
            logger.error("No PRTG server configuration found for vendor 'ot' in database.")
            return None
        all_ot_servers = prtg_conf["servers"]
    except Exception as e:
        logger.error(f"Failed to retrieve PRTG server configuration from DB: {e}", exc_info=True)
        return None

    action_log = []
    for server in all_ot_servers:
        server_url, api_token, server_name = server.get("url"), server.get("apitoken"), server.get("name", server.get("url"))
        if not server_url or not api_token: continue

        try:
            prtg_mgr = PRTGManager(server_url, api_token)
            existing_id = prtg_mgr.search_device(container_id, monitor_name)
            if existing_id:
                if pod_number > 1:
                    logger.warning(f"Found existing monitor '{monitor_name}' (ID: {existing_id}) on {server_name}. Deleting...")
                    if prtg_mgr.delete_monitor_by_id(existing_id): action_log.append(f"Deleted from {server_name}")
                    else: logger.error(f"Failed to delete monitor ID {existing_id} from {server_name}.")
                else:
                    logger.warning(f"Found existing monitor '{monitor_name}' (ID: {existing_id}) on {server_name}. Pausing...")
                    if prtg_mgr.pause_device(existing_id): action_log.append(f"Paused on {server_name}")
                    else: logger.error(f"Failed to pause monitor ID {existing_id} from {server_name}.")
        except Exception as e:
            logger.error(f"Error during pre-creation check on server {server_name}: {e}", exc_info=True)

    if action_log:
        logger.info(f"Finished pre-creation actions for '{monitor_name}': {', '.join(action_log)}.")

    target_server_info = None
    if prtg_server:
        target_server_info = next((s for s in all_ot_servers if s.get("name") == prtg_server), None)
        if not target_server_info:
            logger.error(f"Specified target PRTG server '{prtg_server}' not found in 'ot' configuration.")
            return None
    else:
        for server in all_ot_servers:
            server_url, api_token, server_name = server.get("url"), server.get("apitoken"), server.get("name", server.get("url"))
            if not server_url or not api_token: continue
            try:
                prtg_mgr = PRTGManager(server_url, api_token)
                if (prtg_mgr.get_up_sensor_count() + prtg_mgr.get_template_sensor_count(template_id)) < 499:
                    target_server_info = server
                    break
            except Exception as e:
                logger.error(f"Error checking capacity on server {server_name}: {e}")

    if not target_server_info:
        logger.error(f"Could not find any suitable target PRTG server for monitor '{monitor_name}'.")
        return None

    target_url, target_token, target_name = target_server_info.get("url"), target_server_info.get("apitoken"), target_server_info.get("name", target_server_info.get("url"))
    logger.info(f"Attempting to create monitor '{monitor_name}' on target server: {target_name}")
    try:
        prtg_target_mgr = PRTGManager(target_url, target_token)
        new_device_id = prtg_target_mgr.clone_device(template_id, container_id, monitor_name)
        if not new_device_id:
            logger.error(f"Failed to clone device '{monitor_name}' on {target_name}.")
            return None
        if not prtg_target_mgr.set_device_ip(new_device_id, new_ip):
            logger.error(f"Failed to set IP '{new_ip}' for device ID {new_device_id} on {target_name}.")
            return None
        if not prtg_target_mgr.enable_device(new_device_id):
            logger.error(f"Failed to enable monitor ID {new_device_id} on {target_name}.")
            return None
        monitor_url = f"{target_url}/device.htm?id={new_device_id}"
        logger.info(f"Successfully created and enabled PRTG monitor: {monitor_url}")
        return monitor_url
    except Exception as e:
        logger.error(f"Error during monitor creation on target server {target_name}: {e}", exc_info=True)
        return None