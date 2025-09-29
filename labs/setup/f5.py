from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager
from managers.resource_pool_manager import ResourcePoolManager
from pyVmomi import vim
from monitor.prtg import PRTGManager
from typing import Dict, Any, Optional, List

import logging
import subprocess 
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo     
logger = logging.getLogger(__name__)

# Helper class for ANSI terminal color codes
class LogColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def update_network_dict(vm_name, network_dict, class_number, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')
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
            'network_name': network_name, 'mac_address': mac_address, 'connected_at_power_on': connected_at_power_on
        }
    return updated_network_dict

def build_class(service_instance, class_config, rebuild=False, full=False, selected_components=None):
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    vmm = VmManager(service_instance)

    for network in class_config["networks"]:
        switch_name = network['switch']
        if not nm.create_vswitch(class_config["host_fqdn"], switch_name):
            return False, "create_vswitch", f"Failed creating vswitch {switch_name} on host {class_config['host_fqdn']}"
        if not nm.create_vswitch_portgroups(class_config["host_fqdn"], network["switch"], network["port_groups"]):
            return False, "create_vswitch_portgroups", f"Failed creating portgroups on vswitch {switch_name}"

    class_pool = f'f5-class{class_config["class_number"]}'
    if not rpm.create_resource_pool(f'f5-{class_config["host_fqdn"][0:2]}', class_pool):
        return False, "create_resource_pool", f"Failed creating class resource pool {class_pool}"

    for group in class_config["groups"]:
        group_pool = f'{class_pool}-{group["group_name"]}'
        if not rpm.create_resource_pool(class_pool, group_pool):
            return False, "create_resource_pool", f"Failed creating group resource pool {group_pool}"

        if "srv" in group_pool:
            components_to_clone = group["component"]
            if selected_components:
                components_to_clone = [c for c in group["component"] if c["component_name"] in selected_components]

            for component in components_to_clone:
                clone_name = component["clone_vm"]
                if rebuild and not vmm.delete_vm(clone_name):
                    return False, "delete_vm", f"Failed deleting VM {clone_name}"

                if not full:
                    if not vmm.snapshot_exists(component["base_vm"], "base") and not vmm.create_snapshot(component["base_vm"], "base", description="Base for linked clones"):
                        return False, "create_snapshot", f"Failed creating base snapshot on {component['base_vm']}"
                    if not vmm.create_linked_clone(component["base_vm"], clone_name, "base", group_pool):
                        return False, "create_linked_clone", f"Failed linked clone for {clone_name}"
                else:
                    if not vmm.clone_vm(component["base_vm"], clone_name, group_pool):
                        return False, "clone_vm", f"Failed full clone for {clone_name}"

                vm_network = vmm.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict(clone_name, vm_network, int(class_config["class_number"]), int(class_config["class_number"]))
                if not vmm.update_vm_network(clone_name, updated_vm_network) or not vmm.connect_networks_to_vm(clone_name, updated_vm_network):
                    return False, "update_vm_network", f"Failed updating network for {clone_name}"

                if "bigip" in component["clone_vm"] and not vmm.update_serial_port_pipe_name(clone_name, "Serial port 1", r"\\.\pipe\com_" + str(class_config["class_number"])):
                    return False, "update_serial_port_pipe", f"Failed updating serial port on {clone_name}"

                if not vmm.create_snapshot(clone_name, "base", description=f"Snapshot of {clone_name}"):
                    return False, "create_snapshot", f"Failed creating snapshot on {clone_name}"

                if component.get("state") != "poweroff" and not vmm.poweron_vm(clone_name):
                    return False, "poweron_vm", f"Failed powering on {clone_name}"
    return True, None, None

def build_pod(service_instance, pod_config, mem=None, rebuild=False, full=False, selected_components=None):
    vmm = VmManager(service_instance)
    snapshot_name = 'base'

    for group in pod_config["groups"]:
        pod_number = pod_config["pod_number"]
        class_number = pod_config["class_number"]
        pod_pool = f'f5-class{class_number}-{group["group_name"]}'

        if "srv" not in group["group_name"]:
            components_to_clone = group["component"]
            if selected_components:
                components_to_clone = [c for c in group["component"] if c["component_name"] in selected_components]

            for component in components_to_clone:
                base_clone_name_pattern = component.get("clone_vm")
                if not base_clone_name_pattern:
                    logger.error(f"Missing 'clone_vm' key in component config for group '{group['group_name']}'")
                    return False, "config_error", f"Missing clone_vm key in group {group['group_name']}"
                
                clone_name = base_clone_name_pattern.replace("{X}", str(pod_number))

                if rebuild and not vmm.delete_vm(clone_name):
                    return False, "delete_vm", f"Failed deleting existing VM {clone_name}"
                
                if not vmm.get_obj([vim.VirtualMachine], component["base_vm"]):
                     return False, "find_base_vm", f"Base VM '{component['base_vm']}' not found"

                if not full:
                    if not vmm.snapshot_exists(component["base_vm"], "base") and not vmm.create_snapshot(component["base_vm"], "base", description="Base for linked clones"):
                        return False, "create_base_snapshot", f"Failed to create base snapshot on {component['base_vm']}"
                    if not vmm.create_linked_clone(component["base_vm"], clone_name, "base", pod_pool):
                        return False, "create_linked_clone", f"Failed to create linked clone {clone_name}"
                else:
                    if not vmm.clone_vm(component["base_vm"], clone_name, pod_pool):
                        return False, "clone_vm", f"Failed to create full clone {clone_name}"

                vm_network = vmm.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict(clone_name, vm_network, int(class_number), int(pod_number))
                if not vmm.update_vm_network(clone_name, updated_vm_network):
                    return False, "update_vm_network", f"Failed updating network for {clone_name}"

                if "bigip" in clone_name or "w10" in clone_name:
                    if not vmm.update_serial_port_pipe_name(clone_name, "Serial port 1", r"\\.\pipe\com_" + str(pod_number)):
                        return False, "update_serial_port_pipe", f"Failed updating serial port on {clone_name}"
                    if "w10" in clone_name:
                        datastore_name = "keg2" if vmm.get_obj([vim.Datastore], "keg2") else "datastore2-ho"
                        iso_path = f"podiso/pod-{pod_number}-a.iso"
                        if not vmm.modify_cd_drive(clone_name, "CD/DVD drive 1", "Datastore ISO file", datastore_name, iso_path, connected=True):
                            return False, "modify_cd_drive", f"Failed modifying CD drive for {clone_name}"

                if 'bigip' in clone_name:
                    if mem and not vmm.reconfigure_vm_resources(clone_name, new_memory_size_mb=mem):
                        return False, "reconfigure_memory", f"Failed reconfiguring memory for {clone_name}"
                    hex_pod_number = format(int(pod_number), '02x')
                    uuid = component["uuid"].replace('XX', str(hex_pod_number))
                    vmx_path = f"/tmp/{clone_name}.vmx"
                    if not vmm.download_vmx_file(clone_name, vmx_path) or \
                       not vmm.update_vm_uuid(vmx_path, uuid) or \
                       not vmm.upload_vmx_file(clone_name, vmx_path) or \
                       not vmm.verify_uuid(clone_name, uuid):
                       return False, "update_uuid", f"Failed UUID update process for {clone_name}"

                if not vmm.create_snapshot(clone_name, snapshot_name, description=f"Snapshot of {clone_name}"):
                    return False, "create_snapshot", f"Failed creating snapshot on {clone_name}"
                
                if component.get("state") != "poweroff" and not vmm.poweron_vm(clone_name):
                    return False, "poweron_vm", f"Failed powering on {clone_name}"
    
    pod_number_for_cmd = pod_config.get("pod_number")
    if pod_number_for_cmd is not None:
        logger.warning(f"{LogColors.HEADER}Preparing to schedule background 'pushlic' command for pod {pod_number_for_cmd}.{LogColors.ENDC}")
        try:
            future_time_utc = datetime.now(timezone.utc) + timedelta(minutes=5)
            future_time_local = future_time_utc.astimezone()
            local_time_str = future_time_local.strftime("%H:%M:%S %Z")
            ist_zone = ZoneInfo("Asia/Kolkata")
            future_time_ist = future_time_utc.astimezone(ist_zone)
            ist_time_str = future_time_ist.strftime("%H:%M:%S %Z")

            logger.warning(f"{LogColors.OKBLUE}The 'pushlic' command will run in 5 mins. Server Time: {local_time_str}{LogColors.ENDC}")
            logger.warning(f"{LogColors.OKBLUE}Equivalent time in India: {ist_time_str}{LogColors.ENDC}")
            command_to_run = f"(sleep 300 && /usr/local/bin/pushlic {pod_number_for_cmd} {pod_number_for_cmd}) > /dev/null 2>&1 &"
            logger.debug(f"{LogColors.OKCYAN}{LogColors.BOLD}Exact shell command being executed: {command_to_run}{LogColors.ENDC}")
            subprocess.Popen(command_to_run, shell=True)
            logger.warning(f"{LogColors.HEADER}Successfully launched background 'pushlic' command.{LogColors.ENDC}")
        except Exception as e:
            logger.error(f"{LogColors.FAIL}Failed to launch 'pushlic' command for pod {pod_number_for_cmd}. Error: {e}{LogColors.ENDC}", exc_info=True)

    return True, None, None

def teardown_class(service_instance, course_config: Dict[str, Any], start_pod: Optional[int] = None, end_pod: Optional[int] = None):
    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    
    class_name = course_config.get("class_name")
    if not class_name:
        logger.error("Missing 'class_name' in course_config for teardown.")
        return

    if start_pod is not None and end_pod is not None:
        logger.info(f"Performing selective teardown for pods {start_pod}-{end_pod} in {class_name}.")
        for group in course_config.get("groups", []):
            if "srv" in group.get("group_name", ""): continue
            for component in group.get("component", []):
                vm_name_pattern = component.get("clone_vm")
                if not vm_name_pattern: continue
                for pod_num in range(start_pod, end_pod + 1):
                    target_vm_name = vm_name_pattern.replace("{X}", str(pod_num))
                    logger.info(f"Attempting to delete VM '{target_vm_name}'...")
                    if not vmm.delete_vm(target_vm_name):
                        logger.error(f"Failed to delete VM '{target_vm_name}'. Continuing with others.")
        logger.info(f"Selective teardown for pods {start_pod}-{end_pod} in {class_name} complete.")
        return

    logger.info(f"Performing full teardown for class '{class_name}'.")
    rpm.poweroff_all_vms(class_name)
    logger.info(f"Power-off command sent to all VMs in {class_name}")

    if rpm.delete_resource_pool(class_name):
        logger.info(f"Deleted resource pool '{class_name}' successfully.")
    else: 
        logger.error(f"Failed to delete resource pool '{class_name}'.")

    for network in course_config.get("networks", []):
        switch_name = network.get('switch')
        if switch_name and nm.delete_vswitch(course_config.get("host_fqdn"), switch_name):
            logger.info(f"Deleted vSwitch '{switch_name}' successfully.")
        elif switch_name:
            logger.error(f"Failed to delete vSwitch '{switch_name}'.")

def add_monitor(
    entity_config: Dict[str, Any],
    db_client: Any,
    prtg_server_preference: Optional[str] = None
) -> Optional[str]:
    # ... (This function remains unchanged)
    class_number = entity_config.get("class_number")
    pod_number = entity_config.get("pod_number")

    if class_number is None:
        logger.error("F5 add_monitor: class_number missing in entity_config.")
        return None

    prtg_entries = entity_config.get("prtg")
    if not isinstance(prtg_entries, list) or not prtg_entries:
        logger.info(f"No PRTG entries found in config for F5 Class {class_number}" + (f" Pod {pod_number}" if pod_number else "") + ". Skipping monitor setup.")
        return None

    try:
        db = db_client["labbuild_db"]
        prtg_db_config = db["prtg"].find_one({"vendor_shortcode": "ot"})
        if not prtg_db_config or not prtg_db_config.get("servers"):
            logger.error("No PRTG server configuration found for vendor 'ot' in database.") # Changed from f5 to ot
            return None
        all_f5_servers = prtg_db_config["servers"]
    except Exception as e:
        logger.error(f"Failed to retrieve F5 PRTG server configuration from DB: {e}", exc_info=True)
        return None

    first_successful_monitor_url: Optional[str] = None

    for prtg_entry in prtg_entries:
        try:
            name_pattern = prtg_entry.get("name")
            if not name_pattern: continue

            # Resolve monitor name
            monitor_name = name_pattern.replace("{Y}", str(class_number))
            if pod_number is not None:
                monitor_name = monitor_name.replace("{X}", str(pod_number))
            
            # Skip entries that are not fully resolved for this entity type
            if "{X}" in monitor_name and pod_number is None: continue
            if "{Y}" in monitor_name and class_number is None: continue


            container_id = prtg_entry.get("container")
            template_id = prtg_entry.get("object")

            target_ip: Optional[str] = None
            host_short = entity_config.get("host_fqdn", "").split(".")[0].lower()

            if "f5vr" in name_pattern:
                base_ip = "172.26.2.200" if host_short in ("hotshot", "trypticon") else "172.30.2.200"
                parts = base_ip.split(".")
                new_last_octet = int(parts[3]) + class_number
                if 0 <= new_last_octet <= 255: target_ip = ".".join(parts[:3] + [str(new_last_octet)])
            elif "f5-bigip" in name_pattern and pod_number is not None:
                base_ip_bigip = "192.168.0.31"
                parts = base_ip_bigip.split(".")
                new_third_octet = int(parts[2]) + pod_number
                if 0 <= new_third_octet <= 255: target_ip = ".".join([parts[0], parts[1], str(new_third_octet), parts[3]])
            elif "w10" in name_pattern and pod_number is not None:
                base_ip_w10 = "172.26.2.100" if host_short in ("hotshot", "trypticon") else "172.30.2.100"
                parts = base_ip_w10.split(".")
                new_last_octet_w10 = int(parts[3]) + pod_number
                if 0 <= new_last_octet_w10 <= 255: target_ip = ".".join(parts[:3] + [str(new_last_octet_w10)])
            
            if not target_ip:
                logger.warning(f"Could not calculate target IP for monitor '{monitor_name}'. Skipping.")
                continue

            for server_info in all_f5_servers:
                s_url, s_token, s_name = server_info.get("url"), server_info.get("apitoken"), server_info.get("name", "")
                if not s_url or not s_token: continue
                try:
                    prtg_mgr = PRTGManager(s_url, s_token)
                    existing_id = prtg_mgr.search_device(container_id, monitor_name)
                    if existing_id and prtg_mgr.delete_monitor_by_id(existing_id):
                        logger.warning(f"Found and deleted existing monitor '{monitor_name}' on {s_name}.")
                except Exception as e_del:
                    logger.error(f"Error checking/deleting monitor on {s_name}: {e_del}")
            
            target_server = None
            if prtg_server_preference:
                target_server = next((s for s in all_f5_servers if s.get("name") == prtg_server_preference), None)
            else:
                for server in all_f5_servers:
                    s_url, s_token, s_name = server.get("url"), server.get("apitoken"), server.get("name", "")
                    if not s_url or not s_token: continue
                    try:
                        prtg_mgr = PRTGManager(s_url, s_token)
                        if (prtg_mgr.get_up_sensor_count() + prtg_mgr.get_template_sensor_count(template_id)) < 4990:
                            target_server = server
                            break
                    except Exception as e_cap:
                        logger.error(f"Error checking capacity on {s_name}: {e_cap}")
            
            if not target_server:
                logger.error(f"No suitable PRTG server found for '{monitor_name}'.")
                continue

            final_url, final_token, final_name = target_server.get("url"), target_server.get("apitoken"), target_server.get("name", "")
            prtg_create_mgr = PRTGManager(final_url, final_token)
            new_id = prtg_create_mgr.clone_device(template_id, container_id, monitor_name)
            if new_id and prtg_create_mgr.set_device_ip(new_id, target_ip) and prtg_create_mgr.enable_device(new_id):
                url = f"{final_url}/device.htm?id={new_id}"
                logger.info(f"Successfully created monitor '{monitor_name}' on {final_name}: {url}")
                if first_successful_monitor_url is None: first_successful_monitor_url = url
            else:
                logger.error(f"Failed to create/configure monitor '{monitor_name}' on {final_name}.")
                if new_id: prtg_create_mgr.delete_monitor_by_id(new_id)
        
        except Exception as e_entry:
            logger.error(f"Error processing PRTG entry: {e_entry}", exc_info=True)

    return first_successful_monitor_url