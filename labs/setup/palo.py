# FILE: labs/setup/palo.py
"""
This module contains the vendor-specific logic for building, tearing down,
and managing Palo Alto lab environments within the labbuild ecosystem. It is
called by the main orchestrator and uses vCenter manager classes to perform
low-level tasks.
"""

# Standard library imports
import logging
import re
import os
import getpass
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, List, Tuple

# Third-party imports
from pyVmomi import vim
from tqdm import tqdm

# Local application/library specific imports
from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from monitor.prtg import PRTGManager

# Initialize a logger specific to this module.
logger = logging.getLogger(__name__)


# --- Helper Functions for Network Configuration ---

def update_network_dict_1110(network_dict: Dict, pod_number: int) -> Dict:
    """
    Customizes a VM's network adapter configuration for PA-1110 series courses.
    """
    pod_hex = format(pod_number, '02x')
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


def update_network_dict_1100(network_dict: Dict, pod_number: int) -> Dict:
    """Customizes network settings for PA-1100 series courses."""
    pod_hex = format(pod_number, '02x')
    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = details['network_name'].replace('1', str(pod_number))
    return network_dict


def update_network_dict_cortex(network_dict: Dict, pod_number: int) -> Dict:
    """Customizes network settings for Cortex courses."""
    pod_hex = format(pod_number, '02x')
    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = f"pa-internal-cortex-{pod_number}"
    return network_dict


def solve_vlan_id(port_groups: list) -> list:
    """
    Evaluates string-based VLAN ID formulas in a port group configuration.
    """
    for group in port_groups:
        try:
            if isinstance(group["vlan_id"], str):
                 group["vlan_id"] = eval(group["vlan_id"])
        except Exception as e:
            logger.warning(f"Could not evaluate VLAN ID '{group['vlan_id']}': {e}")
            group["vlan_id"] = 0
    return port_groups


# --- Build Functions ---

def build_1110_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Builds a standard Palo Alto pod (e.g., for PCNSA/PCNSE courses).
    This process involves creating all resources from scratch, cloning all VMs,
    and performing special firewall configuration like UUID updates.
    """
    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    folder_mgr = FolderManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod = int(pod_config["pod_number"])
    target_folder_name = f'pa-pod{pod}-folder'
    parent_rp_name = f"pa-{pod_config['host_fqdn'][0:2]}"
    group_name = f'pa-pod{pod_config["pod_number"]}'

    logger.info(f"Starting build for PA Pod {pod} on host '{pod_config['host_fqdn']}'.")

    # STEP 1: Set up network infrastructure
    for network in tqdm(pod_config['networks'], desc=f"Pod {pod} → Setting up networks", unit="net", leave=False):
        solved_port_groups = solve_vlan_id(network["port_groups"])
        if not nm.create_vswitch_portgroups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups):
            return False, "create_vswitch_portgroups", f"Failed creating port groups on {network['switch_name']}"

    # STEP 2: Create organizational containers
    if not rpm.create_resource_pool(parent_rp_name, group_name, host_fqdn=pod_config["host_fqdn"]):
        return False, "create_resource_pool", f"Failed creating resource pool {group_name}"
    
    if not folder_mgr.create_folder(pod_config["vendor_shortcode"], target_folder_name):
        return False, "create_folder", f"Failed creating folder '{target_folder_name}'"

    # STEP 3: Process each component (VM) for the pod
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [c for c in components_to_build if c["component_name"] in selected_components]

    overall_component_success = True
    component_errors = []
    successful_clones = []

    for component in tqdm(components_to_build, desc=f"Pod {pod} → Cloning/Configuring", unit="vm", leave=False):
        clone_name = component["clone_name"]
        try:
            if rebuild:
                vmm.delete_vm(clone_name)

            base_vm = component["base_vm"]
            if not vmm.get_obj([vim.VirtualMachine], base_vm):
                raise Exception(f"Base VM template '{base_vm}' not found.")

            clone_successful = False
            if not full:
                if not vmm.snapshot_exists(base_vm, "base") and not vmm.create_snapshot(base_vm, "base", "Base snapshot for linked clones"):
                    raise Exception(f"Failed to create 'base' snapshot on {base_vm}")
                clone_successful = vmm.create_linked_clone(base_vm, clone_name, "base", group_name, directory_name=target_folder_name)
            else:
                clone_successful = vmm.clone_vm(base_vm, clone_name, group_name, directory_name=target_folder_name)
            
            if not clone_successful:
                raise Exception("The clone operation failed.")
            
            vm_network = vmm.get_vm_network(base_vm)
            updated_vm_network = update_network_dict_1110(vm_network, pod)
            if not vmm.update_vm_network(clone_name, updated_vm_network):
                raise Exception("Failed to update VM network adapters.")
            
             # --- START OF MODIFIED LOGIC ---
            # --- START OF THE NEW, ROBUST LOGIC ---
            if "firewall" in component["component_name"]:
                logger.info(f"Performing VMX UUID update for firewall '{clone_name}'.")
                
                # 1. Get the current user's login name.
                try:
                    current_user = getpass.getuser()
                except Exception as e:
                    logger.warning(f"Could not determine username via getpass: {e}. Falling back to 'unknown_user'.")
                    current_user = "unknown_user"

                # 2. Define the project's root directory and a local 'tmp' directory inside it.
                # __file__ gives the path to the current script (palo.py)
                script_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.abspath(os.path.join(script_dir, '..', '..')) # Go up two levels
                project_tmp_dir = os.path.join(project_root, 'tmp')
                
                # 3. Define the user-specific directory inside the project's tmp folder.
                user_tmp_dir = os.path.join(project_tmp_dir, current_user)

                # 4. Create the directories with the correct permissions (rwxrwxr-x).
                try:
                    # Create the base 'tmp' directory first
                    os.makedirs(project_tmp_dir, mode=0o775, exist_ok=True)
                    # Then create the user-specific directory
                    os.makedirs(user_tmp_dir, mode=0o775, exist_ok=True)
                    logger.debug(f"Ensured user temp directory exists with group permissions: {user_tmp_dir}")
                except OSError as e:
                    raise Exception(f"Failed to create user temp directory '{user_tmp_dir}': {e}") from e

                # 5. Construct the full path for the VMX file.
                vmx_filename = f"{clone_name}.vmx"
                vmx_path = os.path.join(user_tmp_dir, vmx_filename)
                
                # 6. Execute the download -> update -> upload -> verify sequence.
                if not vmm.download_vmx_file(clone_name, vmx_path) or \
                   not vmm.update_vm_uuid(vmx_path, component["uuid"]) or \
                   not vmm.upload_vmx_file(clone_name, vmx_path) or \
                   not vmm.verify_uuid(clone_name, component["uuid"]):
                   raise Exception("The VMX UUID update process failed.")
                
                logger.info(f"Successfully updated UUID for '{clone_name}'.")

            if not vmm.create_snapshot(clone_name, "base", description=f"Base snapshot of {clone_name}"):
                raise Exception("Failed to create 'base' snapshot on the new clone.")
            
            successful_clones.append(component)

        except Exception as e:
            error_msg = f"Component '{component.get('component_name', 'Unknown')}' failed: {e}"
            logger.error(error_msg, exc_info=True)
            component_errors.append(error_msg)
            overall_component_success = False
            continue

    # STEP 4: Power on all successfully created VMs in parallel.
    power_on_failures = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(vmm.poweron_vm, comp["clone_name"]): comp["clone_name"] for comp in successful_clones}
        for future in tqdm(futures, desc=f"Pod {pod} → Powering on", unit="vm", leave=False):
            try:
                if not future.result():
                    power_on_failures.append(futures[future])
            except Exception as e:
                failed_vm_name = futures[future]
                power_on_failures.append(f"{failed_vm_name} (Exception: {e})")

    if power_on_failures:
        error_msg = f"Failed to power on VMs: {', '.join(power_on_failures)}"
        component_errors.append(error_msg)
        overall_component_success = False

    if not overall_component_success:
        final_error_message = "; ".join(component_errors)
        return False, "component_build_failure", final_error_message
        
    logger.info(f"Successfully completed build for PA Pod {pod}.")
    return True, None, None


def build_1100_220_pod(service_instance, host_details, pod_config, rebuild=False, full=False, selected_components=None) -> Tuple[bool, Optional[str], Optional[str]]:
    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod = int(pod_config["pod_number"])
    logger.info(f"Starting build for PA 1100-220 Pod {pod}.")
    try:
        components_to_process = pod_config.get("components", [])
        if selected_components:
            components_to_process = [c for c in components_to_process if c.get("component_name") in selected_components]
        for component in tqdm(components_to_process, desc=f"Pod {pod} → Processing", unit="comp", leave=False):
            clone_name = component.get("clone_name")
            if rebuild:
                if "firewall" not in component.get("component_name", "") and "panorama" not in component.get("component_name", ""):
                    if not vmm.delete_vm(clone_name): return False, "delete_vm", f"Failed deleting VM '{clone_name}'"
                else:
                    if not vmm.poweroff_vm(component.get('vm_name')): return False, "poweroff_vm", f"Failed powering off VM '{component.get('vm_name')}'"
            host_suffix_map = {"cliffjumper": "-cl", "apollo": "-ap", "nightbird": "-ni", "ultramagnus": "-ul"}
            host_suffix = host_suffix_map.get(host_details.name.lower(), "")
            resource_pool = f"{component.get('group_name')}{host_suffix}"
            if pod % 2 == 0 and "firewall" in component.get("component_name", ""):
                if not vmm.revert_to_snapshot(component.get("vm_name"), component.get("snapshot")):
                    return False, "revert_to_snapshot", f"Failed reverting snapshot on '{component.get('vm_name')}'"
                continue
            if "firewall" not in component.get("component_name", "") and "panorama" not in component.get("component_name", ""):
                if not full:
                    if not vmm.snapshot_exists(component.get("base_vm"), "base") and not vmm.create_snapshot(component.get("base_vm"), "base", "Base snapshot"):
                        return False, "create_snapshot", f"Failed creating snapshot on '{component.get('base_vm')}'"
                    if not vmm.create_linked_clone(component.get("base_vm"), clone_name, "base", resource_pool):
                        return False, "create_linked_clone", f"Failed creating linked clone for '{clone_name}'"
                else:
                    if not vmm.clone_vm(component.get("base_vm"), clone_name, resource_pool):
                        return False, "clone_vm", f"Failed cloning VM for '{clone_name}'"
                vm_network = vmm.get_vm_network(component.get("base_vm"))
                updated_vm_network = update_network_dict_1100(vm_network, pod)
                if not vmm.update_vm_network(clone_name, updated_vm_network):
                    return False, "update_vm_network", f"Failed updating network for '{clone_name}'"
                if not vmm.create_snapshot(clone_name, "base", "Base snapshot"):
                    return False, "create_snapshot", f"Failed creating snapshot on '{clone_name}'"
            elif "firewall" in component.get("component_name", "") or "panorama" in component.get("component_name", ""):
                if not vmm.revert_to_snapshot(component.get("vm_name"), component.get("snapshot")):
                    return False, "revert_to_snapshot", f"Failed reverting snapshot on '{component.get('vm_name')}'"
        vms_to_power_on = [
            comp.get("clone_name") if "firewall" not in comp.get("component_name", "") and "panorama" not in comp.get("component_name", "") else comp.get("vm_name")
            for comp in components_to_process if not (pod % 2 == 0 and ("firewall" in comp.get("component_name", "") or "panorama" in comp.get("component_name", "")))
        ]
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(vmm.poweron_vm, name): name for name in vms_to_power_on if name}
            for future in futures:
                if not future.result(): return False, "poweron_vm", f"Failed powering on '{futures[future]}'"
        logger.info(f"Successfully completed build for PA 1100-220 Pod {pod}.")
        return True, None, None
    except Exception as e:
        logger.error(f"Build for PA 1100-220 Pod {pod} failed: {e}", exc_info=True)
        return False, "build_1100_220_exception", str(e)


def build_1100_210_pod(service_instance, host_details, pod_config, rebuild=False, full=False, selected_components=None) -> Tuple[bool, Optional[str], Optional[str]]:
    return build_1100_220_pod(service_instance, host_details, pod_config, rebuild, full, selected_components)


def build_cortex_pod(service_instance, host_details, pod_config, rebuild=False, full=False, selected_components=None) -> Tuple[bool, Optional[str], Optional[str]]:
    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    folder_mgr = FolderManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod = int(pod_config["pod_number"])
    target_folder_name = 'pa-cortex-folder'
    logger.info(f"Starting build for Cortex Pod {pod}.")
    try:
        for network in pod_config["networks"]:
            if not nm.create_vswitch_portgroups(pod_config["host_fqdn"], network["switch_name"], network["port_groups"]):
                return False, "create_vswitch_portgroups", f"Failed creating port groups on '{network['switch_name']}'"
        if not folder_mgr.create_folder(pod_config["vendor_shortcode"], target_folder_name):
            return False, "create_folder", f"Failed creating folder '{target_folder_name}'"
        
        components_to_build = pod_config.get("components", [])
        if selected_components:
            components_to_build = [c for c in components_to_build if c.get("component_name") in selected_components]
        
        successful_clones = []
        for component in components_to_build:
            clone_name = component["clone_name"]
            host_prefix = pod_config["host_fqdn"].split(".")[0].lower()
            host_suffix_map = {"cliffjumper": "-cl", "apollo": "-ap", "nightbird": "-ni", "ultramagnus": "-ul", "unicron": "-un", "hotshot": "-ho"}
            rp_name = f"{component['component_name']}{host_suffix_map.get(host_prefix, '')}"
            parent_rp_name = f"pa-{pod_config['host_fqdn'][0:2]}"
            if not rpm.create_resource_pool(parent_rp_name, rp_name):
                return False, "create_resource_pool", f"Failed creating RP '{rp_name}'"
            if rebuild and not vmm.delete_vm(clone_name):
                return False, "delete_vm", f"Rebuild failed: Could not delete '{clone_name}'"
            if not vmm.get_obj([vim.VirtualMachine], component["base_vm"]):
                return False, "find_base_vm", f"Base VM '{component['base_vm']}' not found."
            clone_successful = False
            if not full:
                if not vmm.snapshot_exists(component["base_vm"], "base") and not vmm.create_snapshot(component["base_vm"], "base", "Base snapshot"):
                    return False, "create_snapshot", f"Failed creating snapshot on '{component['base_vm']}'"
                clone_successful = vmm.create_linked_clone(component["base_vm"], clone_name, "base", rp_name, directory_name=target_folder_name)
            else:
                clone_successful = vmm.clone_vm(component["base_vm"], clone_name, rp_name, directory_name=target_folder_name)
            if not clone_successful:
                return False, "clone_vm", f"Clone operation failed for '{clone_name}'"
            vm_network = vmm.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict_cortex(vm_network, pod)
            if not vmm.update_vm_network(clone_name, updated_vm_network) or not vmm.connect_networks_to_vm(clone_name, updated_vm_network):
                return False, "configure_network", f"Failed network configuration for '{clone_name}'"
            if not vmm.create_snapshot(clone_name, "base", "Base snapshot"):
                return False, "create_snapshot", f"Failed to create snapshot on '{clone_name}'"
            successful_clones.append(component)
        
        vms_to_power_on = [comp["clone_name"] for comp in successful_clones if comp.get("state") != "poweroff"]
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(vmm.poweron_vm, name): name for name in vms_to_power_on}
            for future in futures:
                if not future.result(): return False, "poweron_vm", f"Failed to power on '{futures[future]}'"
        logger.info(f"Successfully completed build for Cortex Pod {pod}.")
        return True, None, None
    except Exception as e:
        logger.error(f"Build for Cortex Pod {pod} failed: {e}", exc_info=True)
        return False, "build_cortex_exception", str(e)


# --- Teardown Functions ---

def teardown_1110(service_instance, pod_config: Dict):
    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    pod_number = pod_config["pod_number"]
    group_name = f'pa-pod{pod_number}'
    folder_name = f'pa-pod{pod_number}-folder'
    logger.info(f"Starting full teardown for PA Pod {pod_number}.")
    vmm.delete_folder(folder_name, force=True)
    rpm.delete_resource_pool(group_name)
    for network in pod_config.get('networks', []):
        nm.delete_port_groups(pod_config['host_fqdn'], network["switch_name"], network["port_groups"])
    logger.info(f"Teardown for PA Pod {pod_number} complete.")


def teardown_1100(service_instance, pod_config: Dict):
    vmm = VmManager(service_instance)
    pod_number = pod_config["pod_number"]
    logger.info(f"Starting teardown for PA 1100-series Pod {pod_number}.")
    for component in pod_config["components"]:
        if "firewall" in component["component_name"] or "panorama" in component["component_name"]:
            vmm.poweroff_vm(component["vm_name"])
        else:
            vmm.delete_vm(component["clone_name"])
    logger.info(f"Teardown for PA 1100-series Pod {pod_number} complete.")


def teardown_cortex(service_instance, pod_config: Dict):
    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    pod_number = pod_config["pod_number"]
    logger.info(f"Starting teardown for Cortex Pod {pod_number}.")
    for component in pod_config["components"]:
        vmm.delete_vm(component["clone_name"])
    for network in pod_config["networks"]:
        nm.delete_port_groups(pod_config["host_fqdn"], network["switch_name"], network["port_groups"])
    logger.info(f"Teardown for Cortex Pod {pod_number} complete.")


# --- Monitoring Function ---

def add_monitor(pod_config: Dict, db_client, prtg_server: Optional[str] = None) -> Optional[str]:
    try:
        pod_number = int(pod_config.get("pod_number"))
        prtg_details = pod_config.get("prtg", {})
        monitor_name_pattern = prtg_details.get("name")
        container_id = prtg_details.get("container")
        template_id = prtg_details.get("object")
        if not all([monitor_name_pattern, container_id, template_id]): return None
        monitor_name = monitor_name_pattern.replace("{X}", str(pod_number))
        host_short = pod_config.get("host_fqdn", "").split(".")[0].lower()
        base_ip = "172.26.7.100" if host_short in ("hotshot", "trypticon") else "172.30.7.100"
        parts = base_ip.split('.')
        new_last_octet = int(parts[3]) + pod_number
        if not 0 <= new_last_octet <= 255: return None
        new_ip = ".".join(parts[:3] + [str(new_last_octet)])
    except Exception: return None

    try:
        db = db_client["labbuild_db"]
        prtg_conf = db["prtg"].find_one({"vendor_shortcode": "pa"})
        if not prtg_conf or not prtg_conf.get("servers"): return None
        all_pa_servers = prtg_conf["servers"]
    except Exception: return None

    for server in all_pa_servers:
        try:
            prtg_mgr = PRTGManager(server.get("url"), server.get("apitoken"))
            existing_id = prtg_mgr.search_device(container_id, monitor_name)
            if existing_id: prtg_mgr.delete_monitor_by_id(existing_id)
        except Exception: continue

    target_server_info = None
    if prtg_server:
        target_server_info = next((s for s in all_pa_servers if s.get("name") == prtg_server), None)
    else:
        for server in all_pa_servers:
            try:
                prtg_mgr = PRTGManager(server.get("url"), server.get("apitoken"))
                if (prtg_mgr.get_up_sensor_count() + prtg_mgr.get_template_sensor_count(template_id)) < 499:
                    target_server_info = server
                    break
            except Exception: continue
    
    if not target_server_info: return None

    try:
        target_mgr = PRTGManager(target_server_info["url"], target_server_info["apitoken"])
        new_device_id = target_mgr.clone_device(template_id, container_id, monitor_name)
        if not new_device_id: raise Exception("Clone device failed.")
        if not target_mgr.set_device_ip(new_device_id, new_ip): raise Exception("Set device IP failed.")
        if not target_mgr.enable_device(new_device_id): raise Exception("Enable device failed.")
        return f"{target_server_info['url']}/device.htm?id={new_device_id}"
    except Exception: return None