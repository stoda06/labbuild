# In labs/manage/vm_relocation.py

import logging
from typing import Dict, Any, Tuple, List, Optional
from pyVmomi import vim

from managers.vm_manager import VmManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from config_utils import extract_components

logger = logging.getLogger('labbuild.relocation')

def _get_pod_destinations(pod_config: Dict[str, Any]) -> Dict[str, str]:
    """
    Centralized helper to determine the correct folder, resource pool,
    and domain user for a given pod configuration.
    """
    pod_number = pod_config["pod_number"]
    course_name_lower = pod_config.get("course_name", "").lower()
    vendor_short = pod_config.get("vendor_shortcode", "unknown")
    host_short = pod_config.get("host_fqdn", "").split('.')[0]

    # Default naming convention
    target_group_name = f"{vendor_short}-pod{pod_number}"
    target_folder_name = f"{vendor_short}-pod{pod_number}-folder"
    parent_rp_name = f"{vendor_short}-{host_short}"

    # Vendor-specific overrides (e.g., for Checkpoint)
    if vendor_short == "cp":
        if "maestro" in course_name_lower:
            target_group_name = f'cp-maestro-pod{pod_number}'
            target_folder_name = f'cp-maestro-{pod_number}-folder'
        else:
            target_group_name = f'cp-pod{pod_number}'
            target_folder_name = f'cp-pod{pod_number}-folder'
        
        domain_user = f"vcenter.rededucation.com\\labcp-{pod_number}"
        role_name = "labcp-0-role"
    else:
        # Define domain_user/role_name for other vendors if they have specific permissions
        domain_user = None
        role_name = None

    return {
        "folder_name": target_folder_name,
        "rp_name": target_group_name,
        "parent_rp_name": parent_rp_name,
        "domain_user": domain_user,
        "role_name": role_name
    }

def relocate_pod_vms(service_instance, pod_config: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Finds all VMs for a pod, moves them to the correct folder and resource pool,
    and reapplies necessary permissions.
    """
    pod_number = pod_config.get("pod_number")
    identifier = f"Pod {pod_number}"
    logger.info(f"--- Starting relocation for {identifier} ---")

    try:
        # 1. Determine destination names and user/role
        destinations = _get_pod_destinations(pod_config)
        folder_name = destinations['folder_name']
        rp_name = destinations['rp_name']
        parent_rp_name = destinations['parent_rp_name']
        domain_user = destinations['domain_user']
        role_name = destinations['role_name']
        
        # 2. Initialize Managers
        vmm = VmManager(service_instance)
        fm = FolderManager(service_instance)
        rpm = ResourcePoolManager(service_instance)

        # 3. Ensure destination folder and RP exist (create if they don't)
        if not fm.create_folder(pod_config["vendor_shortcode"], folder_name):
            return False, "create_folder", f"Failed to ensure folder '{folder_name}' exists."
        
        if not rpm.create_resource_pool(parent_rp_name, rp_name):
            return False, "create_resource_pool", f"Failed to ensure resource pool '{rp_name}' exists."

        target_folder = fm.get_obj([vim.Folder], folder_name)
        target_rp = rpm.get_obj([vim.ResourcePool], rp_name)

        if not target_folder or not target_rp:
            return False, "find_destinations", "Could not find destination folder or RP after creation attempt."

        # 4. Identify all VMs belonging to this pod
        component_names = extract_components(pod_config)
        vm_names_to_move = []
        for name in component_names:
            # Find the component's clone name pattern in the config
            clone_name_pattern = next(
                (c.get("clone_name") or c.get("clone_vm") for c in pod_config.get("components", []) if c.get("component_name") == name),
                None
            )
            if clone_name_pattern:
                vm_name = clone_name_pattern.replace("{X}", str(pod_number))
                vm_names_to_move.append(vm_name)

        if not vm_names_to_move:
            return True, "no_vms_to_move", f"No VMs defined in config for {identifier}."

        # 5. Move each VM
        for vm_name in vm_names_to_move:
            if not vmm.move_vm_to_folder(vm_name, target_folder):
                logger.warning(f"Failed to move VM '{vm_name}' to folder '{folder_name}'. It might not exist.")
                # Continue trying to move other VMs
            
            if not vmm.move_vm_to_resource_pool(vm_name, target_rp):
                logger.warning(f"Failed to move VM '{vm_name}' to resource pool '{rp_name}'. It might not exist.")

        # 6. Re-apply permissions to the containers
        if domain_user and role_name:
            logger.info(f"Re-applying permissions for '{domain_user}' to containers for {identifier}.")
            if not fm.assign_user_to_folder(folder_name, domain_user, role_name):
                return False, "apply_folder_permissions", f"Failed to apply permissions to folder '{folder_name}'."
            if not rpm.assign_role_to_resource_pool(rp_name, domain_user, role_name):
                return False, "apply_rp_permissions", f"Failed to apply permissions to resource pool '{rp_name}'."
        
        logger.info(f"--- Relocation for {identifier} completed successfully. ---")
        return True, None, None

    except Exception as e:
        error_msg = f"An unexpected error occurred during relocation of {identifier}: {e}"
        logger.error(error_msg, exc_info=True)
        return False, "unexpected_relocation_error", str(e)