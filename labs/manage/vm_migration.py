import logging
from typing import Dict, Any, Tuple, Optional

from pyVmomi import vim

from managers.vm_manager import VmManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from config_utils import extract_components
from labs.setup import checkpoint, palo, nu, f5, avaya, pr # For teardown logic

logger = logging.getLogger('labbuild.migration')

def _get_pod_container_names(pod_config: Dict[str, Any]) -> Dict[str, str]:
    """Helper to determine correct folder and resource pool names."""
    pod_number = pod_config["pod_number"]
    course_name_lower = pod_config.get("course_name", "").lower()
    vendor_short = pod_config.get("vendor_shortcode", "unknown")
    host_short = pod_config.get("host_fqdn", "").split('.')[0]

    parent_rp_name = f"{vendor_short}-{host_short}"
    
    # Default naming
    target_rp_name = f"{vendor_short}-pod{pod_number}"
    target_folder_name = f"{vendor_short}-pod{pod_number}-folder"

    # Vendor-specific overrides
    if vendor_short == "cp":
        prefix = 'cp-maestro' if "maestro" in course_name_lower else 'cp'
        target_rp_name = f'{prefix}-pod{pod_number}'
        target_folder_name = f'{prefix}-{pod_number}-folder'
    elif vendor_short == "f5":
        class_num = pod_config.get("class_number")
        target_rp_name = f'f5-class{class_num}'
        target_folder_name = f'f5-class{class_num}-folder' # Assuming folder is named after class
    # Add other vendor-specific naming conventions here if needed

    return {
        "folder_name": target_folder_name,
        "rp_name": target_rp_name,
        "parent_rp_name": parent_rp_name
    }


def migrate_pod(service_instance, source_pod_config: Dict[str, Any], dest_host_details: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Orchestrates the migration of all VMs in a pod from a source host to a destination host.
    """
    pod_number = source_pod_config["pod_number"]
    identifier = f"Pod {pod_number}"
    logger.info(f"--- Starting migration for {identifier} from '{source_pod_config['host_fqdn']}' to '{dest_host_details['fqdn']}' ---")

    try:
        # 1. Initialize Managers and Determine Names
        vmm = VmManager(service_instance)
        fm = FolderManager(service_instance)
        rpm = ResourcePoolManager(service_instance)
        nm = NetworkManager(service_instance)

        source_containers = _get_pod_container_names(source_pod_config)
        dest_pod_config = source_pod_config.copy()
        dest_pod_config['host_fqdn'] = dest_host_details['fqdn']
        dest_containers = _get_pod_container_names(dest_pod_config)

        # 2. Prepare Destination Environment
        logger.info(f"Preparing destination environment on host '{dest_host_details['fqdn']}'...")
        if not rpm.create_resource_pool(dest_containers['parent_rp_name'], dest_containers['rp_name']):
            return False, "prep_dest_rp", f"Failed to create destination RP '{dest_containers['rp_name']}'."
        
        # Folders are datacenter-wide, so they don't need to be recreated, just ensure permissions
        # We can reuse the original folder
        
        # Recreate networks on the destination host
        for network_config in source_pod_config.get("networks", []):
            if not nm.create_vswitch_portgroups(dest_host_details['fqdn'], network_config["switch_name"], network_config["port_groups"]):
                return False, "prep_dest_networks", f"Failed to create networks on destination host."

        dest_rp = rpm.get_obj([vim.ResourcePool], dest_containers['rp_name'])
        if not dest_rp:
            return False, "find_dest_rp", "Could not find destination resource pool after creation."
        
        # 3. Apply Permissions on Destination
        # This part assumes a permissions model like Checkpoint's. Adapt as needed.
        if source_pod_config.get("vendor_shortcode") == "cp":
            user = f"vcenter.rededucation.com\\labcp-{pod_number}"
            role = "labcp-0-role"
            if not rpm.assign_role_to_resource_pool(dest_containers['rp_name'], user, role):
                return False, "apply_dest_permissions", "Failed to apply permissions to destination RP."

        # 4. Identify and Migrate VMs
        component_details = extract_components(source_pod_config)
        vm_names_to_migrate = [
            (c.get("clone_name") or c.get("clone_vm")).replace("{X}", str(pod_number))
            for c in source_pod_config.get("components", []) if c.get("component_name") in component_details
        ]
        
        for vm_name in vm_names_to_migrate:
            if not vmm.migrate_vm(vm_name, dest_host_details['pyvmomi_obj'], dest_rp):
                # If one VM fails, we stop and report the error.
                return False, "migrate_vm", f"Failed to migrate VM '{vm_name}'."
            # Power on the VM after successful migration
            vmm.poweron_vm(vm_name)

        # 5. Cleanup Source Environment
        logger.info(f"Migration successful. Cleaning up source resources on '{source_pod_config['host_fqdn']}'...")
        if not rpm.delete_resource_pool(source_containers['rp_name']):
            logger.warning(f"Failed to clean up source resource pool '{source_containers['rp_name']}'. Please remove it manually.")
        if not fm.delete_folder(source_containers['folder_name'], force=True):
             logger.warning(f"Failed to clean up source folder '{source_containers['folder_name']}'. Please remove it manually.")
        for network_config in source_pod_config.get("networks", []):
            if not nm.delete_vswitch(source_pod_config['host_fqdn'], network_config["switch_name"]):
                logger.warning(f"Failed to clean up source vSwitch '{network_config['switch_name']}'.")

        logger.info(f"--- Migration for {identifier} completed successfully. ---")
        return True, None, None

    except Exception as e:
        error_msg = f"An unexpected error occurred during migration of {identifier}: {e}"
        logger.error(error_msg, exc_info=True)
        return False, "unexpected_migration_error", str(e)