import logging
from typing import Dict, Any, Tuple, Optional

from pyVmomi import vim

from managers.vm_manager import VmManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from config_utils import extract_components

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
        if class_num is not None:
            target_rp_name = f'f5-class{class_num}'
            target_folder_name = f'f5-class{class_num}-folder'
        else:
            logger.warning(f"F5 course detected for pod {pod_number} but no class_number found in config.")


    return {
        "folder_name": target_folder_name,
        "rp_name": target_rp_name,
        "parent_rp_name": parent_rp_name
    }


def _migrate_pod_intra_vcenter(service_instance, source_pod_config, dest_host_details) -> Tuple[bool, Optional[str], Optional[str]]:
    """Handles migration of a pod between hosts WITHIN the same vCenter."""
    pod_number = source_pod_config["pod_number"]
    identifier = f"Pod {pod_number}"
    logger.info(f"--- Starting INTRA-vCenter migration for {identifier} from '{source_pod_config['host_fqdn']}' to '{dest_host_details['fqdn']}' ---")

    try:
        vmm = VmManager(service_instance)
        rpm = ResourcePoolManager(service_instance)
        nm = NetworkManager(service_instance)

        source_containers = _get_pod_container_names(source_pod_config)
        dest_pod_config = source_pod_config.copy()
        dest_pod_config['host_fqdn'] = dest_host_details['fqdn']
        dest_containers = _get_pod_container_names(dest_pod_config)

        # Prepare Destination Environment (RP and Networks are host-specific)
        if not rpm.create_resource_pool(dest_containers['parent_rp_name'], dest_containers['rp_name']):
            return False, "prep_dest_rp", f"Failed to create destination RP '{dest_containers['rp_name']}'."
        for network_config in source_pod_config.get("networks", []):
            if not nm.create_vswitch_portgroups(dest_host_details['fqdn'], network_config["switch_name"], network_config["port_groups"]):
                return False, "prep_dest_networks", "Failed to create networks on destination host."

        dest_rp = rpm.get_obj([vim.ResourcePool], dest_containers['rp_name'])
        if not dest_rp:
            return False, "find_dest_rp", "Could not find destination resource pool."
        
        # Apply permissions if needed
        if source_pod_config.get("vendor_shortcode") == "cp":
            user = f"vcenter.rededucation.com\\labcp-{pod_number}"
            role = "labcp-0-role"
            if not rpm.assign_role_to_resource_pool(dest_containers['rp_name'], user, role):
                return False, "apply_dest_permissions", "Failed to apply permissions to destination RP."

        # Identify and Migrate VMs
        component_details = extract_components(source_pod_config)
        vm_names_to_migrate = [
            (c.get("clone_name") or c.get("clone_vm")).replace("{X}", str(pod_number))
            for c in source_pod_config.get("components", []) if c.get("component_name") in component_details
        ]
        
        for vm_name in vm_names_to_migrate:
            if not vmm.migrate_vm(vm_name, dest_host_details['pyvmomi_obj'], dest_rp):
                return False, "migrate_vm", f"Failed to migrate VM '{vm_name}'."
            vmm.poweron_vm(vm_name)

        # Cleanup Source (RP and Networks, NOT folder)
        logger.info(f"Intra-vCenter migration successful. Cleaning up source host-specific resources...")
        if not rpm.delete_resource_pool(source_containers['rp_name']):
            logger.warning(f"Failed to clean up source resource pool '{source_containers['rp_name']}'. Please remove it manually.")
        for network_config in source_pod_config.get("networks", []):
            if not nm.delete_vswitch(source_pod_config['host_fqdn'], network_config["switch_name"]):
                logger.warning(f"Failed to clean up source vSwitch '{network_config['switch_name']}'.")

        return True, None, None

    except Exception as e:
        logger.error(f"Unexpected error during intra-vCenter migration: {e}", exc_info=True)
        return False, "unexpected_intra_vcenter_error", str(e)


def _migrate_pod_cross_vcenter(source_si, dest_si, source_pod_config, dest_host_details) -> Tuple[bool, Optional[str], Optional[str]]:
    """Handles migration of a pod between hosts across DIFFERENT vCenters via clone and delete."""
    pod_number = source_pod_config["pod_number"]
    identifier = f"Pod {pod_number}"
    logger.info(f"--- Starting CROSS-vCenter migration for {identifier} from '{source_pod_config['host_fqdn']}' to '{dest_host_details['fqdn']}' ---")

    try:
        # Create managers for both vCenters
        source_vmm = VmManager(source_si)
        source_fm = FolderManager(source_si)
        source_rpm = ResourcePoolManager(source_si)
        source_nm = NetworkManager(source_si)

        dest_vmm = VmManager(dest_si)
        dest_fm = FolderManager(dest_si)
        dest_rpm = ResourcePoolManager(dest_si)
        dest_nm = NetworkManager(dest_si)
        
        source_containers = _get_pod_container_names(source_pod_config)
        dest_pod_config = source_pod_config.copy()
        dest_pod_config['host_fqdn'] = dest_host_details['fqdn']
        dest_containers = _get_pod_container_names(dest_pod_config)

        # Prepare Destination Environment on the DESTINATION vCenter
        logger.info(f"Preparing destination environment on host '{dest_host_details['fqdn']}'...")
        if not dest_fm.create_folder(dest_pod_config["vendor_shortcode"], dest_containers['folder_name']):
            return False, "prep_dest_folder_cross", f"Failed to ensure destination folder '{dest_containers['folder_name']}' exists."
        if not dest_rpm.create_resource_pool(dest_containers['parent_rp_name'], dest_containers['rp_name']):
            return False, "prep_dest_rp_cross", f"Failed to create destination RP '{dest_containers['rp_name']}'."
        for network_config in dest_pod_config.get("networks", []):
            if not dest_nm.create_vswitch_portgroups(dest_host_details['fqdn'], network_config["switch_name"], network_config["port_groups"]):
                return False, "prep_dest_networks_cross", "Failed to create networks on destination host."

        dest_folder = dest_fm.get_obj([vim.Folder], dest_containers['folder_name'])
        dest_rp = dest_rpm.get_obj([vim.ResourcePool], dest_containers['rp_name'])
        dest_datastore = dest_host_details['pyvmomi_obj'].datastore[0]

        if not all([dest_folder, dest_rp, dest_datastore]):
            return False, "find_dest_resources_cross", "Could not find destination folder, RP, or datastore on destination vCenter."

        # Apply permissions on destination
        if source_pod_config.get("vendor_shortcode") == "cp":
            user = f"vcenter.rededucation.com\\labcp-{pod_number}"
            role = "labcp-0-role"
            if not dest_fm.assign_user_to_folder(dest_containers['folder_name'], user, role):
                return False, "apply_dest_folder_permissions", "Failed to apply permissions to destination folder."
            if not dest_rpm.assign_role_to_resource_pool(dest_containers['rp_name'], user, role):
                return False, "apply_dest_rp_permissions", "Failed to apply permissions to destination RP."

        # Identify and Clone VMs
        component_details = extract_components(source_pod_config)
        vm_names_to_migrate = [
            (c.get("clone_name") or c.get("clone_vm")).replace("{X}", str(pod_number))
            for c in source_pod_config.get("components", []) if c.get("component_name") in component_details
        ]

        for vm_name in vm_names_to_migrate:
            # Use the cross-vCenter clone method on the SOURCE VmManager
            if not source_vmm.clone_vm_cross_vcenter(vm_name, dest_si, vm_name, dest_rp, dest_folder, dest_datastore):
                return False, "clone_vm_cross_vcenter", f"Failed to clone VM '{vm_name}' to destination."
            # Power on the NEWLY CREATED VM using the DESTINATION VmManager
            dest_vmm.poweron_vm(vm_name)

        # Cleanup Source Environment (using source managers)
        logger.info(f"Cross-vCenter clone successful. Cleaning up all source resources...")
        
        # Here, we DO delete the folder as it's on a different vCenter
        if not source_fm.delete_folder(source_containers['folder_name'], force=True):
             logger.warning(f"Failed to clean up source folder '{source_containers['folder_name']}'. Please remove it manually.")
        if not source_rpm.delete_resource_pool(source_containers['rp_name']):
            logger.warning(f"Failed to clean up source resource pool '{source_containers['rp_name']}'. Please remove it manually.")
        for network_config in source_pod_config.get("networks", []):
            if not source_nm.delete_vswitch(source_pod_config['host_fqdn'], network_config["switch_name"]):
                logger.warning(f"Failed to clean up source vSwitch '{network_config['switch_name']}'.")

        return True, None, None
    except Exception as e:
        logger.error(f"Unexpected error during cross-vCenter migration: {e}", exc_info=True)
        return False, "unexpected_cross_vcenter_error", str(e)


def migrate_pod(source_si, dest_si, source_pod_config, dest_host_details) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Dispatcher function that decides which migration type to use.
    It compares the service instance objects to determine if they are the same vCenter.
    """
    if source_si.connection._stub.host == dest_si.connection._stub.host:
        logger.info("Source and destination vCenters are the same. Performing intra-vCenter migration.")
        # We only need one service_instance for intra-vCenter
        return _migrate_pod_intra_vcenter(source_si, source_pod_config, dest_host_details)
    else:
        logger.info("Source and destination vCenters are different. Performing cross-vCenter migration.")
        return _migrate_pod_cross_vcenter(source_si, dest_si, source_pod_config, dest_host_details)