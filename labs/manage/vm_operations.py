# labs/manage/vm_operations.py
"""Functions for performing VM power operations (start, stop, reboot)."""

import logging
from typing import Optional, List, Dict, Any, Tuple

from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim # type: ignore

logger = logging.getLogger('labbuild.vmops')

# --- Helper to get component info (name and clone pattern) ---
def _get_component_details(pod_config: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extracts component details (original name and clone pattern)"""
    details = []
    if "components" in pod_config and isinstance(pod_config["components"], list):
        for comp in pod_config["components"]:
            if isinstance(comp, dict):
                 comp_name = comp.get("component_name")
                 clone_patt = comp.get("clone_name")
                 if comp_name and clone_patt:
                     details.append({"name": comp_name, "pattern": clone_patt})
    elif "groups" in pod_config and isinstance(pod_config["groups"], list):
        for group in pod_config["groups"]:
            if isinstance(group.get("component"), list):
                for comp in group["component"]:
                    if isinstance(comp, dict):
                        comp_name = comp.get("component_name")
                        # F5 uses 'clone_vm' sometimes
                        clone_patt = comp.get("clone_vm") or comp.get("clone_name")
                        if comp_name and clone_patt:
                            details.append({"name": comp_name, "pattern": clone_patt})
    else:
        logger.warning(f"Could not find 'components' or 'groups' list in pod_config for pod {pod_config.get('pod_number', 'N/A')}")
    return details


def perform_vm_operations(service_instance: object, pod_config: Dict[str, Any], operation: str, selected_components: Optional[List[str]] = None) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Performs power operations (start, stop, reboot) on VMs within a pod's resource pool.
    Filters by selected_components if provided.
    """
    pod_number = pod_config.get("pod_number")
    class_number = pod_config.get("class_number")
    identifier = f"Pod {pod_number}" + (f" (Class {class_number})" if class_number else "")
    resource_pool_name = None # Determine based on vendor

    # Determine Resource Pool Name
    vendor_code = pod_config.get("vendor_shortcode", "").lower()
    if vendor_code == "cp": resource_pool_name = f"cp-pod{pod_number}"
    elif vendor_code == "pa": resource_pool_name = f"pa-pod{pod_number}"
    elif vendor_code == "f5" and class_number is not None: resource_pool_name = f"f5-class{class_number}"
    elif vendor_code == "av": resource_pool_name = f"av-pod{pod_number}" # Adjust if needed
    # Add other vendor logic if necessary

    if not resource_pool_name:
         err_msg = f"Could not determine resource pool name for {identifier} (vendor: {vendor_code})."
         logger.error(err_msg)
         return False, "determine_rp_name", err_msg

    logger.info(f"Performing '{operation}' on {identifier} in RP '{resource_pool_name}'")
    # Add filter info to log if applicable
    if selected_components:
        logger.info(f"Filtering for components: {', '.join(selected_components)}")


    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    rp_obj = rpm.get_obj([vim.ResourcePool], resource_pool_name)

    if not rp_obj:
        err_msg = f"Resource pool '{resource_pool_name}' not found for {identifier}."
        logger.error(err_msg)
        return False, "find_resource_pool", err_msg

    # --- Get Component Details ---
    all_component_details = _get_component_details(pod_config)
    if not all_component_details:
         err_msg = f"No components found in config for {identifier}."
         logger.error(err_msg)
         return False, "get_components_from_config", err_msg

    # --- Determine Target VM Names based on Selection ---
    target_vm_names = []
    for comp_detail in all_component_details:
        original_comp_name = comp_detail.get("name")
        pattern = comp_detail.get("pattern")

        if not original_comp_name or not pattern:
            continue # Skip if essential info is missing

        # Check if this component should be operated on
        operate_on_this = False
        if selected_components is None:
             # No filter provided, operate on all
             operate_on_this = True
        elif original_comp_name in selected_components:
             # Filter provided, and this component name matches
             operate_on_this = True

        if operate_on_this:
            # Resolve the pattern to the final VM name
            vm_name = pattern
            if "{X}" in vm_name and pod_number is not None:
                vm_name = vm_name.replace("{X}", str(pod_number))
            if "{Y}" in vm_name and class_number is not None:
                 vm_name = vm_name.replace("{Y}", str(class_number))
            # Add potential UUID substitution if needed for specific cases (unlikely for manage)

            target_vm_names.append(vm_name)
            logger.debug(f"Adding '{vm_name}' (from component '{original_comp_name}') to target list.")
        else:
             logger.debug(f"Skipping component '{original_comp_name}' based on filter: {selected_components}")


    if not target_vm_names:
        err_msg = f"No target VMs identified for operation '{operation}' on {identifier}."
        if selected_components:
             err_msg += f" Check if component name(s) '{', '.join(selected_components)}' exist in the config for this pod/class."
        logger.warning(err_msg)
        # No error occurred, just nothing matched the filter (or no components found)
        return True, "no_target_vms", err_msg

    logger.info(f"Final target VMs for {identifier} ({operation}): {', '.join(target_vm_names)}")

    # --- Perform Operation (Keep this part the same) ---
    overall_success = True
    failed_step = None
    error_message = None

    for vm_name in target_vm_names:
        vm = vmm.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            logger.warning(f"VM '{vm_name}' not found in RP '{resource_pool_name}', skipping.")
            continue # Skip this VM

        try:
            if operation == 'start':
                # logger.debug(f"Starting VM '{vm_name}'...") # Less verbose
                vmm.poweron_vm(vm_name)
                logger.info(f"VM '{vm_name}' start initiated/completed.")
            elif operation == 'stop':
                # logger.debug(f"Stopping VM '{vm_name}'...")
                vmm.poweroff_vm(vm_name)
                logger.info(f"VM '{vm_name}' stop initiated/completed.")
            elif operation == 'reboot':
                # logger.debug(f"Rebooting VM '{vm_name}'...")
                vmm.poweroff_vm(vm_name)
                vmm.poweron_vm(vm_name)
                logger.info(f"VM '{vm_name}' reboot initiated/completed.")
        except Exception as e:
            err_msg = f"Operation '{operation}' failed for VM '{vm_name}': {e}"
            logger.error(err_msg, exc_info=False) # Log less traceback by default for manage errors
            overall_success = False
            failed_step = f"{operation}_vm"
            error_message = error_message + f"; {vm_name}: {e}" if error_message else f"{vm_name}: {e}"

    if not overall_success:
       final_err_msg = f"One or more VM operations failed for {identifier}. Errors: {error_message}"
       return False, failed_step, final_err_msg
    else:
        logger.info(f"Operation '{operation}' completed for all targeted VMs in {identifier}.")
        return True, None, None

# --- END OF FILE labs/manage/vm_operations.py ---