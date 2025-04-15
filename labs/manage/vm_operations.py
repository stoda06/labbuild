import logging
from typing import Optional, List, Dict, Any, Tuple

from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
# pyVim/pyVmomi imports
from pyVim.connect import SmartConnect, Disconnect # Likely needed if VCenter instance isn't passed fully connected
from pyVmomi import vim # type: ignore

logger = logging.getLogger('labbuild.vmops') # Use a consistent logger naming scheme

# --- Helper to get component info (name and clone pattern) ---
def _get_component_details(pod_config: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extracts component details (original name and clone pattern/name) from pod config."""
    details = []
    # Check top-level 'components' first (common structure)
    if "components" in pod_config and isinstance(pod_config["components"], list):
        for comp in pod_config["components"]:
            if isinstance(comp, dict):
                comp_name = comp.get("component_name")
                # Check multiple possible keys for the final VM name pattern
                clone_patt = comp.get("clone_name") or comp.get("clone_vm") or comp.get("vm_name")
                if comp_name and clone_patt:
                    details.append({"name": comp_name, "pattern": clone_patt})
    # Check nested 'groups' (used by F5, potentially others)
    elif "groups" in pod_config and isinstance(pod_config["groups"], list):
        for group in pod_config["groups"]:
            if isinstance(group.get("component"), list):
                for comp in group["component"]:
                    if isinstance(comp, dict):
                        comp_name = comp.get("component_name")
                        # Check multiple keys, prioritizing 'clone_vm' then 'clone_name'
                        clone_patt = comp.get("clone_vm") or comp.get("clone_name") or comp.get("vm_name")
                        if comp_name and clone_patt:
                            details.append({"name": comp_name, "pattern": clone_patt})
    else:
        logger.warning(f"Could not find 'components' or 'groups' list in pod_config for pod/class identification.")

    if not details:
         logger.warning(f"No component details extracted from pod_config for pod {pod_config.get('pod_number', 'N/A')} class {pod_config.get('class_number', 'N/A')}")
    return details

# Updated signature to accept pod_config dictionary
def perform_vm_operations(
    service_instance: object, # Keep vague type hint or use VCenter if available
    pod_config: Dict[str, Any],
    operation: str,
    selected_components: Optional[List[str]] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Performs power operations (start, stop, reboot) on VMs within a pod's resource pool.
    Filters by selected_components if provided. Accepts pod_config as a dictionary.
    """
    pod_number = pod_config.get("pod_number") # May be None for F5 class-level context
    class_number = pod_config.get("class_number") # Relevant for F5
    identifier = f"Pod {pod_number}" if pod_number is not None else f"Class {class_number}"
    if pod_number is not None and class_number is not None:
         identifier = f"Pod {pod_number} (Class {class_number})"

    resource_pool_name = None

    # --- Determine Resource Pool Name based on vendor and context ---
    vendor_code = pod_config.get("vendor_shortcode", "").lower()
    host_fqdn = pod_config.get("host_fqdn", "") # Get host FQDN if available
    host_short = host_fqdn.split('.')[0] if host_fqdn else "" # Get short host name (e.g., 'ho', 'k2')

    # Use more specific RP naming based on vendor conventions seen in setup/teardown
    if vendor_code == "cp":
        # Check for maestro course name pattern
        if "maestro" in pod_config.get("course_name", "").lower():
             resource_pool_name = f"cp-maestro-pod{pod_number}"
        else:
             resource_pool_name = f"cp-pod{pod_number}"
    elif vendor_code == "pa":
        # PA labs might use different RPs depending on course/host
        if "1110" in pod_config.get("course_name", "").lower():
            resource_pool_name = f"pa-pod{pod_number}" # Example for 1110
        elif "cortex" in pod_config.get("course_name", "").lower():
            # Cortex might use component names directly as RPs with host suffix
             logger.warning(f"Manage operation for Cortex needs specific component target, resource pool determination might be complex.")
             # Need to determine target RP based on selected_components if provided
             # For now, assume a default or require component selection for Cortex manage?
             # Setting to None, logic below will fail if component isn't selected.
             resource_pool_name = None
        elif "1100" in pod_config.get("course_name", "").lower() and host_short:
             # 1100 courses seem to use group_name + host suffix
             # This is complex for 'manage' as group_name isn't directly known here.
             # We need selected_components to narrow down.
             logger.warning(f"Manage operation for 1100 needs specific component target to determine RP.")
             resource_pool_name = None
        else:
             resource_pool_name = f"pa-pod{pod_number}" # Default guess
    elif vendor_code == "f5" and class_number is not None:
        # F5 operations typically target VMs within the class RP or group RPs
        # Need selected_components to target specific VMs or group RPs
        # Defaulting to class RP, but filtering VMs below is key
        resource_pool_name = f"f5-class{class_number}"
    elif vendor_code == "av":
        if "ipo" in pod_config.get("course_name", "").lower():
             resource_pool_name = f"av-ipo-pod{pod_number}"
        elif "aura" in pod_config.get("course_name", "").lower():
            resource_pool_name = f"av-pod{pod_number}" # Example for Aura
        # Add AEP logic if needed
    elif vendor_code == "nu" and host_short:
         resource_pool_name = f"nu-pod{pod_number}-{host_short}"
    elif vendor_code == "pr":
        parent_rp = pod_config.get("group", "pr") # Get group from config if available
        resource_pool_name = f"{parent_rp}-pod{pod_number}"
    # Add other vendor logic if necessary

    if not resource_pool_name and not selected_components: # Allow skipping RP find if specific components will be searched globally
         err_msg = f"Could not determine resource pool name for {identifier} (vendor: {vendor_code}). Component selection might be required."
         logger.error(err_msg)
         return False, "determine_rp_name", err_msg
    elif resource_pool_name:
        logger.info(f"Targeting resource pool '{resource_pool_name}' for {identifier}.")

    logger.info(f"Performing '{operation}' on {identifier}")
    if selected_components:
        logger.info(f"Filtering for components: {', '.join(selected_components)}")


    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    rp_obj = None
    if resource_pool_name: # Try to find RP if name was determined
        rp_obj = rpm.get_obj([vim.ResourcePool], resource_pool_name)
        if not rp_obj:
            # Don't fail immediately, maybe we are targeting specific VMs by name
            logger.warning(f"Resource pool '{resource_pool_name}' not found for {identifier}. Will search for VMs globally.")
            # return False, "find_resource_pool", err_msg

    # --- Get Component Details from Config ---
    all_component_details = _get_component_details(pod_config)
    if not all_component_details:
         # Don't fail here, maybe the VMs exist anyway? Log a warning.
         logger.warning(f"Could not extract component details from config for {identifier}. Operation will target VMs by name directly.")
         # If selected_components is provided, we assume those ARE the VM names
         if selected_components:
              target_vm_names = selected_components
         else:
              # This case is ambiguous - no components in config AND no filter = operate on what?
              err_msg = f"Cannot determine target VMs: No components found in config and no specific components selected for {identifier}."
              logger.error(err_msg)
              return False, "get_target_vms", err_msg
    else:
        # --- Determine Target VM Names based on Selection ---
        target_vm_names = []
        for comp_detail in all_component_details:
            original_comp_name = comp_detail.get("name")
            pattern = comp_detail.get("pattern")

            if not original_comp_name or not pattern:
                logger.warning(f"Skipping component detail due to missing name or pattern: {comp_detail}")
                continue

            # Check if this component should be operated on
            operate_on_this = False
            if selected_components is None:
                operate_on_this = True # No filter, operate on all components from config
            elif original_comp_name in selected_components:
                operate_on_this = True # Filter provided, and this component name matches

            if operate_on_this:
                # Resolve the pattern to the final VM name
                vm_name = pattern
                if "{X}" in vm_name and pod_number is not None:
                    vm_name = vm_name.replace("{X}", str(pod_number))
                if "{Y}" in vm_name and class_number is not None:
                    vm_name = vm_name.replace("{Y}", str(class_number))
                # Add potential UUID substitution if needed (unlikely for manage)

                target_vm_names.append(vm_name)
                logger.debug(f"Adding '{vm_name}' (from component '{original_comp_name}') to target list.")
            # else: logger.debug(f"Skipping component '{original_comp_name}' based on filter.") # Less verbose


    if not target_vm_names:
        err_msg = f"No target VMs identified for operation '{operation}' on {identifier}."
        if selected_components:
             err_msg += f" Check if component name(s) '{', '.join(selected_components)}' exist in the config and resolve correctly for this pod/class."
        logger.warning(err_msg)
        # No actual error, just nothing matched the filter or config was empty.
        return True, "no_target_vms", err_msg # Return success, but indicate no VMs were targeted

    logger.info(f"Attempting '{operation}' on target VMs for {identifier}: {', '.join(target_vm_names)}")

    # --- Perform Operation ---
    overall_success = True
    failed_step = None
    error_message = None
    vms_found_rp = []
    vms_found_global = [] # Store VMs found globally if RP search fails or wasn't possible

    # Try to find VMs within the RP first if rp_obj exists
    if rp_obj:
        for vm_in_rp in rp_obj.vm:
            if vm_in_rp.name in target_vm_names:
                 vms_found_rp.append(vm_in_rp)
                 logger.debug(f"Found target VM '{vm_in_rp.name}' in RP '{resource_pool_name}'.")

    # Find any remaining target VMs globally if not found in RP or if RP didn't exist
    remaining_target_names = set(target_vm_names) - {vm.name for vm in vms_found_rp}
    if remaining_target_names:
        logger.info(f"Searching globally for remaining target VMs: {', '.join(remaining_target_names)}")
        for vm_name in remaining_target_names:
            vm_obj = vmm.get_obj([vim.VirtualMachine], vm_name)
            if vm_obj:
                vms_found_global.append(vm_obj)
                logger.debug(f"Found target VM '{vm_name}' globally.")
            else:
                 logger.warning(f"Target VM '{vm_name}' not found in RP '{resource_pool_name}' or globally. Skipping.")
                 # Mark as failure? Or just skip? Let's skip for now.
                 # overall_success = False; failed_step = "find_vm"; error_message = ...

    # Combine found VMs
    all_vms_to_operate = vms_found_rp + vms_found_global
    if not all_vms_to_operate:
        err_msg = f"None of the target VMs ({', '.join(target_vm_names)}) found in RP '{resource_pool_name}' or globally."
        logger.error(err_msg)
        return False, "find_vm", err_msg


    # Perform the operation on the found VMs
    logger.info(f"Performing '{operation}' on {len(all_vms_to_operate)} found VMs...")
    for vm in all_vms_to_operate:
        vm_name = vm.name # Get name for logging
        try:
            if operation == 'start':
                logger.debug(f"Starting VM '{vm_name}'...")
                # vmm.poweron_vm uses name, so call by name
                vmm.poweron_vm(vm_name)
                logger.info(f"VM '{vm_name}' start initiated/completed.")
            elif operation == 'stop':
                logger.debug(f"Stopping VM '{vm_name}'...")
                vmm.poweroff_vm(vm_name)
                logger.info(f"VM '{vm_name}' stop initiated/completed.")
            elif operation == 'reboot':
                logger.debug(f"Rebooting VM '{vm_name}'...")
                # Perform reboot by power off then power on
                vmm.poweroff_vm(vm_name) # Ensure it's off first
                vmm.poweron_vm(vm_name)  # Then power on
                logger.info(f"VM '{vm_name}' reboot initiated/completed.")
        except Exception as e:
            err_msg_vm = f"Operation '{operation}' failed for VM '{vm_name}': {vmm.extract_error_message(e)}"
            logger.error(err_msg_vm, exc_info=False) # Log less traceback by default
            overall_success = False
            failed_step = f"{operation}_vm"
            # Append specific VM error
            error_message = error_message + f"; {vm_name}: {e}" if error_message else f"{vm_name}: {e}"

    if not overall_success:
       final_err_msg = f"One or more VM operations failed for {identifier}. Errors: {error_message}"
       return False, failed_step, final_err_msg
    else:
        logger.info(f"Operation '{operation}' completed for all targeted VMs in {identifier}.")
        return True, None, None