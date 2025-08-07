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
    service_instance: object,
    pod_config: Dict[str, Any],
    operation: str,
    selected_components: Optional[List[str]] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Performs power operations (start, stop, reboot) on VMs within a pod's resource pool.

    This function is the core logic for the 'manage' command. It determines the target
    VMs based on the pod configuration and optional user-selected components, then
    executes the requested power operation on them.

    Args:
        service_instance: The active pyVmomi service instance connected to vCenter.
        pod_config: A dictionary containing the fully resolved course configuration
                    for a specific pod or class.
        operation: The power operation to perform ('start', 'stop', 'reboot').
        selected_components: An optional list of component names to filter the
                             operation. If None, all components are targeted.

    Returns:
        A tuple containing:
        - bool: True for success, False for failure.
        - str or None: The name of the step where failure occurred.
        - str or None: A detailed error message if failure occurred.
    """
    # --- 1. Initial Setup: Extract key identifiers from the pod_config ---
    pod_number = pod_config.get("pod_number")
    class_number = pod_config.get("class_number")
    vendor_code = pod_config.get("vendor_shortcode", "").lower()
    host_fqdn = pod_config.get("host_fqdn", "")
    host_short = host_fqdn.split('.')[0] if host_fqdn else ""
    course_name_lower = pod_config.get("course_name", "").lower()

    # Create a user-friendly identifier for logging.
    identifier = f"Pod {pod_number}" if pod_number is not None else f"Class {class_number}"
    if pod_number is not None and class_number is not None:
         identifier = f"Pod {pod_number} (Class {class_number})"

    resource_pool_name = None

    # --- 2. Determine the Target Resource Pool Name ---
    # This block contains vendor-specific logic to determine the name of the vCenter
    # Resource Pool (RP) where the target VMs are expected to be.
    if vendor_code == "cp":
        if "maestro" in course_name_lower:
             resource_pool_name = f"cp-maestro-pod{pod_number}"
        else:
             resource_pool_name = f"cp-pod{pod_number}"
             
    elif vendor_code == "pa":
        # --- CORRECTED LOGIC for Palo Alto courses ---
        if "cortex" in course_name_lower:
            # For Cortex, each component (VR, W10, etc.) is in its OWN resource pool.
            # There is no single parent RP for the whole pod.
            # We intentionally set resource_pool_name to None. This signals the logic
            # later in the function to find all VMs for this pod by name globally,
            # rather than looking inside a single RP.
            logger.info("Cortex course detected. Manage operation will target all pod VMs unless a specific component is selected.")
            resource_pool_name = None
            
        elif "1100" in course_name_lower:
            # For 1100-series labs, the RP is named after the component's GROUP.
            # Therefore, the user MUST specify exactly one component with the '-c' flag
            # so the code can look up its group name in the config.
            if not selected_components or len(selected_components) != 1:
                err_msg = "For 1100-series courses, 'manage' requires exactly one component to be specified via the -c flag to identify the correct resource pool."
                logger.error(err_msg)
                return False, "missing_component_for_1100", err_msg

            component_name_to_find = selected_components[0]
            group_name_for_rp = None
            
            # Find the component in the config to get its group name.
            for component_config in pod_config.get("components", []):
                if isinstance(component_config, dict) and component_config.get("component_name") == component_name_to_find:
                    group_name_for_rp = component_config.get("group_name")
                    break
            
            if not group_name_for_rp:
                err_msg = f"Could not find a 'group_name' in the course config for the selected component '{component_name_to_find}'."
                logger.error(err_msg)
                return False, "group_name_not_found", err_msg

            host_suffix_map = {"cliffjumper": "-cl", "apollo": "-ap", "nightbird": "-ni", "ultramagnus": "-ul"}
            host_suffix = host_suffix_map.get(host_short.lower(), "")
            resource_pool_name = f"{group_name_for_rp}{host_suffix}"
            
        elif "1110" in course_name_lower:
            resource_pool_name = f"pa-pod{pod_number}"
        else:
             resource_pool_name = f"pa-pod{pod_number}" # Default for other PA courses

    elif vendor_code == "f5" and class_number is not None:
        resource_pool_name = f"f5-class{class_number}"
    elif vendor_code == "av":
        if "ipo" in course_name_lower:
             resource_pool_name = f"av-ipo-pod{pod_number}"
        elif "aura" in course_name_lower:
            resource_pool_name = f"av-pod{pod_number}"
    elif vendor_code == "nu" and host_short:
         resource_pool_name = f"nu-pod{pod_number}-{host_short}"
    elif vendor_code == "pr":
        parent_rp = pod_config.get("group", "pr")
        resource_pool_name = f"{parent_rp}-pod{pod_number}"

    # --- 3. Validate Resource Pool or Component Selection ---
    # This is a critical validation step. If we couldn't determine a resource pool name
    # and the user didn't specify a component, we don't know what to target.
    # We add an exception for Cortex courses, because their resource_pool_name is
    # *intentionally* set to None to trigger the global VM search.
    is_cortex_course = (vendor_code == "pa" and "cortex" in course_name_lower)
    if not resource_pool_name and not selected_components and not is_cortex_course:
         err_msg = f"Could not determine resource pool for {identifier} (vendor: {vendor_code}). A specific component may be required."
         logger.error(err_msg)
         return False, "determine_rp_name", err_msg
    elif resource_pool_name:
        logger.info(f"Targeting resource pool '{resource_pool_name}' for {identifier}.")

    logger.info(f"Performing '{operation}' on {identifier}")
    if selected_components:
        logger.info(f"Filtering for components: {', '.join(selected_components)}")

    # --- 4. Initialize Managers and Determine Target VM Names ---
    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    
    all_component_details = _get_component_details(pod_config)
    target_vm_names = []
    
    # If the config was empty but the user specified components, assume component names are the VM names.
    if not all_component_details and selected_components:
        target_vm_names = selected_components
        logger.warning(f"No component details in config; using selected components as direct VM names: {target_vm_names}")
    else:
        # Determine which VMs to operate on based on the component list from the config.
        for comp_detail in all_component_details:
            original_comp_name = comp_detail.get("name")
            pattern = comp_detail.get("pattern")
            if not original_comp_name or not pattern: continue

            # If no components are selected by the user, target all. Otherwise, target only the selected ones.
            if selected_components is None or original_comp_name in selected_components:
                # Resolve the final VM name by replacing placeholders like {X} and {Y}.
                vm_name = pattern
                if "{X}" in vm_name and pod_number is not None:
                    vm_name = vm_name.replace("{X}", str(pod_number))
                if "{Y}" in vm_name and class_number is not None:
                    vm_name = vm_name.replace("{Y}", str(class_number))
                target_vm_names.append(vm_name)

    if not target_vm_names:
        err_msg = f"No target VMs identified for operation '{operation}' on {identifier}."
        logger.warning(err_msg)
        return True, "no_target_vms", err_msg

    # --- 5. Find VM Objects and Perform the Operation ---
    logger.info(f"Attempting '{operation}' on target VMs: {', '.join(target_vm_names)}")
    overall_success = True
    failed_step = None
    error_message = None
    all_vms_to_operate = []

    # First, try to find VMs inside the identified resource pool (more efficient).
    if resource_pool_name:
        rp_obj = rpm.get_obj([vim.ResourcePool], resource_pool_name)
        if rp_obj:
            for vm_in_rp in rp_obj.vm:
                if vm_in_rp.name in target_vm_names:
                     all_vms_to_operate.append(vm_in_rp)
        else:
            logger.warning(f"Resource pool '{resource_pool_name}' not found. Will search for VMs globally.")
    
    # Find any remaining VMs globally. This is the primary method for Cortex.
    found_vm_names = {vm.name for vm in all_vms_to_operate}
    remaining_target_names = set(target_vm_names) - found_vm_names
    
    if remaining_target_names:
        logger.info(f"Searching globally for remaining VMs: {', '.join(remaining_target_names)}")
        for vm_name in remaining_target_names:
            vm_obj = vmm.get_obj([vim.VirtualMachine], vm_name)
            if vm_obj:
                all_vms_to_operate.append(vm_obj)
            else:
                 logger.warning(f"Target VM '{vm_name}' not found globally. Skipping.")

    if not all_vms_to_operate:
        err_msg = f"None of the target VMs ({', '.join(target_vm_names)}) were found."
        logger.error(err_msg)
        return False, "find_vm", err_msg

    # Execute the requested operation on each found VM.
    for vm in all_vms_to_operate:
        try:
            if operation == 'start':
                vmm.poweron_vm(vm.name)
            elif operation == 'stop':
                vmm.poweroff_vm(vm.name)
            elif operation == 'reboot':
                vmm.poweroff_vm(vm.name)
                vmm.poweron_vm(vm.name)
        except Exception as e:
            # If one VM fails, log the error but continue with the others.
            err_msg_vm = f"Operation '{operation}' failed for VM '{vm.name}': {vmm.extract_error_message(e)}"
            logger.error(err_msg_vm)
            overall_success = False
            failed_step = f"{operation}_vm"
            error_message = (error_message + "; " if error_message else "") + err_msg_vm

    if not overall_success:
       return False, failed_step, f"One or more VM operations failed for {identifier}. Errors: {error_message}"
    
    logger.info(f"Operation '{operation}' completed for all targeted VMs in {identifier}.")
    return True, None, None