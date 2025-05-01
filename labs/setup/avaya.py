from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from concurrent.futures import ThreadPoolExecutor
from time import sleep
import logging
import re

logger = logging.getLogger(__name__)

def wait_for_task(futures):
    # Optionally, wait for all cloning tasks to complete and handle their results
    for future in futures:
        try:
            result = future.result()  # This will re-raise any exceptions caught in the task
            # Handle successful cloning result
            if result:
                pass
        except Exception as e:
            # Handle cloning failure
            print(f"Task failed: {e}")


def update_network_dict(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to hex format

    def update_mac_address(mac_address):
        # Split the MAC address into octets
        mac_octets = mac_address.split(':')
        # Update the last octet with the hex value of the pod number
        mac_octets[-2] = pod_hex
        # Join the octets back into a MAC address
        return ':'.join(mac_octets)

    def update_network_name(network_name, pod_number):
        # Use regex to find and replace any "vs" followed by a number in the network name
        return re.sub(r'ipo-\d+', f'ipo-{pod_number}', network_name)

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']
        connected_at_power_on = details['connected_at_power_on']

        # Update the network name if it contains "vsX" (where X is any number)
        network_name = update_network_name(network_name, pod_number)

        # Update the MAC address if the network name contains "rdp"
        if 'rdp' in network_name:
            mac_address = update_mac_address(mac_address)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address,
            'connected_at_power_on': connected_at_power_on
        }

    return updated_network_dict

def build_aura_pod(service_instance, pod_config):
    
    vm_manager = VmManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)
    group_name = f'av-pod{pod_config["pod_number"]}'

    logger.info(f'Power off all the VMs in {group_name}')
    resource_pool_manager.poweroff_all_vms(group_name)

    futures = []
    # Step-2: Revert snapshot to base.
    with ThreadPoolExecutor() as executor:
        for component in pod_config["components"]:
            logger.info(f'Revert {component["component_name"]} snapshot to {component["snapshot"]}.')
            revert_futures = executor.submit(vm_manager.revert_to_snapshot,
                                      component["component_name"], 
                                      component["snapshot"])
            futures.append(revert_futures)
        wait_for_task(futures)
        futures.clear()

    # Step-3: Power on the VMs.
    for component in pod_config["components"]:
        logger.info(f'Power on {component["component_name"]}.')
        vm_manager.poweron_vm(component["component_name"])
        if "smgr" in component["component_name"] or "aads" in component["component_name"]:
            vm_manager.logger.info(f'Waiting for {component["component_name"]} to initialize.')
            sleep(600)

def build_ipo_pod(service_instance, pod_config, rebuild=False, selected_components=None, full=False): # Added full param for signature consistency
    """
    Builds Avaya IP Office Pods. Handles specific logic for 77202/78202 courses.
    """
    vm_manager = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod_number = pod_config.get("pod_number") # Use get for safety
    course_name = pod_config.get("course_name", "").lower()

    # Determine if it's one of the special courses
    is_special_ipo_course = "77202" in course_name or "78202" in course_name

    if is_special_ipo_course:
        logger.info(f"Detected special IPO course: {course_name}. Applying specific build logic.")

        # --- Logic for 77202/78202 Courses (Revert only) ---
        start_pod = pod_config.get("start_pod") # Get range start/end if passed via args_dict/orchestrator
        end_pod = pod_config.get("end_pod")

        # Determine target VMs based on pod range
        target_vms = []
        if start_pod == 0 and end_pod == 0:
            # Build "servers" only
            target_vms = pod_config.get("servers", [])
            logger.info("Pod range 0 specified. Targeting SERVER VMs for revert.")
            if not target_vms: logger.warning("No 'servers' defined in config for pod range 0."); return True, None, None # Nothing to do
        elif start_pod is not None and end_pod is not None and start_pod > 0:
            # Build "components" for the pod range
            logger.info(f"Targeting COMPONENT VMs for revert for pods {start_pod}-{end_pod}.")
            components_template = pod_config.get("components", [])
            if not components_template: logger.warning("No 'components' defined in config for pod range > 0."); return True, None, None # Nothing to do

            for pod_num in range(start_pod, end_pod + 1):
                 # Pad single-digit pod numbers with '0'
                 pod_num_str = str(pod_num).zfill(2) if pod_num < 10 else str(pod_num)
                 for comp_template in components_template:
                     # Replace {X} with the (padded) pod number string
                     vm_name = (comp_template.get("clone_name") or comp_template.get("component_name", "")).replace("{X}", pod_num_str)
                     snapshot = comp_template.get("snapshot")
                     if vm_name and snapshot:
                         # Store as dict similar to server structure for unified processing
                         target_vms.append({"server_name": vm_name, "snapshot": snapshot})
                     else:
                         logger.warning(f"Skipping component due to missing name/snapshot in template: {comp_template}")
        else:
            logger.error(f"Invalid pod range specified for special IPO course: start={start_pod}, end={end_pod}. Cannot determine targets.")
            return False, "invalid_pod_range", "Invalid pod range for special IPO course."

        if not target_vms:
             logger.warning(f"No target VMs identified for special IPO course build based on pod range {start_pod}-{end_pod}.")
             return True, None, None # Successfully did nothing

        logger.info(f"Identified {len(target_vms)} target VMs for poweroff/revert/poweron.")

        # Perform Power Off -> Revert -> Power On sequentially for safety
        # (Parallel could be added with careful state management if needed)
        all_success = True
        failed_vm = None
        failed_op = None

        for vm_info in target_vms:
            vm_name = vm_info.get("server_name") or vm_info.get("component_name") # Handle both keys
            snapshot = vm_info.get("snapshot")
            if not vm_name or not snapshot:
                logger.warning(f"Skipping invalid VM info: {vm_info}")
                continue

            logger.info(f"Processing VM: {vm_name}")
            # 1. Power Off
            logger.debug(f"Powering off {vm_name}...")
            if not vm_manager.poweroff_vm(vm_name):
                 logger.warning(f"Power off failed or VM already off for {vm_name}. Continuing...")
                 # Decide if this is critical? Let's continue for now.

            # Wait briefly after power off command before reverting
            sleep(5)

            # 2. Revert Snapshot
            logger.info(f"Reverting {vm_name} to snapshot '{snapshot}'...")
            if not vm_manager.revert_to_snapshot(vm_name, snapshot):
                logger.error(f"Failed to revert snapshot for {vm_name}.")
                all_success = False; failed_vm = vm_name; failed_op = "revert_snapshot"; break # Stop on critical failure

            # Wait briefly after revert before powering on
            sleep(5)

            # 3. Power On
            logger.info(f"Powering on {vm_name}...")
            if not vm_manager.poweron_vm(vm_name):
                logger.error(f"Failed to power on {vm_name} after revert.")
                all_success = False; failed_vm = vm_name; failed_op = "poweron_vm"; break # Stop on critical failure

            # Optional: Add delay like in build_aura_pod if needed for specific VMs
            # if "smgr" in vm_name or "aads" in vm_name: logger.info(f"Waiting for {vm_name}..."); sleep(600)

        if not all_success:
            return False, failed_op, f"Operation failed for VM {failed_vm}"
        else:
             logger.info(f"Successfully processed all target VMs for special IPO course {course_name}.")
             return True, None, None

    else:
        # --- Existing Logic for other IPO courses (Clone based) ---
        logger.info(f"Detected standard IPO course: {course_name}. Applying clone-based build logic.")
        group_name = f'av-ipo-pod{pod_number}' # Recalculate for clarity

        # Check if pod_number is valid for cloning logic
        if pod_number is None or pod_number <= 0:
             logger.error(f"Standard IPO build requires a pod number > 0. Got: {pod_number}")
             return False, "invalid_pod_number", "Pod number > 0 required for standard IPO build."

        # STEP 1: Create resource pool.
        if not rpm.create_resource_pool('av', group_name, pod_config.get("host_fqdn")): # Pass host FQDN if available
             return False, "create_resource_pool", f"Failed creating resource pool {group_name}"
        logger.info(f"Ensured resource pool '{group_name}' exists.")

        components_to_clone = pod_config.get("components", [])
        if selected_components:
            components_to_clone = [ c for c in components_to_clone if isinstance(c, dict) and c.get("component_name") in selected_components ]

        if not components_to_clone:
            logger.warning("No components selected or defined for standard IPO build.")
            return True, None, None # Successfully did nothing

        # --- Use ThreadPoolExecutor for Cloning/Configuration ---
        futures = []
        with ThreadPoolExecutor(max_workers=4) as executor: # Limit workers
            # Step 1.1: Rebuild - Delete existing VMs first (serially for safety?)
            if rebuild:
                logger.info(f"Rebuild requested. Deleting existing VMs in {group_name}...")
                delete_results = {}
                for component in components_to_clone:
                     clone_name = component.get("clone_name")
                     if clone_name:
                         logger.debug(f"Submitting delete task for {clone_name}")
                         # Submit delete tasks in parallel might be too risky? Let's do serially.
                         if not vm_manager.delete_vm(clone_name):
                              logger.error(f"Failed to delete {clone_name} during rebuild. Aborting.")
                              # Decide how to handle failure - Abort? Continue?
                              return False, "delete_vm_rebuild", f"Failed to delete {clone_name}"
                logger.info("Finished deleting existing VMs for rebuild.")


            # Step 2: Clone components
            logger.info(f"Starting cloning for {len(components_to_clone)} components...")
            clone_tasks = {}
            for component in components_to_clone:
                base_vm = component.get("base_vm")
                clone_name = component.get("clone_name")
                if not base_vm or not clone_name: continue

                if not full:
                    # Ensure base snapshot exists before submitting clone task
                    if not vm_manager.snapshot_exists(base_vm, "base"):
                        logger.info(f"Creating base snapshot for {base_vm}...")
                        if not vm_manager.create_snapshot(base_vm, "base", "Base snapshot"):
                             # Critical error if base snapshot cannot be created
                             logger.error(f"Cannot create base snapshot for {base_vm}. Aborting build.")
                             return False, "create_base_snapshot", f"Failed snapshot on {base_vm}"
                    # Submit linked clone task
                    future = executor.submit(vm_manager.create_linked_clone, base_vm, clone_name, "base", group_name)
                else:
                    # Submit full clone task
                    future = executor.submit(vm_manager.clone_vm, base_vm, clone_name, group_name)
                clone_tasks[future] = clone_name # Map future to name for error reporting

            # Wait for cloning to complete
            logger.info("Waiting for clone operations to complete...")
            cloning_successful = True
            for future in clone_tasks:
                 clone_name = clone_tasks[future]
                 try:
                     if not future.result(): # Check return value of clone function
                          logger.error(f"Cloning task returned failure for {clone_name}.")
                          cloning_successful = False; break # Stop if one fails
                 except Exception as e:
                     logger.error(f"Cloning task failed for {clone_name} with exception: {e}")
                     cloning_successful = False; break # Stop if one fails

            if not cloning_successful:
                # TODO: Cancel pending futures? Cleanup partially cloned VMs?
                return False, "clone_vm", f"Cloning failed for {clone_name}"
            logger.info("All cloning operations completed.")


            # Step 3 & 4: Configure Network, UUIDs, Snapshots, CD drive (can run in parallel per VM)
            logger.info("Starting post-clone configuration...")
            config_tasks = {}
            for component in components_to_clone:
                clone_name = component.get("clone_name")
                if not clone_name: continue
                # Submit configuration task for this VM
                future = executor.submit(configure_standard_ipo_vm, vm_manager, component, pod_config, group_name)
                config_tasks[future] = clone_name

            # Wait for configuration tasks
            logger.info("Waiting for configuration tasks to complete...")
            config_successful = True
            for future in config_tasks:
                 clone_name = config_tasks[future]
                 try:
                     if not future.result(): # Check return value of config function
                         logger.error(f"Configuration task returned failure for {clone_name}.")
                         config_successful = False; break
                 except Exception as e:
                     logger.error(f"Configuration task failed for {clone_name} with exception: {e}")
                     config_successful = False; break

            if not config_successful:
                 # TODO: Cleanup? Revert snapshots?
                 return False, "configure_vm", f"Configuration failed for {clone_name}"
            logger.info("All configuration tasks completed.")


            # Step-5: Power-on components
            logger.info(f"Starting power-on sequence...")
            poweron_tasks = {}
            for component in components_to_clone:
                 clone_name = component.get("clone_name")
                 if clone_name: # Check clone_name again
                     future = executor.submit(vm_manager.poweron_vm, clone_name)
                     poweron_tasks[future] = clone_name

            # Wait for power-on tasks
            logger.info("Waiting for power-on tasks to complete...")
            poweron_successful = True
            for future in poweron_tasks:
                 clone_name = poweron_tasks[future]
                 try:
                     if not future.result():
                          logger.error(f"Power-on task returned failure for {clone_name}.")
                          poweron_successful = False # Log error but maybe continue?
                 except Exception as e:
                     logger.error(f"Power-on task failed for {clone_name} with exception: {e}")
                     poweron_successful = False

            if not poweron_successful:
                 logger.warning("One or more VMs failed to power on.")
                 # Decide if this constitutes overall failure
                 # return False, "poweron_vm", "One or more VMs failed to power on"

        logger.info(f"Standard IPO pod {pod_number} build process completed.")
        return True, None, None


def configure_standard_ipo_vm(vm_manager: VmManager, component: dict, pod_config: dict, group_name: str) -> bool:
    """Helper function to configure a single standard IPO VM after cloning."""
    clone_name = component.get("clone_name")
    base_vm = component.get("base_vm")
    pod_number = pod_config.get("pod_number")
    host_fqdn = pod_config.get("host_fqdn")
    snapshot_name = "base"

    try:
        logger.debug(f"Configuring {clone_name}...")
        # 1. Update Network
        vm_network = vm_manager.get_vm_network(base_vm) # Get network from BASE VM
        if vm_network is None: raise Exception("Could not get base VM network info")
        updated_vm_network = update_network_dict(vm_network, int(pod_number))
        if not vm_manager.update_vm_network(clone_name, updated_vm_network): raise Exception("Failed update_vm_network")
        if not vm_manager.connect_networks_to_vm(clone_name, updated_vm_network): raise Exception("Failed connect_networks_to_vm")
        logger.debug(f"Network updated for {clone_name}")

        # 2. UUID Update (if applicable)
        if "77201" in component.get("component_name","") and component.get("uuid"):
            vmx_path = f"/tmp/{clone_name}.vmx"
            logger.debug(f"Updating UUID for {clone_name}...")
            if not vm_manager.download_vmx_file(clone_name, vmx_path): raise Exception("Failed download_vmx_file")
            if not vm_manager.update_vm_uuid(vmx_path, component["uuid"]): raise Exception("Failed update_vm_uuid")
            if not vm_manager.upload_vmx_file(clone_name, vmx_path): raise Exception("Failed upload_vmx_file")
            # Optional: Verification? vm_manager.verify_uuid(...)
            logger.debug(f"UUID updated for {clone_name}")

        # 3. CD Drive Mount (if applicable)
        if "w10" in component.get("component_name", ""):
             drive_name = "CD/DVD drive 1"; iso_type = "Datastore ISO file"; iso_path = f"podiso/pod-{pod_number}-a.iso"
             datastore_name = "datastore2-ho" if "hotshot" in host_fqdn else "keg2"
             logger.debug(f"Modifying CD drive for {clone_name}...")
             if not vm_manager.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True): raise Exception("Failed modify_cd_drive")
             logger.debug(f"CD drive modified for {clone_name}")

        # 4. Create Snapshot
        if not vm_manager.snapshot_exists(clone_name, snapshot_name):
            logger.debug(f"Creating snapshot '{snapshot_name}' for {clone_name}...")
            if not vm_manager.create_snapshot(clone_name, snapshot_name, description=f"Snapshot of {clone_name}"): raise Exception("Failed create_snapshot")
            logger.debug(f"Snapshot created for {clone_name}")

        logger.info(f"Successfully configured {clone_name}.")
        return True

    except Exception as e:
        logger.error(f"Configuration failed for {clone_name}: {e}")
        return False

def build_aep_pod(service_instance, pod_config, selected_components=None):
    
    vm_manager = VmManager(service_instance)

    if selected_components:
        # Filter components based on selected_components
        components_to_clone = [
            component for component in pod_config["components"]
            if component["component_name"] in selected_components
        ]

    for component in components_to_clone:
        vm_manager.poweroff_vm(component["clone_name"])
        if pod_config["type"] == "common":
            if "Student" not in component["component_name"]:
                vm_manager.revert_to_snapshot(component["clone_name"],component["snapshot"])
                vm_manager.poweron_vm(component["clone_name"])
        if pod_config["type"] == "student":
            if "Student" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["clone_name"],component["snapshot"])
                vm_manager.poweron_vm(component["clone_name"])
        
    
def teardown_ipo(service_instance, pod_config):
    """
    Tears down Avaya IP Office Pods.
    - For 77202/78202 courses: Powers off Server & Component VMs based on pod number.
    - For other IPO courses: Deletes the pod's resource pool and contained VMs.
    """
    vm_manager = VmManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)
    pod_number = pod_config.get("pod_number")
    course_name = pod_config.get("course_name", "").lower()

    if pod_number is None: # Pod number is essential for both scenarios
        logger.error("Missing pod_number for IPO teardown.")
        return False # Indicate failure

    # Determine if it's one of the special courses
    is_special_ipo_course = "77202" in course_name or "78202" in course_name

    if is_special_ipo_course:
        logger.info(f"Detected special IPO course '{course_name}' for teardown (power off only).")

        # --- Power Off Logic for 77202/78202 ---
        target_vm_names = []

        # 1. Identify Server VMs (assuming they follow a pattern or are listed)
        #    Using the 'servers' list from config seems appropriate
        servers_info = pod_config.get("servers", [])
        for server_info in servers_info:
             vm_name = server_info.get("server_name") # Or component_name if key differs
             if vm_name:
                 target_vm_names.append(vm_name)
             else:
                  logger.warning(f"Could not find server name in config: {server_info}")

        # 2. Identify Component VMs for this pod
        components_template = pod_config.get("components", [])
        if pod_number > 0: # Only look for component VMs if pod > 0
             pod_num_str = str(pod_number).zfill(2) if pod_number < 10 else str(pod_number) # Apply padding
             for comp_template in components_template:
                 vm_name_pattern = comp_template.get("clone_name") or comp_template.get("component_name")
                 if vm_name_pattern:
                     vm_name = vm_name_pattern.replace("{X}", pod_num_str)
                     target_vm_names.append(vm_name)
                 else:
                      logger.warning(f"Could not find component name/clone_name in template: {comp_template}")
        elif pod_number == 0 and not servers_info:
            # Handle edge case where pod is 0 but no servers are defined
             logger.warning("Pod number is 0, but no 'servers' defined in config to power off.")

        if not target_vm_names:
            logger.warning(f"No target VMs identified for power off for special IPO course '{course_name}', pod {pod_number}.")
            return True # Successfully did nothing? Or False because targets weren't found? Let's say True.

        logger.info(f"Attempting to power off {len(target_vm_names)} VMs for pod {pod_number}: {', '.join(target_vm_names)}")

        # Power off VMs (can run in parallel)
        all_success = True
        with ThreadPoolExecutor(max_workers=len(target_vm_names) or 1) as executor:
            future_to_vm = {executor.submit(vm_manager.poweroff_vm, vm_name): vm_name for vm_name in target_vm_names}
            for future in future_to_vm:
                vm_name = future_to_vm[future]
                try:
                    # poweroff_vm returns True on success/already off, None on error find
                    if future.result() is None:
                        # VM not found is not necessarily a failure for teardown poweroff
                        logger.warning(f"VM '{vm_name}' not found during power off attempt.")
                    # Add more specific error checking if poweroff_vm returns False on actual power-off failure
                except Exception as e:
                    logger.error(f"Error powering off VM {vm_name}: {e}")
                    all_success = False # Mark failure if any exception occurs

        if all_success:
            logger.info(f"Power off commands sent successfully for all target VMs in pod {pod_number}.")
            return True
        else:
            logger.error(f"One or more errors occurred during power off for pod {pod_number}.")
            return False # Indicate partial or full failure

    else:
        # --- Existing Logic for other IPO courses (Delete Resource Pool) ---
        logger.info(f"Detected standard IPO course '{course_name}' for teardown (delete RP).")
        group_name = f'av-ipo-pod{pod_number}'
        logger.info(f"Starting teardown for {group_name}...")

        # Power off all VMs in the RP first (safer before deletion)
        resource_pool_manager.poweroff_all_vms(group_name)
        logger.info(f"Power off command sent to VMs in {group_name}.")
        sleep(5) # Brief pause after power off command

        # Delete the entire resource pool (which deletes VMs inside)
        if resource_pool_manager.delete_resource_pool(group_name):
            logger.info(f"Successfully deleted resource pool {group_name}.")
            return True
        else:
            logger.error(f"Failed to delete resource pool {group_name}.")
            return False

def teardown_aura(service_instance, pod_config):
    rpm = ResourcePoolManager(service_instance)
    group_name = f'av-pod{pod_config["pod_number"]}'

    rpm.logger.info(f"Teardown {group_name} components.")
    rpm.poweroff_all_vms(group_name)