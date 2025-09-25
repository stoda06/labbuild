from managers.resource_pool_manager import ResourcePoolManager
from managers.vm_manager import VmManager
from pyVmomi import vim

import logging
logger = logging.getLogger(__name__)

# ... (update_network_dict function remains unchanged) ...
def update_network_dict(network_dict, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')
        return ':'.join(mac_parts)
    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']
        connected_at_power_on = details['connected_at_power_on']
        if 'rdp' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)
        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address,
            'connected_at_power_on': connected_at_power_on
        }
    return updated_network_dict


def build_pr_pod(service_instance, pod_config, rebuild=False, thread=4, full=False, selected_components=None):
    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod_number = int(pod_config["pod_number"])
    snapshot_name = 'base'
    components_to_build = pod_config["components"]
    
    vendor_shortcode = pod_config.get("vendor_shortcode", "pr")
    host_fqdn = pod_config.get("host_fqdn", "")
    # Correctly get the 2-letter host short name (e.g., 'un' from 'unicron')
    host_short_name = host_fqdn.split('.')[0][0:2] 
    
    # Correctly define the PARENT resource pool (e.g., 'pr-un')
    parent_resource_pool = f"{vendor_shortcode}-{host_short_name}"
    
    # --- THIS IS THE FIX ---
    # Construct the CHILD resource pool name independently of the parent.
    # It should be 'pr-podX', not 'pr-un-podX'.
    group_pool = f"{vendor_shortcode}-pod{pod_number}"
    # --- END OF FIX ---

    # Create the child pool under the correct parent
    if not rpm.create_resource_pool(parent_resource_pool, group_pool, host_fqdn=host_fqdn):
        return False, "create_resource_pool", f"Failed creating resource pool {group_pool}"

    if selected_components:
        components_to_build = [c for c in components_to_build if c["component_name"] in selected_components]

    overall_component_success = True
    component_errors = []
    successful_clones = []

    for component in components_to_build:
        try:
            clone_name = component["clone_name"]
            if rebuild:
                if not vmm.delete_vm(clone_name):
                    raise Exception(f"Rebuild failed: Could not delete VM {clone_name}")

            if not vmm.get_obj([vim.VirtualMachine], component["base_vm"]):
                raise Exception(f"Base VM '{component['base_vm']}' not found.")
            
            clone_successful = False
            if not full:
                if not vmm.snapshot_exists(component["base_vm"], snapshot_name) and not vmm.create_snapshot(component["base_vm"], snapshot_name, description="Base for linked clones"):
                    raise Exception(f"Failed to create base snapshot on {component['base_vm']}")
                clone_successful = vmm.create_linked_clone(component["base_vm"], clone_name, snapshot_name, group_pool)
            else:
                clone_successful = vmm.clone_vm(component["base_vm"], clone_name, group_pool)

            if not clone_successful:
                raise Exception("Clone operation failed")

            vm_network = vmm.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict(vm_network, pod_number)
            if not vmm.update_vm_network(clone_name, updated_vm_network) or \
               not vmm.connect_networks_to_vm(clone_name, updated_vm_network):
                raise Exception("Failed to configure network")

            if not vmm.create_snapshot(clone_name, snapshot_name, description=f"Snapshot of {clone_name}"):
                raise Exception("Failed to create snapshot on clone")

            iso_path = f"podiso/pod-{pod_number}-a.iso"
            
            host_datastore_map = {
                "ni": "datastore1-ni", "cl": "datastore1-cl",
                "ul": "datastore1-ul", "un": "datastore1-un",
                "ho": "datastore2-ho", "tr": "datastore2-tr"
            }
            
            datastore_name = host_datastore_map.get(host_short_name)
            
            if not datastore_name:
                raise Exception(f"Could not determine ISO datastore for host '{host_fqdn}'. No mapping found for short name '{host_short_name}'.")

            logger.info(f"Selected datastore '{datastore_name}' for ISO mount based on host '{host_fqdn}'.")
            
            if not vmm.modify_cd_drive(clone_name, "CD/DVD drive 1", "Datastore ISO file", datastore_name, iso_path, connected=True):
                raise Exception("Failed to modify CD drive")
            
            successful_clones.append(component)

        except Exception as e:
            error_msg = f"Component '{component.get('component_name', 'Unknown')}' failed: {e}"
            logger.error(error_msg, exc_info=True)
            component_errors.append(error_msg)
            overall_component_success = False
            continue

    power_on_failures = []
    for component in successful_clones:
        if component.get("state") != "poweroff":
            if not vmm.poweron_vm(component["clone_name"]):
                power_on_failures.append(component["clone_name"])
    
    if power_on_failures:
        error_msg = f"Failed to power on VMs: {', '.join(power_on_failures)}"
        component_errors.append(error_msg)
        overall_component_success = False

    if not overall_component_success:
        final_error_message = "; ".join(component_errors)
        return False, "component_build_failure", final_error_message

    return True, None, None


def teardown_pr_pod(service_instance, pod_config):
    rpm = ResourcePoolManager(service_instance)
    vendor_shortcode = pod_config.get("vendor_shortcode", "pr")
    
    # --- THIS IS THE FIX for teardown ---
    # Construct the resource pool name to be deleted using the correct convention.
    group_pool = f'{vendor_shortcode}-pod{pod_config["pod_number"]}'
    # --- END OF FIX ---
    
    rpm.poweroff_all_vms(group_pool)
    rpm.delete_resource_pool(group_pool)