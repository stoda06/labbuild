from managers.resource_pool_manager import ResourcePoolManager
from managers.vm_manager import VmManager
from pyVmomi import vim

import logging
logger = logging.getLogger(__name__)

def update_network_dict(network_dict, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')  # This ensures zero-padded two-digit hex
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
    parent_resource_pool = pod_config["group"]
    group_pool = parent_resource_pool + "-pod" + str(pod_number)

    if not rpm.create_resource_pool(parent_resource_pool, group_pool):
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
            datastore_name = "keg2" if "k2" in pod_config.get("host_fqdn", "") else "datastore2-ho"
            if not vmm.modify_cd_drive(clone_name, "CD/DVD drive 1", "Datastore ISO file", datastore_name, iso_path, connected=True):
                raise Exception("Failed to modify CD drive")
            
            successful_clones.append(component)

        except Exception as e:
            error_msg = f"Component '{component.get('component_name', 'Unknown')}' failed: {e}"
            logger.error(error_msg)
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
    group_pool = f'pr-pod{pod_config["pod_number"]}'
    
    rpm.poweroff_all_vms(group_pool)
    rpm.delete_resource_pool(group_pool)