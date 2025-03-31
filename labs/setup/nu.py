from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from monitor.prtg import PRTGManager
from tqdm import tqdm

def update_network_dict(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to a two-digit hexadecimal string
    network_name = f'nuvr-{pod_number}'

    for adapter, details in network_dict.items():
        if 'nu-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:05:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = network_name

    return network_dict


def build_nu_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):
    rpm = ResourcePoolManager(service_instance)
    vmm = VmManager(service_instance)
    pod = pod_config["pod_number"]
    snapshot_name = 'base'

    # Determine parent resource pool based on host_fqdn.
    if "hotshot" not in pod_config["host_fqdn"]:
        parent_resource_pool = f'{pod_config["vendor_shortcode"]}-{pod_config["host_fqdn"][0:2]}'
    else:
        parent_resource_pool = f'{pod_config["vendor_shortcode"]}'
    resource_pool = f'nu-pod{pod}-{pod_config["host_fqdn"][0:2]}'

    # STEP 1: Create resource pool.
    if not rpm.create_resource_pool(parent_resource_pool, resource_pool):
        return False, "create_resource_pool", f"Failed creating resource pool {resource_pool} under {parent_resource_pool}"

    # STEP 2: Filter components if needed.
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]

    # STEP 3: Process each component.
    for component in tqdm(components_to_build, desc=f"nu-pod{pod} → Building components", unit="comp"):
        # Rebuild: delete existing VM if needed.
        if rebuild:
            vmm.logger.info(f'Deleting VM {component["clone_name"]}.')
            if not vmm.delete_vm(component["clone_name"]):
                return False, "delete_vm", f"Failed deleting VM {component['clone_name']}"

        # Clone operation.
        if not full:
            vmm.logger.info(f'Creating linked clone for {component["clone_name"]}.')
            # Ensure base snapshot exists.
            if not vmm.snapshot_exists(component["base_vm"], "base"):
                if not vmm.create_snapshot(component["base_vm"], "base", description="Snapshot used for creating linked clones."):
                    return False, "create_snapshot", f"Failed creating snapshot on {component['base_vm']}"
            if not vmm.create_linked_clone(component["base_vm"], component["clone_name"], "base", resource_pool):
                return False, "create_linked_clone", f"Failed creating linked clone for {component['clone_name']}"
        else:
            if not vmm.clone_vm(component["base_vm"], component["clone_name"], resource_pool):
                return False, "clone_vm", f"Failed cloning VM for {component['clone_name']}"

        # STEP 4: Update VM networks and connect networks.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict(vm_network, int(pod))
        if not vmm.update_vm_network(component["clone_name"], updated_vm_network):
            return False, "update_vm_network", f"Failed updating network for {component['clone_name']}"
        if not vmm.connect_networks_to_vm(component["clone_name"], updated_vm_network):
            return False, "connect_networks_to_vm", f"Failed connecting networks for {component['clone_name']}"

        # STEP 5: Create snapshot on cloned VM.
        if not vmm.snapshot_exists(component["clone_name"], snapshot_name):
            if not vmm.create_snapshot(component["clone_name"], snapshot_name, description=f"Snapshot of {component['clone_name']}"):
                return False, "create_snapshot", f"Failed creating snapshot on {component['clone_name']}"

        # STEP 6: Power on VM if not set to poweroff.
        if component.get("state") != "poweroff":
            if not vmm.poweron_vm(component["clone_name"]):
                return False, "poweron_vm", f"Failed powering on {component['clone_name']}"

    return True, None, None


def teardown_nu_pod(service_instance, pod_config):
    rpm = ResourcePoolManager(service_instance)
    group_name = f'nu-pod{pod_config["pod_number"]}-{pod_config["host_fqdn"][0:2]}'

    rpm.poweroff_all_vms(group_name)
    rpm.logger.info(f'Power-off all VMs in {group_name}')
    rpm.delete_resource_pool(group_name)
    rpm.logger.info(f'Removed resource pool {group_name} and all its VMs.')