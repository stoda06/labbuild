from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.permission_manager import PermissionManager
from monitor.prtg import PRTGManager

def update_network_dict_1110(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to a two-digit hexadecimal string

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:05:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = details['network_name'].replace('0', str(pod_number))

    return network_dict

def build_nu_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):

    # Build Resource pool.
    rpm = ResourcePoolManager(service_instance)
    vmm = VmManager(service_instance)
    pod = pod_config["pod_number"]
    if not "hotshot" in pod_config["host_fqdn"]:
        parent_resource_pool = f'{pod_config["vendor_shortcode"]}-{pod_config["host_fqdn"][0:2]}'
    else:
        parent_resource_pool = f'{pod_config["vendor_shortcode"]}'
    resource_pool = f'nu-pod{pod_config["pod_number"]}-{pod_config["host_fqdn"].split(".")[0]}'
    snapshot_name = 'base'

    rpm.create_resource_pool(parent_resource_pool, resource_pool)
    components_to_build = pod_config["components"]

    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]

    for component in components_to_build:
        if rebuild:
            vmm.logger.info(f'Deleting VM {component["clone_name"]}.')
            vmm.delete_vm(component["clone_name"])
        if not full:
            vmm.logger.info(f'Cloning linked component {component["clone_name"]}.')
            vmm.create_linked_clone(component["base_vm"], component["clone_name"], 
                                    "base", resource_pool)
        else:
            vmm.clone_vm(component["base_vm"], component["clone_name"], resource_pool)
        
        # Update VM networks and MAC address.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_1110(vm_network, int(pod))
        vmm.update_vm_network(component["clone_name"], updated_vm_network)
        vmm.connect_networks_to_vm(component["clone_name"], updated_vm_network)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vmm.snapshot_exists(component["clone_name"], snapshot_name):
            vmm.create_snapshot(component["clone_name"], snapshot_name, 
                                description=f"Snapshot of {component['clone_name']}")
        
        if "state" not in component and "poweroff" not in component["state"]:
            vmm.poweron_vm(component["clone_name"])