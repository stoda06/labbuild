from concurrent.futures import ThreadPoolExecutor, as_completed
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from managers.vm_manager import VmManager
from hosts.host import get_host_by_name
import json

cpu_allocation = {
    'limit': -1,
    'reservation': 0,
    'expandable_reservation': True,
    'shares': 4000
}
memory_allocation = {
    'limit': -1,
    'reservation': 0,
    'expandable_reservation': True,
    'shares': 163840
}


def update_network_dict(network_dict, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')  # This ensures zero-padded two-digit hex
        return ':'.join(mac_parts)

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']

        if 'rdp' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address
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
    rpm.create_resource_pool(parent_resource_pool, group_pool, cpu_allocation, memory_allocation)

    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]
    
    for component in components_to_build:
        clone_name = component["clone_name"]
        if rebuild:
            vmm.delete_vm(clone_name)
        if not full:
            vmm.create_linked_clone(component["base_vm"], clone_name, 
                                    snapshot_name, group_pool)
        else:
            vmm.clone_vm(component["base_vm"], clone_name, group_pool)
        
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict(vm_network, pod_number)
        vmm.update_vm_network(clone_name, updated_vm_network)
        vmm.connect_networks_to_vm(clone_name, updated_vm_network)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vmm.snapshot_exists(clone_name, snapshot_name):
            vmm.create_snapshot(clone_name, snapshot_name, 
                                description=f"Snapshot of {component['clone_name']}")
        
        if "2012" not in clone_name:
            drive_name = "CD/DVD drive 1"
            iso_type = "Datastore ISO file"
            datastore_name = "vms"
            iso_path = "podiso/pod-"+str(pod_number)+"-a.iso"
            vmm.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True)
        else:
            drive_name = "CD/DVD drive 1"
            iso_type = "Datastore ISO file"
            datastore_name = "keg2"
            iso_path = "podiso/pod-"+str(pod_number)+"-a.iso"
            vmm.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True)
