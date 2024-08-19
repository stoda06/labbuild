from concurrent.futures import ThreadPoolExecutor, as_completed
from hosts.host import get_host_by_name
from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.resource_pool_manager import ResourcePoolManager


def update_network_dict(vm_name, network_dict, class_number, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')  # This ensures zero-padded two-digit hex
        return ':'.join(mac_parts)

    class_vs = f"vs{class_number}"

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']

        if 'rdp' in network_name and 'apm' not in vm_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        if 'mgt' in network_name and 'srv-bigip' not in vm_name:
            mac_address = replace_mac_octet(mac_address, pod_number)
        
        if 'vs0' in network_name:
            network_name = network_name.replace('vs0', class_vs)
        
        if 'bigip' in vm_name and not 'srv-bigip' in vm_name and ('ext' in network_name or 'int' in network_name):
            mac_address = replace_mac_octet(mac_address, pod_number)

        if 'w10' in vm_name and 'ext' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address
        }

    return updated_network_dict

def build_class(service_instance, hostname, class_number, course_config):
    host = get_host_by_name(hostname)
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    
    # Create resource pool for the pod.
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
    
    # Step-1: Create f5 pod network and assign user and permission.
    for network in course_config["network"]:
        switch_name = network['switch'] + class_number + "-" + "f5"
        nm.create_vswitch(host.fqdn, switch_name)
        nm.logger.info(f"Created vswitch {switch_name}.")
        updated_network_details = {
            k: [
                {
                    sub_k: (f"{sub_v}-{switch_name}" if sub_k == 'name' else sub_v)
                    for sub_k, sub_v in pg.items()
                }
                for pg in v
            ] if k == "port_groups" else v
            for k, v in network.items()
        }
        nm.create_vm_port_groups(host.fqdn, switch_name, updated_network_details["port_groups"])
        nm.logger.info(f"Created port groups on vswitch {switch_name}")
    # Step-2: Create class resource pool and assign user and permission.
    parent_resource_pool = course_config["vendor"] + "-" + host.name[:2]
    class_pool = course_config["class"] + str(class_number)
    rpm.create_resource_pool(parent_resource_pool, class_pool, cpu_allocation, memory_allocation)
    rpm.logger.info(f"Created resource pool {class_pool}.")
    # Step-3: Create group resource pools and assign user and permission.
    for group in course_config["group"]:
        group_pool = class_pool + "-" + group["name"]
        rpm.create_resource_pool(class_pool, group_pool, cpu_allocation, memory_allocation)
        rpm.logger.info(f"Created reosurce pool {group_pool}.")

def build_srv(service_instance, class_number, parent_resource_pool, components, rebuild=False, thread=4, linked=False):
    vmm = VmManager(service_instance)
    snapshot_name = 'base'
    for component in components:
        clone_name = component["clone_vm"]+class_number
        if rebuild:
            vmm.delete_vm(clone_name)
            vmm.logger.info(f"Deleted VM {clone_name}.")
        if linked:
            if not vmm.snapshot_exists(component["base_vm"], "base"):
                vmm.create_snapshot(component["base_vm"], "base", 
                                    description="Snapshot used for creating linked clones.")
            vmm.create_linked_clone(component["base_vm"], clone_name, 
                                    "base", parent_resource_pool)
            vmm.logger.info(f'Created linked clone {clone_name}.')
        else:
            vmm.clone_vm(component["base_vm"], clone_name, parent_resource_pool)
            vmm.logger.info(f'Created direct clone {clone_name}.')
        # Step-3.2: Update VR Mac adderess and VM networks.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict(clone_name, vm_network, int(class_number), int(class_number))
        vmm.update_vm_network(clone_name, updated_vm_network)
        vmm.logger.info(f'Updated VM {clone_name} networks.')
        if "bigip" in component["clone_vm"]:
            vmm.update_serial_port_pipe_name(clone_name, "Serial port 1",r"\\.\pipe\com_"+class_number)
            vmm.logger.info(f'Updated serial port pipe name on {clone_name}.')
        # Create a snapshot of all the cloned VMs to save base config.
        if not vmm.snapshot_exists(clone_name, snapshot_name):
            vmm.create_snapshot(clone_name, snapshot_name, 
                                description=f"Snapshot of {clone_name}")
    # Step-3.3: Power-on all VMs.
    vmm.logger.info(f'Power on all components in {parent_resource_pool}.')
    for component in components:
        with ThreadPoolExecutor(max_workers=thread) as executor:
            tasks = [
                executor.submit(vmm.poweron_vm, component["clone_vm"]+class_number)
                for component in components
                if "state" not in component or "poweroff" not in component["state"]
            ]
            for future in as_completed(tasks):
                try:
                    future.result()
                    vmm.logger.debug(f'VM powered on successfully.')
                except Exception as e:
                    vmm.logger.error(f'Error powering on VM: {e}')

def build_pod(service_instance, class_number, parent_resource_pool, components, pod_number, rebuild=False, thread=4, linked=False):
    vmm = VmManager(service_instance)
    snapshot_name = 'base'
    # Step-3.1: Clone components.
    for component in components:
        if 'w10' in component["clone_vm"]:
            clone_name = component["clone_vm"].format(X=pod_number)
        else:
            clone_name = component["clone_vm"] + pod_number
        if rebuild:
            vmm.delete_vm(clone_name)
            vmm.logger.info(f'Deleted VM {clone_name}.')
        if linked:
            if not vmm.snapshot_exists(component["base_vm"], "base"):
                vmm.create_snapshot(component["base_vm"], "base", 
                                    description="Snapshot used for creating linked clones.")
            vmm.create_linked_clone(component["base_vm"], clone_name, 
                                    "base", parent_resource_pool)
            vmm.logger.info(f'Created linked clone {clone_name}.')
        else:
            vmm.clone_vm(component["base_vm"], clone_name, parent_resource_pool)
            vmm.logger.info(f'Created direct clone {clone_name}.')

        # Step-3.2: Update VR Mac adderess and VM networks.
        vm_network = vmm.get_vm_network(component["base_vm"])
        update_vm_network = update_network_dict(clone_name, vm_network, class_number, int(pod_number))
        vmm.update_vm_network(clone_name,update_vm_network)
        vmm.logger.info(f'Updated VM {clone_name} networks.')
        if "bigip" in clone_name or "w10" in clone_name:
            vmm.update_serial_port_pipe_name(clone_name, "Serial port 1",r"\\.\pipe\com_"+pod_number)
            vmm.logger.info(f'Updated serial port pipe name on {clone_name}.')
            if "w10" in clone_name:
                drive_name = "CD/DVD drive 1"
                iso_type = "Datastore ISO file"
                datastore_name = "vms"
                iso_path = "podiso/pod-"+pod_number+"-a.iso"
                vmm.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True)
        # Create a snapshot of all the cloned VMs to save base config.
        if not vmm.snapshot_exists(clone_name, snapshot_name):
            vmm.create_snapshot(clone_name, snapshot_name, 
                                description=f"Snapshot of {clone_name}")

    # Step-3.3: Power-on all VMs.
    vmm.logger.info(f'Power on all components in {parent_resource_pool}.')
    for component in components:
        if 'w10' in component["clone_vm"]:
            clone_name = component["clone_vm"].format(X=pod_number)
        else:
            clone_name = component["clone_vm"] + pod_number
        with ThreadPoolExecutor(max_workers=thread) as executor:
            tasks = [
                executor.submit(vmm.poweron_vm, clone_name)
                for component in components
                if "state" not in component or "poweroff" not in component["state"]
            ]
            for future in as_completed(tasks):
                try:
                    future.result()
                    vmm.logger.debug(f'VM powered on successfully.')
                except Exception as e:
                    vmm.logger.error(f'Error powering on VM: {e}')

def teardown_class(service_instance, course_config, class_name, class_number):
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    if rpm.delete_resource_pool(class_name):
        rpm.logger.info(f'Deleted {class_name} successfully.')
    else: 
        rpm.logger.error(f'Failed to delete {class_name}.')
    for network in course_config["network"]:
        switch_name = network['switch'] + class_number + "-" + "f5"
        if nm.delete_vswitch(switch_name):
            nm.logger.info(f'Deleted switch {switch_name} successfully.')
        else:
            nm.logger.error(f'Failed to delete switch {switch_name}.')
