from concurrent.futures import ThreadPoolExecutor, as_completed
from hosts.host import get_host_by_name
from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.resource_pool_manager import ResourcePoolManager

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
    for component in components:
        if rebuild:
            vmm.delete_vm(component["clone_vm"]+class_number)
            vmm.logger.info(f"Deleted VM {component['clone_vm']}.")
        if linked:
            if not vmm.snapshot_exists(component["base_vm"], "base"):
                vmm.create_snapshot(component["base_vm"], "base", 
                                    description="Snapshot used for creating linked clones.")
            vmm.create_linked_clone(component["base_vm"], component["clone_vm"]+class_number, 
                                    "base", parent_resource_pool)
            vmm.logger.info(f'Created linked clone {component["clone_vm"]+class_number}.')
        else:
            vmm.clone_vm(component["base_vm"], component["clone_vm"]+class_number, parent_resource_pool)
            vmm.logger.info(f'Created direct clone {component["clone_vm"]+class_number}.')
        # Step-3.2: Update VR Mac adderess and VM networks.
        vm_network = vmm.get_vm_network(component["clone_vm"]+class_number)
        specified_mac_address = "00:50:56:02:" + "{:02x}".format(int(class_number)) + ":0a" # Replace with the desired MAC address
        updated_vm_network = {
            k: {
                sub_k: (
                    sub_v.replace('0', str(class_number)) if sub_k == 'network_name' else specified_mac_address
                ) if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else (
                    sub_v.replace('0', str(class_number)) if sub_k == 'network_name' else sub_v
                )
                for sub_k, sub_v in v.items()
            }
            for k, v in vm_network.items()
        }
        vmm.update_vm_network(component["clone_vm"]+class_number, updated_vm_network)
        vmm.logger.info(f'Updated VM {component["clone_vm"]+class_number} networks.')
        if "bigip" in component["clone_vm"]:
            vmm.update_serial_port_pipe_name(component["clone_vm"]+class_number, "Serial port 1",r"\\.\pipe\com_"+class_number)
            vmm.logger.info(f'Updated serial port pipe name on {component["clone_vm"]+class_number}.')
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

def build_pod(service_instance, class_number, parent_resource_pool, components, pod, rebuild=False, thread=4, linked=False):
    vmm = VmManager(service_instance)
    # Step-3.1: Clone components.
    for component in components:
        if rebuild:
            vmm.delete_vm(component["clone_vm"]+pod)
            vmm.logger.info(f'Deleted VM {component["clone_vm"]+pod}.')
        if linked:
            if not vmm.snapshot_exists(component["base_vm"], "base"):
                vmm.create_snapshot(component["base_vm"], "base", 
                                    description="Snapshot used for creating linked clones.")
            vmm.create_linked_clone(component["base_vm"], component["clone_vm"]+pod, 
                                    "base", parent_resource_pool)
            vmm.logger.info(f'Created linked clone {component["clone_vm"]+pod}.')
        else:
            vmm.clone_vm(component["base_vm"], component["clone_vm"]+pod, parent_resource_pool)
            vmm.logger.info(f'Created direct clone {component["clone_vm"]+pod}.')
        # Step-3.2: Update VR Mac adderess and VM networks.
        vm_network = vmm.get_vm_network(component["clone_vm"]+pod)
        specified_mac_address = "00:50:56:02:" + "{:02x}".format(int(class_number)) + ":0a" # Replace with the desired MAC address
        updated_vm_network = {
            k: {
                sub_k: (
                    sub_v.replace('0', str(class_number)) if sub_k == 'network_name' else specified_mac_address
                ) if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else (
                    sub_v.replace('0', str(class_number)) if sub_k == 'network_name' else sub_v
                )
                for sub_k, sub_v in v.items()
            }
            for k, v in vm_network.items()
        }
        vmm.update_vm_network(component["clone_vm"]+pod, updated_vm_network)
        vmm.logger.info(f'Updated VM {component["clone_vm"]+pod} networks.')
        if "bigip" in component["clone_vm"] or "w10" in component["clone_vm"]:
            vmm.update_serial_port_pipe_name(component["clone_vm"]+pod, "Serial port 1",r"\\.\pipe\com_"+pod)
            vmm.logger.info(f'Updated serial port pipe name on {component["clone_vm"]+pod}.')
    # Step-3.3: Power-on all VMs.
    vmm.logger.info(f'Power on all components in {parent_resource_pool}.')
    for component in components:
        with ThreadPoolExecutor(max_workers=thread) as executor:
            tasks = [
                executor.submit(vmm.poweron_vm, component["clone_vm"]+pod)
                for component in components
                if "state" not in component or "poweroff" not in component["state"]
            ]
            for future in as_completed(tasks):
                try:
                    future.result()
                    vmm.logger.debug(f'VM powered on successfully.')
                except Exception as e:
                    vmm.logger.error(f'Error powering on VM: {e}')