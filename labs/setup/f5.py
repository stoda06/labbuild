from concurrent.futures import ThreadPoolExecutor, as_completed
from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.resource_pool_manager import ResourcePoolManager
from pyVmomi import vim

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

        if 'li' in vm_name and 'int' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address
        }

    return updated_network_dict
            

def build_class(service_instance, class_config, rebuild=False, full=False, selected_components=None):
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    vmm = VmManager(service_instance)

    for network in class_config["networks"]:
        switch_name = network['switch']
        nm.create_vswitch(class_config["host_fqdn"], switch_name)
        nm.logger.info(f"Created vswitch {switch_name}.")

    class_pool = f'f5-class{class_config["class_number"]}'
    rpm.create_resource_pool(f'f5-{class_config["host_fqdn"][0:2]}', class_pool)
    rpm.logger.info(f'Created class resource pool {class_pool}.')
    for group in class_config["groups"]:
        group_pool = f'{class_pool}-{group["group_name"]}'
        rpm.create_resource_pool(class_pool, group_pool)
        rpm.logger.info(f'Created group resource pool {group_pool}.')
        if "srv" in group_pool:

            components_to_clone = group["component"]
            if selected_components:
                # Filter components based on selected_components
                components_to_clone = [
                    component for component in group["component"]
                    if component["component_name"] in selected_components
                ]

            for component in components_to_clone:
                clone_name = component["clone_vm"]
                if rebuild:
                    vmm.delete_vm(clone_name)
                    vmm.logger.info(f"Deleted VM {clone_name}.")
                if not full:
                    if not vmm.snapshot_exists(component["base_vm"], "base"):
                        vmm.create_snapshot(component["base_vm"], "base", 
                                            description="Snapshot used for creating linked clones.")
                    vmm.create_linked_clone(component["base_vm"], clone_name, 
                                            "base", group_pool)
                    vmm.logger.info(f'Created linked clone {clone_name}.')
                else:
                    vmm.clone_vm(component["base_vm"], clone_name, group_pool)
                    vmm.logger.info(f'Created direct clone {clone_name}.')
                
                vm_network = vmm.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict(clone_name, vm_network, int(class_config["class_number"]), int(class_config["class_number"]))
                vmm.update_vm_network(clone_name, updated_vm_network)
                vmm.logger.info(f'Updated VM {clone_name} networks.')
                if "bigip" in component["clone_vm"]:
                    vmm.update_serial_port_pipe_name(clone_name, "Serial port 1",r"\\.\pipe\com_"+str(class_config["class_number"]))
                    vmm.logger.info(f'Updated serial port pipe name on {clone_name}.')
                # Create a snapshot of all the cloned VMs to save base config.
                if not vmm.snapshot_exists(clone_name, "base"):
                    vmm.create_snapshot(clone_name, "base", 
                                        description=f"Snapshot of {clone_name}")
                
                if "state" not in component and "poweroff" not in component["state"]:
                    vmm.poweron_vm(component["clone_name"])

def build_pod(service_instance, pod_config, mem=None, rebuild=False, full=False, selected_components=None):
    vmm = VmManager(service_instance)
    snapshot_name = 'base'
    for group in pod_config["groups"]:
        pod_number = pod_config["pod_number"]
        class_number = pod_config["class_number"]
        pod_pool = f'f5-class{class_number}-{group["group_name"]}'
        if "srv" not in group["group_name"]:
            components_to_clone = group["component"]
            if selected_components:
                # Filter components based on selected_components
                components_to_clone = [
                    component for component in group["component"]
                    if component["component_name"] in selected_components
                ]
            # Step-3.1: Clone components.
            for component in components_to_clone:
                clone_name = component["clone_vm"]
                if rebuild:
                    vmm.delete_vm(clone_name)
                    vmm.logger.info(f'Deleted VM {clone_name}.')
                if not full:
                    if not vmm.snapshot_exists(component["base_vm"], "base"):
                        vmm.create_snapshot(component["base_vm"], "base", 
                                            description="Snapshot used for creating linked clones.")
                    vmm.create_linked_clone(component["base_vm"], clone_name, 
                                            "base", pod_pool)
                    vmm.logger.info(f'Created linked clone {clone_name}.')
                else:
                    vmm.clone_vm(component["base_vm"], clone_name, pod_pool)
                    vmm.logger.info(f'Created direct clone {clone_name}.')

                # Step-3.2: Update VR Mac adderess and VM networks.
                vm_network = vmm.get_vm_network(component["base_vm"])
                update_vm_network = update_network_dict(clone_name, vm_network, int(class_number), int(pod_number))
                vmm.update_vm_network(clone_name,update_vm_network)
                vmm.logger.info(f'Updated VM {clone_name} networks.')
                if "bigip" in clone_name or "w10" in clone_name:
                    vmm.update_serial_port_pipe_name(clone_name, "Serial port 1",r"\\.\pipe\com_"+pod_number)
                    vmm.logger.info(f'Updated serial port pipe name on {clone_name}.')
                    if "w10" in clone_name:
                        drive_name = "CD/DVD drive 1"
                        iso_type = "Datastore ISO file"
                        if vmm.get_obj([vim.Datastore], "keg2"):
                            datastore_name = "keg2" 
                        else:
                            datastore_name = "datastore2-ho"
                        iso_path = "podiso/pod-"+pod_number+"-a.iso"
                        vmm.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True)
                if 'bigip' in clone_name:
                    if mem:
                        vmm.reconfigure_vm_resources(clone_name, new_memory_size_mb=mem)
                        vmm.logger.info(f'Updated {clone_name} with memory {mem} MB.')
                    hex_pod_number = format(int(pod_number), '02x')
                    uuid = component["uuid"].replace('XX', str(hex_pod_number))
                    vmm.download_vmx_file(clone_name,f"/tmp/{clone_name}.vmx")
                    vmm.update_vm_uuid(f"/tmp/{clone_name}.vmx", uuid)
                    vmm.upload_vmx_file(clone_name, f"/tmp/{clone_name}.vmx")
                    vmm.verify_uuid(clone_name, uuid)
                # Create a snapshot of all the cloned VMs to save base config.
                if not vmm.snapshot_exists(clone_name, snapshot_name):
                    vmm.create_snapshot(clone_name, snapshot_name, 
                                        description=f"Snapshot of {clone_name}")
                
                if "state" not in component and "poweroff" not in component["state"]:
                    vmm.poweron_vm(component["clone_name"])


def teardown_class(service_instance, course_config):

    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)
    
    class_name = course_config["class_name"]

    rpm.poweroff_all_vms(class_name)
    rpm.logger.info(f'Power-off all VMs in {class_name}')

    if rpm.delete_resource_pool(class_name):
        rpm.logger.info(f'Deleted {class_name} successfully.')
    else: 
        rpm.logger.error(f'Failed to delete {class_name}.')

    for network in course_config["networks"]:
        switch_name = network['switch']
        if nm.delete_vswitch(course_config["host_fqdn"], switch_name):
            nm.logger.info(f'Deleted switch {switch_name} successfully.')
        else:
            nm.logger.error(f'Failed to delete switch {switch_name}.')