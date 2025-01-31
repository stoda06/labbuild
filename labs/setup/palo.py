from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager

def update_network_dict_1110(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to a two-digit hexadecimal string

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = details['network_name'].replace('pa-', 'pan-')
            if 'pan-internal-1' in details['network_name']:
                details['network_name'] = details['network_name'].replace('1', str(pod_number))
            details['network_name'] = details['network_name'].replace('0', str(pod_number))

    return network_dict

def update_network_dict_1100(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to a two-digit hexadecimal string

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = details['network_name'].replace('1', str(pod_number))

    return network_dict

def update_network_dict_cortex(network_dict, pod_number):
    pod_hex = format(100+pod_number, '02x')  # Convert pod number to hex with at least two digits

    for adapter, details in network_dict.items():
        if 'pa-rdp' in details['network_name']:
            mac_address_preset = '00:50:56:07:00:00'
            mac_parts = mac_address_preset.split(':')
            mac_parts[-1] = pod_hex
            details['mac_address'] = ':'.join(mac_parts)
        else:
            details['network_name'] = f"pa-internal-cortex-{pod_number}"

    return network_dict

def solve_vlan_id(port_groups):
    for group in port_groups:
        # Evaluate the expression in the vlan_id and update it as an integer
        group["vlan_id"] = eval(group["vlan_id"])
    return port_groups


def build_1100_220_pod(service_instance, host_details, pod_config, rebuild=False, full=False):
    vm_manager = VmManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    for component in pod_config["components"]:
        if rebuild:
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                vm_manager.delete_vm(component["clone_name"])
            else:
                vm_manager.poweroff_vm(component["vm_name"])

        if host_details.name == "cliffjumper":
            resource_pool = component["group_name"] + "-cl"
        elif host_details.name == "apollo":
            resource_pool = component["group_name"] + "-ap"
        elif host_details.name == "nightbird":
            resource_pool = component["group_name"] + "-ni"
        elif host_details.name == "ultramagnus":
            resource_pool = component["group_name"] + "-ul"
        else:
            resource_pool = component["group_name"]

        if not pod % 2:
            if "firewall" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
                continue
        else:
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                if not full:
                    vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
                    if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                        vm_manager.create_snapshot(component["base_vm"], "base", 
                                                description="Snapshot used for creating linked clones.")
                    vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], 
                                                "base", resource_pool)
                else:
                    vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)
            elif "firewall" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
            elif "panorama" in component["component_name"]:
                vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
            
            # Step-4: Update VM Network
            if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
                vm_network = vm_manager.get_vm_network(component["base_vm"])
                updated_vm_network = update_network_dict_1100(vm_network, int(pod))
                vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
                # Create a snapshot of all the cloned VMs to save base config.
                if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                    vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                                description=f"Snapshot of {component['clone_name']}")
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
            vm_manager.poweron_vm(component["clone_name"])
        else:
            vm_manager.poweron_vm(component["vm_name"])

def build_1100_210_pod(service_instance, host_details, pod_config, rebuild=False, full=False):
    vm_manager = VmManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    # Step-3: Clone VMs
    
    for component in pod_config["components"]:
        if rebuild:
            if "firewall" not in component["component_name"]:
                vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
                vm_manager.delete_vm(component["clone_name"])
            else:
                vm_manager.logger.info(f'Power-off VM {component["vm_name"]}')
                vm_manager.poweroff_vm(component["vm_name"])

        if host_details.name == "cliffjumper":
            resource_pool = component["group_name"] + "-cl"
        elif host_details.name == "apollo":
            resource_pool = component["group_name"] + "-ap"
        elif host_details.name == "nightbird":
            resource_pool = component["group_name"] + "-ni"
        elif host_details.name == "ultramagnus":
            resource_pool = component["group_name"] + "-ul"
        else:
            resource_pool = component["group_name"]
        if "firewall" not in component["component_name"]:
            if not full:
                vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
                if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                    vm_manager.create_snapshot(component["base_vm"], "base", 
                                            description="Snapshot used for creating linked clones.")
                vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], 
                                               "base", resource_pool)
            else:
                vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)
        else:
            vm_manager.revert_to_snapshot(component["vm_name"], component["snapshot"])
        
        # Step-4: Update VM Network
        if "firewall" not in component["component_name"]:
            # Update VM networks and MAC address.
            vm_network = vm_manager.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict_1100(vm_network, int(pod))
            vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
            # Create a snapshot of all the cloned VMs to save base config.
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                            description=f"Snapshot of {component['clone_name']}")
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        if "firewall" not in component["component_name"]:
            vm_manager.poweron_vm(component["clone_name"])
        else:
            vm_manager.poweron_vm(component["vm_name"])

def build_1110_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):

    vmm = VmManager(service_instance)
    nm = NetworkManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]

    for network in pod_config['networks']:
        nm.create_vswitch(pod_config["host_fqdn"], network["switch_name"])
        solved_port_groups = solve_vlan_id(network["port_groups"])
        nm.create_vswitch_portgroups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups)
        nm.logger.info(f'Created portgoups on {network["switch_name"]}.')

    group_name = f'pa-pod{pod_config["pod_number"]}'
    rpm.create_resource_pool("pa", group_name, host_fqdn=pod_config["host_fqdn"])
    rpm.logger.info(f'Created resource pool {group_name}')

    for component in components_to_build:
        if rebuild:
            vmm.logger.info(f'Deleting VM {component["clone_name"]}.')
            vmm.delete_vm(component["clone_name"])
        if not full:
            vmm.logger.info(f'Cloning linked component {component["clone_name"]}.')
            vmm.create_linked_clone(component["base_vm"], component["clone_name"], 
                                    "base", group_name)
        else:
            vmm.clone_vm(component["base_vm"], component["clone_name"], group_name)
        
        # Update VM networks and MAC address.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_1110(vm_network, int(pod))
        vmm.update_vm_network(component["clone_name"], updated_vm_network)
        if "firewall" in component["component_name"]:
                vmm.download_vmx_file(component["clone_name"],f"/tmp/{component['clone_name']}.vmx")
                vmm.update_vm_uuid(f"/tmp/{component['clone_name']}.vmx", component["uuid"])
                vmm.upload_vmx_file(component["clone_name"],f"/tmp/{component['clone_name']}.vmx")
                vmm.verify_uuid(component["clone_name"], component["uuid"])
        # Create a snapshot of all the cloned VMs to save base config.
        if not vmm.snapshot_exists(component["clone_name"], snapshot_name):
            vmm.create_snapshot(component["clone_name"], snapshot_name, 
                                description=f"Snapshot of {component['clone_name']}")
        # Step-5: Poweron VMs
    for component in components_to_build:
        vmm.poweron_vm(component["clone_name"])
    vmm.logger.info('Power-on all VMs.')

def build_cortex_pod(service_instance, pod_config, rebuild=False, full=False, selected_components=None):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    # Step-2: Create Network
    components_to_build = pod_config["components"]
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]
    if rebuild:
        for component in components_to_build:
            vm_manager.delete_vm(component["clone_name"])
        for network in pod_config["networks"]:
            solved_port_groups = solve_vlan_id(network["port_groups"])
            network_manager.delete_port_groups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups)

    for network in pod_config["networks"]:
        if not rebuild:
            solved_port_groups = solve_vlan_id(network["port_groups"])
        network_manager.create_vswitch_portgroups(pod_config["host_fqdn"],
                                                network["switch_name"],
                                                solved_port_groups)
        
    # Step-3: Clone VMs
    for component in components_to_build:
        if pod_config["host_fqdn"].split(".")[0] == "cliffjumper":
            resource_pool = component["component_name"] + "-cl"
        elif pod_config["host_fqdn"].split(".")[0] == "apollo":
            resource_pool = component["component_name"] + "-ap"
        elif pod_config["host_fqdn"].split(".")[0] == "nightbird":
            resource_pool = component["component_name"] + "-ni"
        elif pod_config["host_fqdn"].split(".")[0] == "ultramagnus":
            resource_pool = component["component_name"] + "-ul"
        elif pod_config["host_fqdn"].split(".")[0] == "unicron":
            resource_pool = component["component_name"] + "-un"
        else:
            resource_pool = component["component_name"]
        if not full:
            vm_manager.create_linked_clone(component["base_vm"], component["clone_name"],
                                            "base", resource_pool)
        else:
            vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)

        # Step-4: Update VM Network
        vm_network = vm_manager.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict_cortex(vm_network, pod)
        vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
        vm_manager.connect_networks_to_vm(component["clone_name"], updated_vm_network)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
            vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                        description=f"Snapshot of {component['clone_name']}")
    
    # Step-5: Poweron VMs
    for component in components_to_build:
        vm_manager.poweron_vm(component["clone_name"])

def teardown_cortex(service_instance, pod_config):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)

    for component in pod_config["components"]:
        vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
        vm_manager.delete_vm(component["clone_name"])

    for network in pod_config["networks"]:
        network_manager.logger.info(f'Deleting port-groups from {network["switch_name"]} vswitch.')
        solved_port_groups = solve_vlan_id(network["port_groups"])
        network_manager.delete_port_groups(pod_config["host_fqdn"], network["switch_name"], solved_port_groups)

def teardown_1100(service_instance, pod_config):
    vm_manager = VmManager(service_instance)

    for component in pod_config["components"]:
        if "firewall" not in component["component_name"] and "panorama" not in component["component_name"]:
            vm_manager.logger.info(f'Deleting VM {component["clone_name"]}')
            vm_manager.delete_vm(component["clone_name"])
        else:
            vm_manager.logger.info(f'Power-off VM {component["vm_name"]}')
            vm_manager.poweroff_vm(component["vm_name"])

def teardown_1110(service_instance, pod_config):
    rpm = ResourcePoolManager(service_instance)
    nm = NetworkManager(service_instance)

    group_name = f'pa-pod{pod_config["pod_number"]}'

    for network in pod_config['networks']:
        solved_port_groups = solve_vlan_id(network["port_groups"])
        rpm.poweroff_all_vms(group_name)
        rpm.logger.info(f'Power-off all VMs in {group_name}')
        rpm.delete_resource_pool(group_name)
        rpm.logger.info(f'Removed resource pool {group_name} and all its VMs.')
        nm.delete_port_groups(pod_config['host_fqdn'], network["switch_name"], solved_port_groups)
        nm.logger.info(f'Deleted associated port groups from vswitch {network["switch_name"]}')