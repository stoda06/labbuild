from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager

def update_network_dict(vm_networks, base_vs, new_vs, new_mac):
    return {
        k: {
            sub_k: (
                sub_v.replace(base_vs, new_vs) if sub_k == 'network_name' else new_mac
            ) if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else (
                sub_v.replace(base_vs, new_vs) if sub_k == 'network_name' else sub_v
            )
            for sub_k, sub_v in v.items()
        }
        for k, v in vm_networks.items()
    }

def build_1100_220_pod(service_instance, host_details, pod_config, rebuild=False, linked=False):
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
                if linked:
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
                vm_network = vm_manager.get_vm_network(component["clone_name"])
                new_mac = "00:50:56:07:00:" + "{:02x}".format(pod)
                updated_vm_network = update_network_dict(vm_network, '1', str(pod), new_mac)
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

def build_1100_210_pod(service_instance, host_details, pod_config, rebuild=False, linked=False):
    vm_manager = VmManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    # Step-3: Clone VMs
    
    for component in pod_config["components"]:
        if rebuild:
            if "firewall" not in component["component_name"]:
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
        if "firewall" not in component["component_name"]:
            if linked:
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
            vm_network = vm_manager.get_vm_network(component["clone_name"])
            new_mac = "00:50:56:07:00:" + "{:02x}".format(100+pod)
            updated_vm_network = update_network_dict(vm_network, '1', str(pod), new_mac)
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

def build_cortex_pod(service_instance, host_details, pod_config, rebuild=False, linked=False):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    pod = int(pod_config["pod_number"])
    snapshot_name = "base"
    # Step-2: Create Network
    for network in pod_config["network"]:
        network_manager.create_vm_port_groups(host_details.fqdn,
                                                network["switch_name"],
                                                network["port_groups"],
                                                pod_number=pod)
    # Step-3: Clone VMs
    for component in pod_config["components"]:
        if rebuild:
            vm_manager.delete_vm(component["clone_name"])
        if host_details.name == "cliffjumper":
            resource_pool = component["component_name"] + "-cl"
        elif host_details.name == "apollo":
            resource_pool = component["component_name"] + "-ap"
        elif host_details.name == "nightbird":
            resource_pool = component["component_name"] + "-ni"
        elif host_details.name == "ultramagnus":
            resource_pool = component["component_name"] + "-ul"
        else:
            resource_pool = component["component_name"]
        if linked:
            vm_manager.create_linked_clone(component["base_vm"], component["clone_name"],
                                            "base", resource_pool)
        else:
            vm_manager.clone_vm(component["base_vm"], component["clone_name"], resource_pool)

        # Step-4: Update VM Network
        vm_network = vm_manager.get_vm_network(component["clone_name"])
        new_mac = "00:50:56:07:00:" + "{:02x}".format(100+pod)
        updated_vm_network = update_network_dict(vm_network, '0', str(pod), new_mac)
        vm_manager.update_vm_network(component["clone_name"], updated_vm_network)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
            vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                        description=f"Snapshot of {component['clone_name']}")
    
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        vm_manager.poweron_vm(component["clone_name"])