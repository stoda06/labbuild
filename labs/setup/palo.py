from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager
from pyVmomi import vim

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
                vm_manager.update_vm_networks(component["clone_name"], "1100-210", pod)
                # Update MAC address on the VR with the pod number with HEX base.
                if "vr" in component["clone_name"]:
                    new_mac = "00:50:56:07:00:" + "{:02x}".format(pod)
                    vm_manager.update_mac_address(component["clone_name"], 
                                                    "Network adapter 1", 
                                                    new_mac)
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
            vm_network = vm_manager.get_vm_network(component["clone_name"]);print(vm_network)
            new_mac = "00:50:56:07:00:" + "{:02x}".format(100+pod)
            updated_vm_network = {
                k: {
                    sub_k: (
                        # If the network name contains 'rdp' and we are updating the mac_address
                        sub_v.replace('0', str(pod)) if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else (
                            # Specific case for network names containing '100'
                            sub_v[:-1] + str(pod) if '100' in sub_v and sub_k == 'network_name' else (
                                # General case of replacing all zeros in network_name with the pod number
                                sub_v.replace('0', str(pod)) if sub_k == 'network_name' else sub_v
                            )
                        )
                    ) if sub_k == 'network_name' or (sub_k == 'mac_address' and 'rdp' in v.get('network_name', '')) else (
                        # Ensuring mac_address is set to new_mac when the network name contains 'rdp'
                        new_mac if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else sub_v
                    )
                    for sub_k, sub_v in v.items()
                }
                for k, v in vm_network.items()
            }
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
        updated_vm_network = {
            k: {
                sub_k: (
                    sub_v.replace('0', str(pod)) if sub_k == 'network_name' else new_mac
                ) if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else (
                    sub_v.replace('0', str(pod)) if sub_k == 'network_name' else sub_v
                )
                for sub_k, sub_v in v.items()
            }
            for k, v in vm_network.items()
        }
        vm_manager.update_vm_network(component["clone_name"], updated_vm_network)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
            vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                        description=f"Snapshot of {component['clone_name']}")
    
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        vm_manager.poweron_vm(component["clone_name"])