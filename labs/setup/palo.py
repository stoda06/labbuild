from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager
from concurrent.futures import ThreadPoolExecutor


def build_1100_210_pod(service_instance, pod_config):
    vm_manager = VmManager(service_instance)

def build_cortex_pod(service_instance, host_details, pod_config, datastore="vms", rebuild=False, linked=False):
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
        vm_manager.update_vm_networks(component["clone_name"], pod)
        # Update MAC address on the VR with the pod number with HEX base.
        if "vr" in component["clone_name"]:
            new_mac = "00:50:56:07:00:" + "{:02x}".format(100+pod)
            vm_manager.update_mac_address(component["clone_name"], 
                                            "Network adapter 1", 
                                            new_mac)

        # Create a snapshot of all the cloned VMs to save base config.
        if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
            vm_manager.create_snapshot(component["clone_name"], snapshot_name, 
                                        description=f"Snapshot of {component['clone_name']}")
    
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        vm_manager.poweron_vm(component["clone_name"])