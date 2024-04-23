from managers.vm_manager import VmManager
from managers.network_manager import NetworkManager
from concurrent.futures import ThreadPoolExecutor

def wait_for_futures(futures):
        
    # Optionally, wait for all cloning tasks to complete and handle their results
    for future in futures:
        try:
            result = future.result()  # This will re-raise any exceptions caught in the task
            # Handle successful cloning result
        except Exception as e:
            # Handle cloning failure
            print(f"Task failed: {e}")

def build_1100_210_pod(service_instance, pod_config):
    vm_manager = VmManager(service_instance)

def build_cortex_pod(service_instance, host_details, pod_config, datastore="vms", rebuild=False):
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    pod = int(pod_config["pod_number"])
    # Step-2: Create Network
    for network in pod_config["network"]:
        network_manager.create_vm_port_groups(host_details.fqdn,
                                                network["switch_name"],
                                                network["port_groups"],
                                                pod_number=pod)
    # Step-3: Clone VMs
    with ThreadPoolExecutor() as executor:
        futures = []
        for component in pod_config["components"]:
            if rebuild:
                vm_manager.delete_vm(component["clone_name"])
            if host_details.name == "ultramagnus":
                resource_pool = component["component_name"] + "-ul"
            else:
                resource_pool = component["component_name"]
            clone_futures = executor.submit(
                vm_manager.clone_vm,
                component["base_vm"],
                component["clone_name"],
                resource_pool,
                datastore_name=datastore
            )
            futures.append(clone_futures)
        wait_for_futures(futures)
        futures.clear()

    # Step-4: Update VM Network
    with ThreadPoolExecutor() as executor:
        futures = []
        for component in pod_config["components"]:
            network_futures = executor.submit(
                vm_manager.update_vm_networks,
                component["clone_name"],
                pod
            )
            futures.append(network_futures)
            # Update MAC address on the VR with the pod number with HEX base.
            if "vr" in component["clone_name"]:
                new_mac = "00:50:56:07:00:" + "{:02x}".format(100+pod)
                vm_manager.update_mac_address(component["clone_name"], 
                                                "Network adapter 1", 
                                                new_mac)
        wait_for_futures(futures)
        futures.clear()

        snapshot_name = "base"
        for component in pod_config["components"]:
            # Create a snapshot of all the cloned VMs to save base config.
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                snapshot_futures = executor.submit(
                    vm_manager.create_snapshot,
                    component["clone_name"],
                    snapshot_name,
                    description=f"Snapshot of {component['clone_name']}"
                )
                futures.append(snapshot_futures)
        wait_for_futures(futures)
        futures.clear()
    
    # Step-5: Poweron VMs
    for component in pod_config["components"]:
        vm_manager.poweron_vm(component["clone_name"])