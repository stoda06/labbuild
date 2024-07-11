from hosts.host import get_host_by_name
from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor
from managers.network_manager import NetworkManager
from managers.folder_manager import FolderManager
from managers.resource_pool_manager import ResourcePoolManager
import sys

def wait_for_futures(futures):
        
    # Optionally, wait for all cloning tasks to complete and handle their results
    for future in futures:
        try:
            result = future.result()  # This will re-raise any exceptions caught in the task
            # Handle successful cloning result
        except Exception as e:
            # Handle cloning failure
            print(f"Task failed: {e}")

def build_cp_pod(service_instance, pod_config, hostname, pod, rebuild=False, thread=4, linked=False):

    host = get_host_by_name(hostname)
    vm_manager = VmManager(service_instance)
    folder_manager = FolderManager(service_instance)
    network_manager = NetworkManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)

    if rebuild:
        vm_manager.logger.info(f'P{pod} - Rebuild flag is enabled.')
        vm_manager.logger.info(f'P{pod} - Deleting folder {pod_config["folder_name"]}')
        vm_manager.delete_folder(pod_config["folder_name"], force=True)
        for network in pod_config['network']:
            network_manager.logger.info(f'P{pod} - Deleting vswitch {network["switch_name"]}')
            network_manager.delete_vswitch(host.fqdn, network['switch_name'])
        resource_pool_manager.logger.info(f'P{pod} - Deleting resource pool {pod_config["group_name"]}')
        resource_pool_manager.delete_resource_pool(pod_config["group_name"])
    
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
    try:
        resource_pool_manager.logger.info(f'P{pod} - Creating resource pool {pod_config["group_name"]}')
        resource_pool_manager.create_resource_pool(host.resource_pool, 
                                                pod_config["group_name"],
                                                cpu_allocation, 
                                                memory_allocation)
    except Exception as e:
        resource_pool_manager.logger.error(f"An error occurred: {e}")
        sys.exit(1)
    # Assign user and role to the created resource pool.
    resource_pool_manager.assign_role_to_resource_pool(pod_config["group_name"], 
                                                       pod_config["domain"]+"\\"+pod_config["user"], 
                                                       pod_config["role"])
    
    # Create pod folder
    folder_manager.logger.info(f'P{pod} - Creating folder {pod_config["folder_name"]}')
    folder_manager.create_folder(host.folder, pod_config['folder_name'])
    # Assign user and role to the created folder.
    folder_manager.assign_user_to_folder(pod_config["folder_name"],
                                         pod_config["domain"]+"\\"+pod_config["user"],
                                         pod_config["role"])
    
    # Create vSwitches
    network_manager.logger.info(f'P{pod} - Creating network')
    for network in pod_config['network']:
        network_manager.create_vswitch(host.fqdn, network['switch_name'])
        # Create necessary port groups/networks.
        network_manager.create_vm_port_groups(host.fqdn, network["switch_name"], network["port_groups"])
        # Assign user and role to created port groups/networks.
        network_names = [pg["port_group_name"] for pg in network["port_groups"]]
        network_manager.apply_user_role_to_networks(pod_config["domain"]+"\\"+pod_config["user"],
                                                    pod_config["role"], network_names)
        # Check if any of the created networks need to be set to promiscuous mode.
        if network['promiscuous_mode']:
            network_manager.enable_promiscuous_mode(host.fqdn, network['promiscuous_mode'])
    
    # Start cloning the required VMs simultaneously.
    for component in pod_config["components"]:
        vm_manager.logger.name = f'P{pod}'
        if linked:
            vm_manager.logger.info(f'Cloning linked component {component["clone_name"]}.')
            if not vm_manager.snapshot_exists(component["base_vm"], "base"):
                vm_manager.create_snapshot(component["base_vm"], "base", 
                                           description="Snapshot used for creating linked clones.")
            vm_manager.create_linked_clone(component["base_vm"], component["clone_name"], "base", 
                                            pod_config["group_name"], 
                                            directory_name=pod_config["folder_name"])
        else:
            vm_manager.logger.info(f'Cloning component {component["clone_name"]}.')
            vm_manager.clone_vm(component["base_vm"], component["clone_name"], 
                                pod_config["group_name"], directory_name=pod_config["folder_name"])
            
        vm_manager.logger.info(f'Updating VM networks for {component["clone_name"]}.')
        vm_network = vm_manager.get_vm_network(component["clone_name"])
        specified_mac_address = "00:50:56:04:00:" + "{:02x}".format(pod) # Replace with the desired MAC address
        updated_vm_network = {
            k: {
                sub_k: (
                    sub_v.replace('0', str(pod)) if sub_k == 'network_name' else specified_mac_address
                ) if 'rdp' in v.get('network_name', '') and sub_k == 'mac_address' else (
                    sub_v.replace('0', str(pod)) if sub_k == 'network_name' else sub_v
                )
                for sub_k, sub_v in v.items()
            }
            for k, v in vm_network.items()
        }
        vm_manager.update_vm_network(component["clone_name"], updated_vm_network)
        
    with ThreadPoolExecutor(max_workers=thread) as executor:
        futures = []
        vm_manager.logger.info(f'Power on all components.')
        for component in pod_config["components"]:
            # Schedule the VM cloning task
            if "state" in component:
                if "poweroff" in component["state"]:
                    continue
            poweron_future = executor.submit(
                vm_manager.poweron_vm,
                component["clone_name"]
            )
            futures.append(poweron_future)
        wait_for_futures(futures)

def teardown_pod(service_instance, pod_config, hostname):

    host = get_host_by_name(hostname)
    vm_manager = VmManager(service_instance)
    network_manager = NetworkManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)

    vm_manager.delete_folder(pod_config["folder_name"], force=True)
    for network in pod_config['network']:
        network_manager.delete_vswitch(host.fqdn, network['switch_name'])
    resource_pool_manager.delete_resource_pool(pod_config["group_name"])