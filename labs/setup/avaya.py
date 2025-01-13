from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from concurrent.futures import ThreadPoolExecutor
from time import sleep
import re

def wait_for_task(futures):
    # Optionally, wait for all cloning tasks to complete and handle their results
    for future in futures:
        try:
            result = future.result()  # This will re-raise any exceptions caught in the task
            # Handle successful cloning result
            if result:
                pass
        except Exception as e:
            # Handle cloning failure
            print(f"Task failed: {e}")


def update_network_dict(network_dict, pod_number):
    pod_hex = format(pod_number, '02x')  # Convert pod number to hex format

    def update_mac_address(mac_address):
        # Split the MAC address into octets
        mac_octets = mac_address.split(':')
        # Update the last octet with the hex value of the pod number
        mac_octets[-1] = pod_hex
        # Join the octets back into a MAC address
        return ':'.join(mac_octets)

    def update_network_name(network_name, pod_number):
        # Use regex to find and replace any "vs" followed by a number in the network name
        return re.sub(r'ipo\d+', f'ipo-{pod_number}', network_name)

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']

        # Update the network name if it contains "vsX" (where X is any number)
        network_name = network_name.replace('ipo-1',f'ipo-{pod_number}')

        # Update the MAC address if the network name contains "rdp"
        if 'rdp' in network_name:
            mac_address = update_mac_address(mac_address)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address
        }

    return updated_network_dict

def build_aura_pod(service_instance, pod_config):
    
    vm_manager = VmManager(service_instance)
    resource_pool_manager = ResourcePoolManager(service_instance)
    resource_pool_manager.logger.info(f'Power off all the VMs in {pod_config["group"]}')
    resource_pool_manager.poweroff_all_vms(pod_config["group"])

    futures = []
    # Step-2: Revert snapshot to base.
    with ThreadPoolExecutor() as executor:
        for component in pod_config["components"]:
            vm_manager.logger.info(f'Revert {component["component_name"]} snapshot to {component["snapshot"]}.')
            revert_futures = executor.submit(vm_manager.revert_to_snapshot,
                                      component["component_name"], 
                                      component["snapshot"])
            futures.append(revert_futures)
        wait_for_task(futures)
        futures.clear()

    # Step-3: Power on the VMs.
    for component in pod_config["components"]:
        vm_manager.logger.info(f'Power on {component["component_name"]}.')
        vm_manager.poweron_vm(component["component_name"])
        if "smgr" in component["component_name"] or "aads" in component["component_name"]:
            vm_manager.logger.info(f'Waiting for {component["component_name"]} to initialize.')
            sleep(600)

def build_ipo_pod(service_instance, pod_config, pod, rebuild=False, selected_components=None):
    components_to_clone = pod_config["components"]
    if selected_components:
        # Filter components based on selected_components
        components_to_clone = [
            component for component in pod_config["components"]
            if component["component_name"] in selected_components
        ]
    # Step-1: Create an instance on VmManager.
    vm_manager = VmManager(service_instance)
    futures = []
    with ThreadPoolExecutor() as executor:
        # Step-1.1: If Reb-build flag is set, delete the existing components.
        if rebuild:
            vm_manager.logger.info(f"Teardown {pod_config['group']} components.")
            for component in components_to_clone:
                delete_future = executor.submit(vm_manager.delete_vm, 
                                                component["clone_name"])
                futures.append(delete_future)
            wait_for_task(futures)
            futures.clear()

        # Step-2: Start cloning the components mentioned in the pod_config.
        vm_manager.logger.info(f"Begin cloning {pod_config['group']} components.")
        for component in components_to_clone:
            clone_future = executor.submit(vm_manager.create_linked_clone, component["base_vm"], 
                                           component["clone_name"], "base", pod_config["group"])
            futures.append(clone_future)
        wait_for_task(futures)
        futures.clear()
        vm_manager.logger.info(f"Cloning {pod_config['group']} components completed.")

        # Step-3: Change VM UUIDs and MAC on VR.
        vm_manager.logger.info(f"Changing ipo VM UUIDs, Update MAC address on VR and change Networks on cloned VMs..")
        for component in pod_config["components"]:
            # Change VM Network
            vm_network = vm_manager.get_vm_network(component["base_vm"])
            updated_vm_network = update_network_dict(vm_network, int(pod))
            vm_manager.update_vm_network(component["clone_name"], updated_vm_network)

        wait_for_task(futures)
        futures.clear()

        # Step-4: Create a "base" snapshot on the cloned components.
        snapshot_name = "base"
        for component in components_to_clone:
            # Create a snapshot of all the cloned VMs to save base config.
            if not vm_manager.snapshot_exists(component["clone_name"], snapshot_name):
                snapshot_futures = executor.submit(
                    vm_manager.create_snapshot,
                    component["clone_name"],
                    snapshot_name,
                    description=f"Snapshot of {component['clone_name']}"
                )
                futures.append(snapshot_futures)
                        # Update IPO components' UUIDs
            if "77201" in component["component_name"]:
                vm_manager.download_vmx_file(component["clone_name"],f"/tmp/{component['clone_name']}.vmx")
                vm_manager.update_vm_uuid(f"/tmp/{component['clone_name']}.vmx", component["uuid"])
                vm_manager.upload_vmx_file(component["clone_name"],f"/tmp/{component['clone_name']}.vmx")
                # vm_manager.register_vm(component["clone_name"])
        wait_for_task(futures)
        futures.clear()

        # Step-5: Power-on all the components.
        vm_manager.logger.info(f"Begin poweron process.")
        for component in components_to_clone:
            poweron_future = executor.submit(vm_manager.poweron_vm,
                            component["clone_name"])
            futures.append(poweron_future)
        wait_for_task(futures)
        futures.clear()
    
def teardown_ipo(service_instance, pod_config):
    vm_manager = VmManager(service_instance)
    vm_manager.logger.info(f"Teardown {pod_config['group']} components.")
    futures = []
    with ThreadPoolExecutor() as executor:
        for component in pod_config["components"]:
            delete_future = executor.submit(vm_manager.delete_vm, 
                                            component["clone_name"])
            futures.append(delete_future)
        wait_for_task(futures)
    futures.clear()

def teardown_aura(service_instance, pod_config):
    rpm = ResourcePoolManager(service_instance)
    rpm.logger.info(f"Teardown {pod_config['group']} components.")
    rpm.poweroff_all_vms(pod_config["group"])