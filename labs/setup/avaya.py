from managers.vm_manager import VmManager
from managers.resource_pool_manager import ResourcePoolManager
from concurrent.futures import ThreadPoolExecutor
from time import sleep

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

def build_ipo_pod(service_instance, pod_config, pod, rebuild=False):
    
    # Step-1: Create an instance on VmManager.
    vm_manager = VmManager(service_instance)
    futures = []
    with ThreadPoolExecutor() as executor:
        # Step-1.1: If Reb-build flag is set, delete the existing components.
        if rebuild:
            vm_manager.logger.info(f"Teardown {pod_config['group']} components.")
            for component in pod_config["components"]:
                delete_future = executor.submit(vm_manager.delete_vm, 
                                                component["clone_name"])
                futures.append(delete_future)
            wait_for_task(futures)
            futures.clear()

        # Step-2: Start cloning the components mentioned in the pod_config.
        vm_manager.logger.info(f"Begin cloning {pod_config['group']} components.")
        for component in pod_config["components"]:
            clone_future = executor.submit(vm_manager.create_linked_clone, component["base_vm"], 
                                           component["clone_name"], pod_config["group"])
            futures.append(clone_future)
        wait_for_task(futures)
        futures.clear()
        vm_manager.logger.info(f"Cloning {pod_config['group']} components completed.")

        # Step-3: Change VM UUIDs and MAC on VR.
        vm_manager.logger.info(f"Changing ipo VM UUIDs, Update MAC address on VR and change Networks on cloned VMs..")
        for component in pod_config["components"]:
            # Change VM Network
            vm_manager.update_vm_networks(component["clone_name"], "ipo", pod)
            # Update VR MAC Address
            if "vr" in component["component_name"]:
                vm_manager.update_mac_address(component["clone_name"],
                                              "Network adapter 1",
                                              "00:50:56:0f:" + "{:02x}".format(pod) + ":00")

        wait_for_task(futures)
        futures.clear()

        # Step-4: Create a "base" snapshot on the cloned components.
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
        for component in pod_config["components"]:
            poweron_future = executor.submit(vm_manager.poweron_vm,
                            component["clone_name"])
            futures.append(poweron_future)
        wait_for_task(futures)
        futures.clear()