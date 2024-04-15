from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor

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

    futures = []
    # Step-2: Revert snapshot to base.
    with ThreadPoolExecutor() as executor:
        for component in pod_config["components"]:
            revert_futures = executor.submit(vm_manager.revert_to_snapshot,
                                      component["component_name"], 
                                      component["snapshot"])
            futures.append(revert_futures)
        wait_for_task(futures)
        futures.clear()

        # Step-3: Power on the VMs.
        for component in pod_config["components"]:
            power_futures = executor.submit(vm_manager.poweron_vm,
                                      component["component_name"])
            futures.append(power_futures)
        wait_for_task(futures)

def build_ipo_pod(service_instance, pod_config):
    
    # Step-1: Clone VMs.
    vm_manager = VmManager(service_instance)
    futures = []

    with ThreadPoolExecutor() as executor:
        vm_manager.logger.info(f"Begin cloning {pod_config['group']} components.")
        for component in pod_config["components"]:
            clone_future = executor.submit(vm_manager.clone_vm, 
                                           component["base_vm"], 
                                           component["clone_name"], 
                                           pod_config["group"])
            futures.append(clone_future)
        wait_for_task(futures)
        futures.clear()
        vm_manager.logger.info(f"Cloning {pod_config['group']} components completed.")

        # Step-2: Change VM UUIDs and MAC on VR.
        vm_manager.logger.info(f"Changing ipo VM UUIDs and Update MAC address on VR")
        for component in pod_config["components"]:
            if "77201" in component["component_name"]:
                # uuid = vm_manager.get_vm_uuid(component["base_vm"])
                uuid = component["uuid"]
                status_futures = executor.submit(vm_manager.change_vm_uuid, 
                                                 component["clone_name"],
                                                 uuid)
                futures.append(status_futures)
            if "vr" in component["component_name"]:
                pod = int(component["clone_name"].split("av-ipo-vr-")[1])
                vm_manager.update_mac_address(component["clone_name"],
                                              "Network adapter 1",
                                              "00:50:56:0f:00:" + "{:02x}".format(pod))
        wait_for_task(futures)
        futures.clear()

        # Step-3: Power-on VMs
        # vm_manager.logger.info(f"Begin poweron process.")
        # for component in pod_config["components"]:
        #     poweron_future = executor.submit(vm_manager.poweron_vm,
        #                     component["clone_name"])
        #     futures.append(poweron_future)
        # wait_for_task(futures)
        # futures.clear()