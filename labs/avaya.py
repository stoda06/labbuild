from managers.vm_manager import VmManager
from concurrent.futures import ThreadPoolExecutor

def wait_for_futures(futures):
        
        # Optionally, wait for all cloning tasks to complete and handle their results
        for future in futures:
            try:
                result = future.result()  # This will re-raise any exceptions caught in the task
                # Handle successful cloning result
                if result:
                    print(result)
            except Exception as e:
                # Handle cloning failure
                print(f"Cloning task failed: {e}")

def build_pod(service_instance, pod_config):
    
    vm_manager = VmManager(service_instance)

    # Step-2: Revert snapshot to base.
    with ThreadPoolExecutor() as executor:
        futures = []
        for component in pod_config["components"]:
            revert_futures = executor.submit(vm_manager.revert_to_snapshot,
                                      component["component_name"], 
                                      component["snapshot"])
            futures.append(revert_futures)
        wait_for_futures(futures)
        futures.clear()

    # Step-3: Power on the VMs.
    with ThreadPoolExecutor() as executor:
        for component in pod_config["components"]:
            power_futures = executor.submit(vm_manager.poweron_vm,
                                      component["component_name"])
            futures.append(power_futures)
        wait_for_futures(futures)
