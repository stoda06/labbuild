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

def start_vm(vm_manager, vm_name):
    vm_manager.logger.info(f"Starting {vm_name}")
    vm_manager.poweron_vm(vm_name)

def stop_vm(vm_manager, vm_name):
    vm_manager.logger.info(f"Stopping {vm_name}")
    vm_manager.poweroff_vm(vm_name)

def reboot_vm(vm_manager, vm_name):
    vm_manager.logger.info(f"Rebooting {vm_name}")
    vm_manager.poweroff_vm(vm_name)
    vm_manager.poweron_vm(vm_name)

def perform_vm_operations(service_instance, pod_config, operation):

    vm_manager = VmManager(service_instance)

    with ThreadPoolExecutor() as executor:
        futures = []
        for component in pod_config["components"]:
            if operation == "start":
                start_future = executor.submit(start_vm, vm_manager, component["clone_name"])
                futures.append(start_future)
            elif operation == "stop":
                stop_future = executor.submit(stop_vm, vm_manager,  component["clone_name"])
                futures.append(stop_future)
            elif operation == "reboot":
                reboot_future = executor.submit(reboot_vm, vm_manager,  component["clone_name"])
                futures.append(reboot_future)
        wait_for_task(futures)