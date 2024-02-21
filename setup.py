from managers.resource_pool_manager import ResourcePoolManager
from managers.folder_manager import FolderManager
from managers.network_manager import NetworkManager
from managers.vm_manager import VmManager
from managers.vcenter import VCenter
from dotenv import load_dotenv
import os
import json
import time

load_dotenv()

def load_setup_template(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def get_setup_config(setup_name):
    setup_template = load_setup_template('setup_template.json')
    setup_config = setup_template.get(setup_name)
    if setup_config is None:
        print(f"Setup {setup_name} not found.")
        return None
    return setup_config

def replace_placeholder(setup_config, value):
    # Convert the network map dictionary to a JSON string
    setup_config_str = json.dumps(setup_config)

    # Replace the placeholder in the string
    updated_setup_config_str = setup_config_str.replace("{X}", str(value))

    # Convert the updated string back to a dictionary
    updated_setup_config = json.loads(updated_setup_config_str)

    return updated_setup_config

if __name__ == "__main__":
    vc_host = "vcenter-appliance-2.rededucation.com"
    vc_user = os.getenv("VC_USER"); print(vc_user)
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    vc = VCenter(vc_host, vc_user, vc_password, vc_port)
    vc.connect()

    # User inputs
    host_name = "ultramagnus.rededucation.com"
    course_name = "CCSA-R81.20"
    parent_group_name = "cp-ultramagnus"
    parent_folder_name = "cp"
    start_pod = 55
    end_pod = 55

    course_config = get_setup_config(course_name)

    # Capture the start time with higher precision
    start_time = time.perf_counter()

    if course_config:
        for pod in range(start_pod, end_pod+1):
            pod_config = replace_placeholder(course_config, pod)

            # Create resource pool for the pod.
            print(f"Creating resource pool for pod: {pod}")
            resource_pool_manager = ResourcePoolManager(vc)
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
            group_name = pod_config["group_name"]
            resource_pool_manager.create_resource_pool(parent_group_name, group_name, 
                                                       cpu_allocation, memory_allocation)
            print(f"Resource pool created for pod: {pod}")
            # print(f"Assign user {pod_config["user"]} and role {pod_config["role"]}")
            resource_pool_manager.assign_role_to_resource_pool(group_name, 
                                                               pod_config["domain"]+"\\"+pod_config["user"], 
                                                               pod_config["role"])
            # Resource pool created


            # Create folder for the pod.
            print(f"Creating folder for pod {pod}")
            folder_manager = FolderManager(vc)
            folder_manager.create_folder(parent_folder_name, pod_config["folder_name"])
            print(f"Folder created.")
            # print(f"Assign user {pod_config["user"]} and role {pod_config["role"]} for pod {pod}")
            folder_manager.assign_user_to_folder(pod_config["folder_name"],
                                                 pod_config["domain"]+"\\"+pod_config["user"],
                                                 pod_config["role"])
            print("Permissions applied.")


            # Create network, vswitch and port groups.
            # print(f"Creating vswitch {pod_config["network"]["switch_name"]} on host {host_name}")
            network_manager = NetworkManager(vc)
            network_manager.create_vswitch(host_name, pod_config["network"]["switch_name"])
            print(f"vSwitch created on {host_name}")
            network_manager.create_vm_port_groups(pod_config["network"]["switch_name"],
                                                  pod_config["network"]["port_groups"])
            network_names = [pg["port_group_name"] for pg in pod_config["network"]["port_groups"]]
            network_manager.apply_user_role_to_networks(pod_config["domain"]+"\\"+pod_config["user"],
                                                        pod_config["role"], network_names)
            

            # Clone VM
            vm_manager = VmManager(vc)
            for component in pod_config["components"]:
                vm_manager.clone_vm(component["base_vm"], component["clone_name"], 
                                    pod_config["group_name"], pod_config["folder_name"], datastore_name="vms")
                
                vm_manager.update_vm_networks(component["clone_name"],pod_config["folder_name"],component["network_map"])
            
                if component["base_vm"] == "cp-R81.20-vr":
                    vm_manager.update_mac_address(component["clone_name"], 
                                                  "Network adapter 1", 
                                                  "00:50:56:04:00:" + hex(pod)[2:])
    
    # Capture the end time with higher precision
    end_time = time.perf_counter()

    # Calculate the duration in seconds
    duration_seconds = end_time - start_time

    # Convert seconds to minutes
    duration_minutes = duration_seconds / 60

    print(f"The program took {duration_minutes:.2f} minutes to run.")
    # 13.40 minutes