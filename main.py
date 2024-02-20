from managers.vcenter import VCenter
from managers.resource_pool_manager import ResourcePoolManager
from managers.network_manager import NetworkManager
from managers.vm_manager import VmManager
from managers.permission_manager import PermissionManager
from managers.folder_manager import FolderManager
from dotenv import load_dotenv
import os

load_dotenv()

def create_resource_pool(connection, parent_rp, child_rp):

    manager = ResourcePoolManager(connection)
    # Create a resource pool under a specific host
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

    manager.create_resource_pool(parent_rp, child_rp, cpu_allocation, memory_allocation)

    domain = "vcenter.rededucation.com"
    user = "labcp-55"
    role_name = "labcp-0-role"

    manager.assign_role_to_resource_pool(child_rp, domain + "\\" + user, role_name)

def create_folder(connection, parent_folder, child_folder, user, role):

    manager = FolderManager(connection)
    manager.create_folder(parent_folder, child_folder)
    # Apply permissions to the folder similar to the resource pool
    permission_manager = FolderManager(connection)
    domain = "vcenter.rededucation.com"
    user = "labcp-55"
    role_name = "labcp-0-role"
    permission_manager.assign_user_to_folder(child_folder, domain + "\\" + user, role_name)

def create_network(connection, host, vswitch, port_groups):

    manager = NetworkManager(connection)
    manager.create_vswitch(host, vswitch)

    manager.create_vm_port_groups(vswitch, port_groups)

def create_vm(connection, base_vm, new_vm, child_rp, child_folder):

    manager = VmManager(connection)
    manager.clone_vm(base_vm, new_vm, child_rp, child_folder, datastore_name="vms")

def update_vm(connection, vm_name, child_folder, network_map, new_mac):

    manager = VmManager(connection)
    manager.update_vm_networks(vm_name, child_folder, network_map)
    manager.update_mac_address(vm_name, "Network adapter 1", new_mac)

def setup(setups):

    for setup in setups:
        pass

if __name__ == "__main__":
    vc_host = "vcenter-appliance-2.rededucation.com"
    vc_user = os.getenv("VC_USER"); print(vc_user)
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    vc = VCenter(vc_host, vc_user, vc_password, vc_port)
    vc.connect()

    parent_rp = "cp-ultramagnus"
    child_rp = "cp-pod58"

    user = "labcp-58"
    role_name = "labcp-0-role"

    parent_folder = "cp"
    child_folder = "cp-pod58-folder"

    vswitch = "vs58-cp"
    host_name = "ultramagnus.rededucation.com"
    port_groups = {
        "cp-mgt-" + vswitch: {"vlan_id": 401},
        "cp-inta-" + vswitch: {"vlan_id": 402},
        "cp-dmz-" + vswitch: {"vlan_id": 403},
        "cp-wifi-" + vswitch: {"vlan_id": 404},
        "cp-sync-" + vswitch: {"vlan_id": 405},
        "cp-ext-" + vswitch: {"vlan_id": 406},
        "cp-intb-" + vswitch: {"vlan_id": 407},
        "cp-none1-" + vswitch: {"vlan_id": 408},
        "cp-none2-" + vswitch: {"vlan_id": 409},
        "cp-mgmtb-" + vswitch: {"vlan_id": 410},
        "cp-none3-" + vswitch: {"vlan_id": 411},
        "cp-ext-b-" + vswitch: {"vlan_id": 412},
        # Add more port groups as needed
    }
    network_map = {
        "Network adapter 2": "cp-ext-" + vswitch,
        "Network adapter 3": "cp-inta-" + vswitch,
        "Network adapter 4": "cp-mgt-" + vswitch,
        "Network adapter 5": "cp-dmz-" + vswitch,
        "Network adapter 6": "cp-intb-" + vswitch,
        "Network adapter 7": "cp-ext-b-" + vswitch,
        "Network adapter 8": "cp-none2-" + vswitch,
        "Network adapter 9": "cp-ext-b-" + vswitch,
        "Network adapter 10": "cp-mgmtb-" + vswitch,
    }

    base_vm = "cp-R81.20-vr"
    new_vm = "cp-R81-vr-58"

    # Setup
    # create_resource_pool(vc, parent_rp, child_rp)
    # create_folder(vc, parent_folder, child_folder, user, role_name)
    # create_network(vc, host_name, vswitch, port_groups)
    # create_vm(vc, base_vm, new_vm, child_rp, child_folder)
    # update_vm(vc, new_vm, child_folder, network_map, new_mac = "00:50:56:04:00:" + hex(56)[2:])
    
    # Setup Done


    # Teardown
    vm_manager = VmManager(vc)
    vm_manager.delete_folder(child_folder, force=True)

    network_manager = NetworkManager(vc)
    network_manager.delete_vswitch(host_name, vswitch)

    rp_manager = ResourcePoolManager(vc)
    rp_manager.delete_resource_pool(child_rp)

    # Teardown Done
