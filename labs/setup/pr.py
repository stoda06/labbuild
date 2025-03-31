from managers.resource_pool_manager import ResourcePoolManager
from managers.vm_manager import VmManager


def update_network_dict(network_dict, pod_number):
    def replace_mac_octet(mac_address, pod_num):
        mac_parts = mac_address.split(':')
        mac_parts[4] = format(pod_num, '02x')  # This ensures zero-padded two-digit hex
        return ':'.join(mac_parts)

    updated_network_dict = {}
    for adapter, details in network_dict.items():
        network_name = details['network_name']
        mac_address = details['mac_address']
        connected_at_power_on = details['connected_at_power_on']

        if 'rdp' in network_name:
            mac_address = replace_mac_octet(mac_address, pod_number)

        updated_network_dict[adapter] = {
            'network_name': network_name,
            'mac_address': mac_address,
            'connected_at_power_on': connected_at_power_on
        }

    return updated_network_dict


def build_pr_pod(service_instance, pod_config, rebuild=False, thread=4, full=False, selected_components=None):
    vmm = VmManager(service_instance)
    rpm = ResourcePoolManager(service_instance)
    pod_number = int(pod_config["pod_number"])
    snapshot_name = 'base'
    components_to_build = pod_config["components"]

    # STEP 1: Create resource pool.
    parent_resource_pool = pod_config["group"]
    group_pool = parent_resource_pool + "-pod" + str(pod_number)
    if not rpm.create_resource_pool(parent_resource_pool, group_pool):
        return False, "create_resource_pool", f"Failed creating resource pool {group_pool} under {parent_resource_pool}"

    # STEP 2: Filter components if needed.
    if selected_components:
        components_to_build = [
            component for component in components_to_build
            if component["component_name"] in selected_components
        ]

    # STEP 3: Process each component.
    for component in components_to_build:
        clone_name = component["clone_name"]

        # If rebuild is requested, delete the existing VM.
        if rebuild:
            if not vmm.delete_vm(clone_name):
                return False, "delete_vm", f"Failed deleting VM {clone_name}"

        # STEP 3a: Clone the VM.
        if not full:
            # Ensure the base snapshot exists.
            if not vmm.snapshot_exists(component["base_vm"], snapshot_name):
                if not vmm.create_snapshot(component["base_vm"], snapshot_name, description="Snapshot used for creating linked clones."):
                    return False, "create_snapshot", f"Failed creating snapshot on base VM {component['base_vm']}"
            if not vmm.create_linked_clone(component["base_vm"], clone_name, snapshot_name, group_pool):
                return False, "create_linked_clone", f"Failed creating linked clone for {clone_name}"
        else:
            if not vmm.clone_vm(component["base_vm"], clone_name, group_pool):
                return False, "clone_vm", f"Failed cloning VM for {clone_name}"

        # STEP 3b: Update VM network.
        vm_network = vmm.get_vm_network(component["base_vm"])
        updated_vm_network = update_network_dict(vm_network, pod_number)
        if not vmm.update_vm_network(clone_name, updated_vm_network):
            return False, "update_vm_network", f"Failed updating network for {clone_name}"
        if not vmm.connect_networks_to_vm(clone_name, updated_vm_network):
            return False, "connect_networks_to_vm", f"Failed connecting networks for {clone_name}"

        # STEP 3c: Create a snapshot on the cloned VM (if not already present).
        if not vmm.snapshot_exists(clone_name, snapshot_name):
            if not vmm.create_snapshot(clone_name, snapshot_name, description=f"Snapshot of {clone_name}"):
                return False, "create_snapshot", f"Failed creating snapshot on {clone_name}"

        # STEP 3d: Modify the CD drive.
        drive_name = "CD/DVD drive 1"
        iso_type = "Datastore ISO file"
        iso_path = "podiso/pod-" + str(pod_number) + "-a.iso"
        # Retrieve the current CD drive info.
        cd_drive_info = vmm.get_cd_drive(clone_name)
        if "datastore" in cd_drive_info.get("datastore", ""):
            datastore_name = "datastore2-ho"
        else:
            datastore_name = "keg2"
        if not vmm.modify_cd_drive(clone_name, drive_name, iso_type, datastore_name, iso_path, connected=True):
            return False, "modify_cd_drive", f"Failed modifying CD drive for {clone_name}"

    # STEP 4: Power on VMs for components not marked as "poweroff".
    for component in components_to_build:
        if component.get("state") != "poweroff":
            if not vmm.poweron_vm(component["clone_name"]):
                return False, "poweron_vm", f"Failed powering on {component['clone_name']}"

    return True, None, None


def teardown_pr_pod(service_instance, pod_config):

    rpm = ResourcePoolManager(service_instance)
    group_pool = f'pr-pod{pod_config["pod_number"]}'
    
    rpm.poweroff_all_vms(group_pool)
    rpm.delete_resource_pool(group_pool)