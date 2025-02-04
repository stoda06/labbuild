import urllib3
import requests
from pyVmomi import vim, vmodl
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor, as_completed
from storage.check_utilization import is_overutilized
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class VmManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger

    def poweron_vm(self, vm_name):
        """
        Powers On a new virtual machine within a specified resource pool.
        :param vm_name: Name of the virtual machine.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return

        # Check if the VM is powered off. If so, power it on.
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOff:
            self.logger.debug(f"VM '{vm_name}' is powered off. Attempting to power on")
            power_on_task = vm.PowerOnVM_Task()
            self.wait_for_task(power_on_task)
            self.logger.debug(f"VM '{vm_name}' powered on successfully.")
            return True
    
    def poweroff_vm(self, vm_name):
        """
        Powers off a single virtual machine.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return
        
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            self.logger.debug(f"VM '{vm_name}' is powered on. Attempting to power off")
            power_off_task = vm.PowerOffVM_Task()
            self.wait_for_task(power_off_task)
            self.logger.debug(f"VM '{vm_name}' powered off successfully.")
            return True

    def clone_vm(self, base_name, clone_name, resource_pool_name, directory_name=None, datastore_name=None, power_on=False):
        """
        Clones a VM from an existing template into a specified directory (VM folder).

        :param base_name: The name of the base VM to clone from.
        :param clone_name: The name for the cloned VM.
        :param resource_pool_name: The name of the resource pool where the cloned VM will be located.
        :param directory_name: The name of the directory (VM folder) where the cloned VM will be placed.
        :param datastore_name: Optional. The name of the datastore where the cloned VM will be stored. Uses template's datastore if None.
        :param power_on: Whether to power on the cloned VM after creation.
        """
        try:
            # Check if the cloning VM already exists in the target directory (VM folder)
            existing_vm = self.get_obj([vim.VirtualMachine], clone_name)
            if existing_vm:
                self.logger.warning(f"VM '{clone_name}' already exists.")
                return
            
            base_vm = self.get_obj([vim.VirtualMachine], base_name)
            # If the base VM is not found, check all hosts in the datacenter
            if not base_vm:
                self.logger.warning(f"Base VM '{base_name}' not found on the current host. Searching all hosts in the datacenter...")
                datacenter = self.get_obj([vim.Datacenter], "Red Education")
                if datacenter:
                    for cluster in datacenter.hostFolder.childEntity:
                        for host in cluster.host:
                            for vm in host.vm:
                                if vm.name == base_name:
                                    base_vm = vm
                                    self.logger.info(f"Base VM '{base_name}' found on host '{host.name}'.")
                                    break
                            if base_vm:
                                break
                        if base_vm:
                            break
                if not base_vm:
                    self.logger.error(f"Base VM '{base_name}' not found on any host in the datacenter.")
                    return

            resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
            if not resource_pool:
                self.logger.error(f"Resource pool '{resource_pool_name}' not found.")
                return

            vm_folder = None
            if directory_name:
                vm_folder = self.get_obj([vim.Folder], directory_name)
                if not vm_folder:
                    self.logger.error(f"VM Folder '{directory_name}' not found.")
                    return
            else:
                datacenter = self.get_obj([vim.Datacenter], "Red Education")
                vm_folder = datacenter.vmFolder

            if datastore_name:
                datastore = self.get_obj([vim.Datastore], datastore_name)
                if not datastore:
                    self.logger.error(f"Datastore '{datastore_name}' not found.")
                    return
            else:
                datastore = base_vm.datastore[0]

            clone_spec = vim.vm.CloneSpec()
            clone_spec.location = vim.vm.RelocateSpec()
            clone_spec.location.pool = resource_pool
            clone_spec.location.datastore = datastore
            clone_spec.powerOn = power_on

            # retry_count = 99
            # while retry_count > 0:
            #     if not is_overutilized(datastore.name):
            #         task = base_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            #         self.logger.debug(f"VM '{clone_name}' cloning started from base VM '{base_name}' into folder '{directory_name}'.")
            #         if self.wait_for_task(task):
            #             self.logger.info(f"VM '{clone_name}' cloned successfully from base VM '{base_name}' on datastore '{datastore.name}'.")
            #             return
            #         else:
            #             self.logger.error(f"Failed to clone VM '{clone_name}' from base VM '{base_name}' into folder '{directory_name}'.")
            #             return
            #     else:
            #         retry_count -= 1
            #         self.logger.warning(f"Datastore '{datastore.name}' is overutilized. Retrying in 30 minutes... {retry_count} retries left.")
            #         time.sleep(1800)

            # If still overutilized after retries, proceed with cloning
            task = base_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            self.logger.debug(f"Datastore overutilized, proceeding with cloning VM '{clone_name}' from base VM '{base_name}' into folder '{directory_name}'.")
            if self.wait_for_task(task):
                self.logger.info(f"VM '{clone_name}' cloned successfully from base VM '{base_name}' on datastore '{datastore.name}'.")
            else:
                self.logger.error(f"Failed to clone VM '{clone_name}' from base VM '{base_name}' into folder '{directory_name}'.")
        except Exception as e:
            self.logger.error(f"Failed to clone VM '{clone_name}': {e}")

    def find_vm_folder_by_name(self, folder_name, starting_folder=None):
        """
        Recursively searches for a VM folder by name.

        :param folder_name: The name of the VM folder to find.
        :param starting_folder: The folder to start the search from; if None, starts from the root folder.
        :return: The VM folder object if found, None otherwise.
        """
        if starting_folder is None:
            starting_folder = self.connection.content.rootFolder
            self.logger.debug(f"Starting folder: {starting_folder}")
        
        return self.search_for_folder(starting_folder, folder_name)

    def search_for_folder(self, folder, folder_name):
        """
        Recursively searches for a folder with the specified name starting from the given folder.

        :param folder: The folder to start the search from.
        :param folder_name: The name of the folder to search for.
        :return: The folder if found, None otherwise.
        """
        self.logger.debug(f"Current folder name: {folder.name}")
        if folder.name == folder_name and isinstance(folder, vim.Folder):
            return folder
        for child in folder.childEntity:
            if isinstance(child, vim.Folder):
                found_folder = self.search_for_folder(child, folder_name)
                if found_folder:
                    return found_folder
        return None

    def list_vms(self):
        """Lists all virtual machines available in the connected vCenter."""
        try:
            vms = self.get_all_objects_by_type(vim.VirtualMachine)
            for vm in vms:
                self.logger.debug(f"VM Name: {vm.name}, Power State: {vm.runtime.powerState}")
        except Exception as e:
            self.logger.error(f"Failed to list VMs: {e}")

    def get_network_adapters(self, vm_name):
        """
        Fetches all network adapters for a given VM.

        :param vm_name: The name of the VM to retrieve network adapters from.
        :return: A list of network adapter devices.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return []

        network_adapters = []
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                network_adapters.append(device)

        return network_adapters
    
    def refresh_vm(self, vm):
        try:
            vm.Reload()
            self.logger.debug(f"VM configuration for '{vm.name}' has been reloaded.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to reload VM configuration for '{vm.name}': {str(e)}")
            return False

    def update_mac_address(self, vm_name, adapter_label, new_mac_address):
        """
        Updates the MAC address of a specified network adapter on a VM and verifies the update.

        :param vm_name: The name of the VM to update.
        :param adapter_label: The label of the network adapter (e.g., "Network adapter 1").
        :param new_mac_address: The new MAC address to assign to the adapter.
        :return: True if the MAC address was successfully updated and verified, False otherwise.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False
        
        # Retrieve the network adapter based on the label
        network_name = None
        nic_spec = None
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard) and device.deviceInfo.label == adapter_label:
                if hasattr(device.backing, 'network'):
                    network_name = device.backing.network.name if hasattr(device.backing.network, 'name') else None
                    if network_name:
                        nic_spec = vim.vm.device.VirtualDeviceSpec()
                        nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                        nic_spec.device = device
                        nic_spec.device.macAddress = new_mac_address
                        nic_spec.device.addressType = 'manual'
                        break

        if not nic_spec:
            self.logger.error(f"Network adapter '{adapter_label}' not found on VM '{vm_name}', or it is not connected to a network.")
            return False

        # Apply the configuration change
        config_spec = vim.vm.ConfigSpec(deviceChange=[nic_spec])
        task = vm.ReconfigVM_Task(config_spec)
        if self.wait_for_task(task):
            # Verify the MAC address update
            return self.verify_mac_address(vm, adapter_label, new_mac_address, network_name)
        else:
            self.logger.error(f"Failed to change MAC address for '{adapter_label}' on VM '{vm_name}'.")
            return False

    def verify_mac_address(self, vm, adapter_label, expected_mac, expected_network):
        """
        Verifies the MAC address and network name of a specified adapter on a VM.

        :param vm: The VirtualMachine object.
        :param adapter_label: The label of the network adapter.
        :param expected_mac: The expected MAC address.
        :param expected_network: The expected network name.
        :return: True if the verification passes, False otherwise.
        """
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard) and device.deviceInfo.label == adapter_label:
                actual_mac = device.macAddress
                network_name = device.backing.network.name if hasattr(device.backing.network, 'name') else None
                if actual_mac == expected_mac and network_name == expected_network:
                    self.logger.debug(f"MAC address and network verified for '{adapter_label}' on VM '{vm.name}'.")
                    return True
                else:
                    self.logger.error(f"Verification failed for MAC or network on '{adapter_label}' for VM '{vm.name}'.")
                    return False
        self.logger.error(f"Network adapter '{adapter_label}' not found during verification on VM '{vm.name}'.")
        return False

    def delete_vm(self, vm_name):
        """
        Deletes a virtual machine (VM) by its name.

        :param vm_name: The name of the VM to be deleted.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return

        # Check if the VM is powered on. If so, power it off first.
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            self.logger.warning(f"VM '{vm_name}' is powered on. Attempting to power off before deletion.")
            power_off_task = vm.PowerOffVM_Task()
            self.wait_for_task(power_off_task)
            self.logger.debug(f"VM '{vm_name}' powered off successfully.")

        # Proceed to delete the VM
        try:
            delete_task = vm.Destroy_Task()
            self.wait_for_task(delete_task)
            self.logger.debug(f"VM '{vm_name}' deleted successfully.")
        except Exception as e:
            self.logger.error(f"Failed to delete VM '{vm_name}': {e}")

    def get_vm_max_resources(self, vm_name):
        """
        Retrieves the maximum allocated resources (CPU, memory, and storage) for a VM.

        :param vm_name: The name of the VM.
        :return: A dictionary with the maximum allocated CPU (in cores), memory (in MB), and storage (in GB).
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return None

        # Maximum allocated CPU (number of cores)
        cpu_cores = vm.config.hardware.numCPU
        
        # Maximum allocated memory (in MB)
        memory_mb = vm.config.hardware.memoryMB

        # Maximum allocated storage (in GB), summing up all disk sizes
        storage_gb = sum(disk.capacityInKB for disk in vm.config.hardware.device 
                         if isinstance(disk, vim.vm.device.VirtualDisk)) / (1024 * 1024)

        max_resources = {
            "cpu_cores": cpu_cores,
            "memory_mb": memory_mb,
            "storage_gb": storage_gb,
        }

        return max_resources
    
    def get_vm_current_usage(self, vm_name):
        """
        Retrieves the current usage of CPU and memory for a VM.

        :param vm_name: The name of the VM.
        :return: A dictionary with the current CPU usage (in MHz) and memory usage (in MB).
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return None

        # Current CPU usage (in MHz)
        cpu_usage_mhz = vm.summary.quickStats.overallCpuUsage

        # Current memory usage (in MB)
        memory_usage_mb = vm.summary.quickStats.guestMemoryUsage

        current_usage = {
            "cpu_usage_mhz": cpu_usage_mhz,
            "memory_usage_mb": memory_usage_mb,
        }

        return current_usage

    def power_off_vm(self, vm):
        """
        Powers off a single virtual machine.
        """
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            power_off_task = vm.PowerOffVM_Task()
            self.wait_for_task(power_off_task)

    def delete_folder(self, folder_name, force=False):
        """
        Deletes a folder by its name if the folder is empty, or if force is True, deletes it and its contents recursively.
        If force is True and there are VMs in the folder, it powers them off concurrently before deletion.
        """
        folder = self.get_obj([vim.Folder], folder_name)
        if not folder:
            self.logger.error(f"Folder '{folder_name}' not found.")
            return None
        
        if folder.childEntity and not force:
            self.logger.error(f"Folder '{folder_name}' is not empty. Cannot delete without enabling the force option.")
            return None

        if force and folder.childEntity:
            # Filter for VMs only if we need to force delete
            vms = [child for child in folder.childEntity if isinstance(child, vim.VirtualMachine)]
            
            # Proceed with concurrent power off if there are any VMs
            if vms:
                with ThreadPoolExecutor(max_workers=max(1, len(vms))) as executor:
                    future_to_vm = {executor.submit(self.power_off_vm, vm): vm for vm in vms}
                    for future in as_completed(future_to_vm):
                        vm = future_to_vm[future]
                        try:
                            future.result()  # wait for power off task to complete
                        except Exception as exc:
                            self.logger.error(f'VM {vm.name} power off generated an exception: {exc}')

        try:
            delete_task = folder.Destroy_Task()
            self.wait_for_task(delete_task)
            self.logger.debug(f"Folder '{folder_name}' and its contents were deleted successfully.")
            return None
        except Exception as e:
            self.logger.error(f"Failed to delete folder '{folder_name}': {str(e)}")
            return None

        
    def get_portgroups_for_vswitch(self, host_name, vswitch_name):
        """
        Retrieves all network objects associated with port groups for a specified vSwitch on a host,
        avoiding nested loops for efficiency.

        :param host_name: The name of the host system.
        :param vswitch_name: The name of the vSwitch.
        :return: A list of network objects associated with the port groups on the vSwitch, 
                 or None if the host or vSwitch is not found.
        """
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Host '{host_name}' not found.")
            return None

        # Filter port groups for those associated with the specified vSwitch
        self.logger.debug("Fetching associated port groups")
        associated_portgroups = [pg for pg in host.config.network.portgroup if pg.spec.vswitchName == vswitch_name]
        self.logger.debug("Done")

        if not associated_portgroups:
            self.logger.error(f"No port groups found for vSwitch '{vswitch_name}' on host '{host_name}'.")
            return None

        # Create a dictionary mapping network names to network objects for the host
        self.logger.debug("Creating Network Dict")
        network_dict = {network.name: network for network in host.network if vswitch_name in network.name}
        # network_dict = {network.name: network for network in host.network}
        self.logger.debug("Done")

        # Retrieve the network objects corresponding to the filtered port groups
        self.logger.debug("Retrieve Network objects")
        network_objects = [network_dict.get(pg.spec.name) for pg in associated_portgroups if pg.spec.name in network_dict]
        self.logger.debug("Done")

        if not network_objects:
            self.logger.error(f"No network objects found for port groups on vSwitch '{vswitch_name}'.")
            return None

        return network_objects

    def update_vm_networks(self, vm_name, course_type, pod_number):
        """
        Updates the networks of an existing VM by changing the vSwitch number in the network names
        that contain a 'vs' pattern. Networks without 'vs' in their names remain unchanged.

        :param vm_name: The name of the VM to update.
        :param pod_number: The new vSwitch number to apply to the VM's network interfaces.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False

        # Ensure VM is powered off for changes
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            self.logger.debug(f"Powering off VM '{vm_name}' for network update.")
            self.wait_for_task(vm.PowerOffVM_Task())

        device_changes = []
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                # Preserve the original network backing
                network_backing = device.backing

                if 'checkpoint' in course_type:
                    if 'vs' in device.backing.deviceName:
                        prefix, suffix = device.backing.deviceName.rsplit('vs0', 1)
                        network_backing.deviceName = f"{prefix}vs{pod_number}{suffix}"
                if '1100-210' in course_type:
                    if 'rdp' not in device.backing.deviceName:
                        prefix = device.backing.deviceName.split('1')[0]
                        network_backing.deviceName = f"{prefix}{pod_number}"
                if 'cortex' in course_type:
                    if 'internal' in device.backing.deviceName:
                        network_backing.deviceName = f"pa-internal-cortex-{pod_number}"
                if 'ipo' in course_type:
                    if 'ipo' in device.backing.deviceName and 'rdp' not in device.backing.deviceName:
                        network_backing.deviceName = f"av-ipo-{pod_number}"

                # Configure the NIC spec
                nic_spec = vim.vm.device.VirtualDeviceSpec()
                nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                nic_spec.device = device
                nic_spec.device.backing = network_backing
                device_changes.append(nic_spec)

        # Apply the changes in a batch job within a try-except block
        try:
            if device_changes:
                spec = vim.vm.ConfigSpec(deviceChange=device_changes)
                task = vm.ReconfigVM_Task(spec=spec)
                if self.wait_for_task(task):
                    self.logger.debug(f"Network interfaces on VM '{vm_name}' updated successfully.")
                    return True
                else:
                    self.logger.error("Failed to update network interfaces due to task failure.")
                    return False
            else:
                self.logger.warning("No network interface changes detected or applicable.")
                return True
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while updating VM '{vm_name}': {str(e)}")
            raise

    def get_vm_by_name_and_folder(self, vm_name, folder_name):
        """
        Retrieves a VM object based on the VM name and the name of its containing folder.

        :param vm_name: The name of the VM to retrieve.
        :param folder_name: The name of the folder in which the VM is located.
        :return: The VM object if found, None otherwise.
        """
        # Find the folder by name
        folder = self.get_obj([vim.Folder], folder_name)
        if not folder:
            self.logger.error(f"Folder '{folder_name}' not found.")
            return None

        # Search for the VM within the folder's child entities
        for child in folder.childEntity:
            if isinstance(child, vim.VirtualMachine) and child.name == vm_name:
                return child
            elif isinstance(child, vim.Folder):  # Recursively search in sub-folders
                vm = self.get_vm_by_name_and_folder(vm_name, child.name)
                if vm:
                    return vm

        self.logger.error(f"VM '{vm_name}' not found in folder '{folder_name}'.")
        return None

    def create_snapshot(self, vm_name, snapshot_name, description="", memory=False, quiesce=False):
        """
        Creates a snapshot of the specified virtual machine. Returns True if the snapshot is successfully created, otherwise False.

        :param vm_name: The name of the virtual machine.
        :param snapshot_name: The name for the new snapshot.
        :param description: An optional description for the snapshot.
        :param memory: Whether to include the virtual machine's memory in the snapshot.
        :param quiesce: Whether to quiesce the file system in the virtual machine.
        :return: True if the snapshot was successfully created, False otherwise.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False

        try:
            task = vm.CreateSnapshot_Task(name=snapshot_name, description=description,
                                        memory=memory, quiesce=quiesce)
            if self.wait_for_task(task):
                self.logger.debug(f"Snapshot '{snapshot_name}' created successfully for VM '{vm_name}'.")
                return True
            else:
                self.logger.error(f"Task failed to create snapshot '{snapshot_name}' for VM '{vm_name}'.")
                return False
        except Exception as e:
            self.logger.error(f"Failed to create snapshot for VM '{vm_name}': {e}")
            return False
    
    def snapshot_exists(self, vm_name, snapshot_name):
        """
        Checks if a snapshot with the given name exists on the specified VM.

        :param vm: vim.VirtualMachine object
            The VM to check for the snapshot.
        :param snapshot_name: str
            The name of the snapshot to search for.

        :return: bool
            True if the snapshot exists, False otherwise.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return None
        
        def _search_snapshot_tree(snapshot_tree):
            """
            Recursively search through the snapshot tree for a snapshot with the given name.

            :param snapshot_tree: list
                A list of snapshot tree nodes to search through.

            :return: bool
                True if the snapshot exists, False otherwise.
            """
            for snapshot in snapshot_tree:
                if snapshot.name == snapshot_name:
                    return True
                if snapshot.childSnapshotList:
                    if _search_snapshot_tree(snapshot.childSnapshotList):
                        return True
            return False

        if vm.snapshot is None:
            return False

        return _search_snapshot_tree(vm.snapshot.rootSnapshotList)
    
    def revert_to_snapshot(self, vm_name, snapshot_name):
        """
        Reverts the specified VM to a snapshot by its name.

        :param vm_name: Name of the VM to revert.
        :param snapshot_name: Name of the snapshot to revert the VM to.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if vm is None:
            self.logger.error(f"VM {vm_name} not found")
            return
        
        # Check if the VM is powered on. If so, power it off first.
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            self.logger.warning(f"VM '{vm_name}' is powered on. Attempting to power off before reverting to snapshot '{snapshot_name}'.")
            power_off_task = vm.PowerOffVM_Task()
            self.wait_for_task(power_off_task)
            self.logger.debug(f"VM '{vm_name}' powered off successfully.")

        snapshot = self.find_snapshot_in_tree(vm.snapshot.rootSnapshotList, snapshot_name)
        if snapshot:
            try:
                revert_task = snapshot.snapshot.RevertToSnapshot_Task()
                self.wait_for_task(revert_task)
                self.logger.debug(f"VM '{vm_name}' successfully reverted to snapshot '{snapshot_name}'")
            except vmodl.MethodFault as error:
                self.logger.error(f"Error reverting to snapshot: {error.msg}")
        else:
            self.logger.error(f"Snapshot '{snapshot_name}' not found in VM '{vm_name}'")

    def find_snapshot_in_tree(self, snapshot_tree, snapshot_name):
        """
        Recursively search for a snapshot by name in a snapshot tree.

        :param snapshot_tree: List of snapshot tree nodes.
        :param snapshot_name: Name of the snapshot to find.
        :return: Snapshot object if found, else None.
        """
        for node in snapshot_tree:
            if node.name == snapshot_name:
                return node
            elif node.childSnapshotList:
                snapshot = self.find_snapshot_in_tree(node.childSnapshotList, snapshot_name)
                if snapshot:
                    return snapshot
        return None
    
    def get_vm_uuid(self, vm_name):
        """
        Retrieve the UUID of a virtual machine given its name.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if vm:
            self.logger.debug(f"VM: {vm_name} UUID is {vm.config.uuid}")
            return vm.config.uuid
        else:
            self.logger.error("Virtual machine not found.")
            return None
    
    def download_vmx_file(self, vm_name, local_path):
        """
        Downloads the VMX file for a specified VM.

        :param vm_name: The name of the VM to download the VMX file for.
        :param local_path: The local path where the VMX file should be saved.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False

        try:
            # Get the Datacenter object
            datacenter = self.get_datacenter(vm.runtime.host)
            datastore = vm.datastore[0]
            vmx_path = vm.config.files.vmPathName

            url, session_cookie = self.get_vmx_file_url(datacenter, datastore, vmx_path)

            self.download_file(url, local_path, session_cookie)
            self.logger.debug(f"VMX file downloaded successfully to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error downloading VMX file: {e}")
            return False

    def get_datacenter(self, host):
        """
        Recursively move up in the vSphere API hierarchy to find the Datacenter for a given host.

        :param host: The HostSystem object.
        :return: The Datacenter object.
        """
        if isinstance(host.parent, vim.Datacenter):
            return host.parent
        else:
            return self.get_datacenter(host.parent)

    def get_vmx_file_url(self, datacenter, datastore, vmx_path):
        """
        Construct the URL to access the VMX file on the datastore.

        :param datacenter: The Datacenter object where the datastore is located.
        :param datastore: The Datastore object where the VMX file is stored.
        :param vmx_path: The path to the VMX file on the datastore.
        :return: The full URL to download the VMX file, and the session cookie.
        """
        vc_host = self.vcenter.host  # Use the vCenter hostname or IP stored in vcenter object
        datacenter_name = datacenter.name
        datastore_name = datastore.name
        path = f"/folder/{vmx_path.split('] ')[1]}"

        url = f"https://{vc_host}{path}?dcPath={datacenter_name}&dsName={datastore_name}"

        # Add the session cookie to the HTTP request header
        session_cookie = self.connection._stub.cookie.split('=', 1)[1].strip()
        return url, session_cookie

    def download_file(self, url, local_path, session_cookie):
        """
        Download a file from a given URL using a session cookie for authentication.

        :param url: The URL from which to download the file.
        :param local_path: The local path where the file should be saved.
        :param session_cookie: The session cookie for authenticated access.
        """
        headers = {
            'Cookie': f'vmware_soap_session={session_cookie}'
        }
        try:
            with requests.get(url, headers=headers, stream=True, verify=False) as response:
                response.raise_for_status()  # Raises HTTPError for bad responses
                with open(local_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            file.write(chunk)
            self.logger.debug(f"VMX file downloaded successfully to {local_path}")
            return True
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP Error downloading VMX file: {e}")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error downloading VMX file: {e}")
            return False
    
    def upload_vmx_file(self, vm_name, local_vmx_path):
        """
        Uploads a local VMX file to the VM's datastore location on the server.

        :param vm_name: The name of the VM to upload the VMX file for.
        :param local_vmx_path: The absolute path to the local VMX file.
        :return: True if the upload was successful, False otherwise.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False

        vmx_path = vm.config.files.vmPathName
        datacenter = self.get_datacenter(vm.runtime.host)
        datastore = vm.datastore[0]
        url, session_cookie = self.get_vmx_file_url(datacenter, datastore, vmx_path)

        headers = {
            'Cookie': f'vmware_soap_session={session_cookie}'
        }

        try:
            with open(local_vmx_path, 'rb') as file:
                response = requests.put(url, data=file, headers=headers, verify=False)
                if response.status_code == 200:
                    self.logger.debug(f"Successfully uploaded VMX file to '{url}'.")
                    return True
                else:
                    self.logger.error(f"Failed to upload VMX file. Server responded with status code: {response.status_code}")
                    return False
        except Exception as e:
            self.logger.error(f"Failed to upload VMX file: {str(e)}")
            return False
    
    def update_vm_uuid(self, vmx_file_path, new_uuid):
        """
        Updates the 'uuid.bios' entry in a VMX file with a new UUID.

        :param vmx_file_path: The absolute path to the VMX file.
        :param new_uuid: The new UUID to set for the virtual machine.
        :return: True if the update was successful, False otherwise.
        """
        try:
            with open(vmx_file_path, 'r') as file:
                lines = file.readlines()
        
            updated = False
            for i, line in enumerate(lines):
                if line.strip().startswith('uuid.bios'):
                    lines[i] = f"uuid.bios = \"{new_uuid}\"\n"
                    updated = True
                    break
            
            if not updated:
                self.logger.error("uuid.bios entry not found in the VMX file.")
                return False

            with open(vmx_file_path, 'w') as file:
                file.writelines(lines)
            
            self.logger.debug(f"UUID updated successfully in the VMX file: {vmx_file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update the VMX file '{vmx_file_path}': {str(e)}")
            return False
        
    def create_linked_clone(self, base_vm_name, clone_name, snapshot_name, resource_pool_name, directory_name=None, datastore_name=None):
        """
        Creates a linked clone from an existing snapshot.

        :param base_vm_name: The name of the source VM.
        :param clone_name: The name of the new cloned VM.
        :param snapshot_name: The name of the snapshot to clone from.
        :param resource_pool_name: The name of the resource pool to place the cloned VM.
        :param datastore_name: The name of the datastore where the cloned VM will be placed. If None, uses the same datastore as the source VM.
        :return: True if the clone was created successfully, False otherwise.
        """
        try:
            # Retrieve the resource pool
            resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
            if not resource_pool:
                self.logger.error(f"Resource pool '{resource_pool_name}' not found.")
                return False

            # Check if the VM already exists in the specified resource pool
            for vm in resource_pool.vm:
                if vm.name == clone_name:
                    self.logger.warning(f"VM '{clone_name}' already exists in the resource pool '{resource_pool_name}'.")
                    return True

            base_vm = self.get_obj([vim.VirtualMachine], base_vm_name)
            if not base_vm:
                self.logger.error(f"VM '{base_vm_name}' not found.")
                return False

            if not base_vm.snapshot:
                try:
                    task = base_vm.CreateSnapshot_Task(name=snapshot_name, description="",
                                                memory=False, quiesce=False)
                    if self.wait_for_task(task):
                        self.logger.debug(f"Snapshot '{snapshot_name}' created successfully for VM '{base_vm_name}'.")
                        return True
                    else:
                        self.logger.error(f"Task failed to create snapshot '{snapshot_name}' for VM '{base_vm_name}'.")
                        return False
                except Exception as e:
                    self.logger.error(f"Failed to create snapshot for VM '{base_vm_name}': {e}")
                    return False

            snapshot = self.find_snapshot_in_tree(base_vm.snapshot.rootSnapshotList, snapshot_name)
            if not snapshot:
                self.logger.error(f"Snapshot '{snapshot_name}' not found in VM '{base_vm_name}'.")
                return False

            vm_folder = None
            if directory_name:
                vm_folder = self.get_obj([vim.Folder], directory_name)
                if not vm_folder:
                    self.logger.error(f"VM Folder '{directory_name}' not found.")
                    return False
            else:
                datacenter = self.get_obj([vim.Datacenter], "Red Education")
                vm_folder = datacenter.vmFolder

            datastore = self.get_obj([vim.Datastore], datastore_name) if datastore_name else base_vm.datastore[0]

            # Set up the clone specification to use the provided resource pool and datastore
            clone_spec = vim.vm.CloneSpec()
            relocate_spec = vim.vm.RelocateSpec()
            relocate_spec.pool = resource_pool
            relocate_spec.datastore = datastore
            relocate_spec.diskMoveType = 'createNewChildDiskBacking'

            clone_spec.location = relocate_spec
            clone_spec.snapshot = snapshot.snapshot

            task = base_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            if self.wait_for_task(task):
                self.logger.info(f"Linked clone '{clone_name}' created successfully on datastore '{datastore.name}'.")
                return True
            else:
                self.logger.error(f"Failed to create linked clone '{clone_name}'.")
                return False
        except Exception as e:
            self.logger.error(f"Failed to create linked clone: {e}")
            return False
        
    def verify_uuid(self, vm_name, expected_uuid):
        """
        Verifies if the VM's current UUID matches the provided UUID.

        :param vm_name: The name of the VM to verify the UUID for.
        :param expected_uuid: The UUID to verify against.
        :return: True if the UUID matches, False otherwise.
        """
        local_vmx_path = f"/tmp/{vm_name}.vmx"  # Temporary path to download the VMX file

        # Step 1: Download the VMX file
        if not self.download_vmx_file(vm_name, local_vmx_path):
            self.logger.error(f"Failed to download VMX file for VM '{vm_name}'.")
            return False

        try:
            # Step 2: Read the VMX file and find the line with 'uuid.bios'
            with open(local_vmx_path, 'r') as file:
                for line in file:
                    if line.strip().startswith('uuid.bios'):
                        actual_uuid = line.split('=')[1].strip().strip('"')
                        if actual_uuid == expected_uuid:
                            self.logger.info(f"UUID matches for VM '{vm_name}': {actual_uuid}")
                            return True
                        else:
                            self.logger.warning(f"UUID does not match for VM '{vm_name}'. Expected: {expected_uuid}, Actual: {actual_uuid}")
                            return False

            # If 'uuid.bios' is not found in the VMX file
            self.logger.error(f"'uuid.bios' entry not found in the VMX file for VM '{vm_name}'.")
            return False
        except Exception as e:
            self.logger.error(f"Error reading the VMX file '{local_vmx_path}': {str(e)}")
            return False


    def get_vm_network(self, vm_name):
        """
        Retrieves the network details of an existing VM, including network names,
        MAC addresses, and physical adapters.

        :param vm_name: The name of the VM to retrieve network details from.
        :return: A dictionary containing network names, MAC addresses, and physical adapters.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return None

        network_details = {}
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                network_name = device.backing.network.name if device.backing.network else "N/A"
                mac_address = device.macAddress
                connected_at_power_on = device.connectable.startConnected

                network_details[device.deviceInfo.label] = {
                    "network_name": network_name,
                    "mac_address": mac_address,
                    "connected_at_power_on": connected_at_power_on
                }

        return network_details

    def update_vm_network(self, vm_name, network_dict):
        """
        Updates the networks of an existing VM based on the provided network_dict.

        :param vm_name: The name of the VM to update.
        :param network_dict: A dictionary containing the network details to update.
                            Expected format:
                            {
                                "Network adapter 1": {
                                    "network_name": "NewNetwork",
                                    "mac_address": "00:50:56:XX:XX:XX",
                                    "connected_at_power_on": True
                                },
                                ...
                            }
        :return: True if the network update is successful, False otherwise.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False

        max_retries = 3  # Number of retries for the network update
        retry_interval = 10  # Wait time in seconds between retries

        for attempt in range(1, max_retries + 1):
            try:
                # Ensure VM is powered off for changes
                if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
                    self.logger.debug(f"Powering off VM '{vm_name}' for network update.")
                    self.wait_for_task(vm.PowerOffVM_Task())

                device_changes = []
                for device in vm.config.hardware.device:
                    if isinstance(device, vim.vm.device.VirtualEthernetCard):
                        device_label = device.deviceInfo.label
                        if device_label in network_dict:
                            network_backing = device.backing
                            network_name = network_dict[device_label].get("network_name")
                            mac_address = network_dict[device_label].get("mac_address")
                            connected_at_power_on = network_dict[device_label].get("connected_at_power_on")

                            if network_name:
                                network_backing.deviceName = network_name

                            nic_spec = vim.vm.device.VirtualDeviceSpec()
                            nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                            nic_spec.device = device
                            nic_spec.device.backing = network_backing

                            if mac_address and device.macAddress != mac_address:
                                nic_spec.device.macAddress = mac_address

                            # Ensure connectable attributes are updated correctly
                            if hasattr(device, "connectable") and device.connectable:
                                nic_spec.device.connectable.startConnected = connected_at_power_on
                            else:
                                nic_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
                                nic_spec.device.connectable.startConnected = connected_at_power_on
                                nic_spec.device.connectable.allowGuestControl = True  # Allow guest OS to control connection state
                                nic_spec.device.connectable.connected = False  # Adapter will connect on power-on

                            device_changes.append(nic_spec)

                if not device_changes:
                    self.logger.warning("No network interface changes detected or applicable.")
                    return True

                spec = vim.vm.ConfigSpec(deviceChange=device_changes)
                task = vm.ReconfigVM_Task(spec=spec)

                if self.wait_for_task(task):
                    self.logger.debug(f"Network interfaces on VM '{vm_name}' updated successfully.")
                    return True
                else:
                    task_info = task.info
                    error_message = task_info.error.localizedMessage if task_info.error else "Unknown error"
                    self.logger.error(f"Failed to update network interfaces on VM '{vm_name}': {error_message}")
                    return False

            except vim.fault.TaskInProgress as e:
                self.logger.warning(
                    f"VM '{vm_name}' is busy with another operation. Waiting for it to complete (Attempt {attempt}/{max_retries})."
                )
                time.sleep(retry_interval)
            except Exception as e:
                self.logger.error(f"An unexpected error occurred during attempt {attempt}: {str(e)}")
                if attempt == max_retries:
                    self.logger.error(f"All {max_retries} retry attempts failed for updating VM '{vm_name}'.")
                    raise

        self.logger.error(f"Failed to update VM '{vm_name}' after {max_retries} retries.")
        return False


    def update_serial_port_pipe_name(self, vm_name, serial_port_label, new_pipe_name):
        """
        Update the pipe name of the specified serial port for a given VM.

        :param vm_name: Name of the virtual machine.
        :param serial_port_label: Label of the serial port (e.g., "Serial port 1").
        :param new_pipe_name: New pipe name to be set (e.g., "\\.\pipe\com_21").
        """
        try:
            # Get the VM object
            vm = self.get_obj([vim.VirtualMachine], vm_name)
            if not vm:
                self.logger.error(f"VM {vm_name} not found.")
                return

            # Find the serial port by its label
            serial_port = None
            for dev in vm.config.hardware.device:
                if isinstance(dev, vim.vm.device.VirtualSerialPort) and dev.deviceInfo.label == serial_port_label:
                    serial_port = dev
                    break

            if not serial_port:
                self.logger.error(f"Serial port with label '{serial_port_label}' not found.")
                return

            # Update the pipe name
            serial_port.backing.pipeName = new_pipe_name

            # Create the config spec
            dev_changes = []
            dev_changes.append(vim.vm.device.VirtualDeviceSpec(
                operation=vim.vm.device.VirtualDeviceSpec.Operation.edit,
                device=serial_port
            ))

            config_spec = vim.vm.ConfigSpec(deviceChange=dev_changes)

            # Reconfigure the VM
            task = vm.ReconfigVM_Task(config_spec)
            self.logger.debug(f"Reconfiguring VM {vm_name}...")
            self.wait_for_task(task)

            if task.info.state == 'success':
                self.logger.debug("Reconfiguration completed successfully.")
            else:
                self.logger.error(f"Reconfiguration task finished with state: {task.info.state}")

        except Exception as e:
            self.logger.error(f"Error updating serial port pipe name: {e}")
    
    def get_cd_drive(self, vm_name):
        """
        Retrieves all CD/DVD drive information for the specified VM.
        
        :param vm_name: The name of the virtual machine.
        :return: A dictionary containing CD/DVD drive information, or an empty dictionary if none found.
        """
        try:
            content = self.connection.content
            vm = self.get_obj([vim.VirtualMachine], vm_name)
            if not vm:
                self.logger.error(f"VM {vm_name} not found.")
                return {}

            cd_drive_info = {}
            for device in vm.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualCdrom):
                    drive_details = {
                        "device_label": device.deviceInfo.label,
                        "iso_type": None,
                        "datastore": None,
                        "iso_path": None,
                        "connected": device.connectable.connected if device.connectable else False,
                    }

                    # Determine the type of backing
                    if isinstance(device.backing, vim.vm.device.VirtualCdrom.RemoteAtapiBackingInfo):
                        drive_details["iso_type"] = "Client Device"
                    elif isinstance(device.backing, vim.vm.device.VirtualCdrom.IsoBackingInfo):
                        drive_details["iso_type"] = "Datastore ISO file"
                        drive_details["iso_path"] = device.backing.fileName
                        # Extract datastore name from the ISO path
                        if device.backing.fileName.startswith("["):
                            datastore_end = device.backing.fileName.find("]")
                            if datastore_end != -1:
                                drive_details["datastore"] = device.backing.fileName[1:datastore_end]

                    cd_drive_info[device.deviceInfo.label] = drive_details

            if not cd_drive_info:
                self.logger.warning(f"No CD/DVD drives found in VM {vm_name}.")
            else:
                self.logger.debug(f"Retrieved CD/DVD drive info for VM {vm_name}: {cd_drive_info}")

            return cd_drive_info

        except vmodl.MethodFault as error:
            self.logger.error(f"Failed to retrieve CD/DVD drive information for VM {vm_name}: {error}")
            return {}
    
    def modify_cd_drive(self, vm_name, drive_name, iso_type, datastore_name, iso_path, connected=False):
        try:
            content = self.connection.content
            vm = self.get_obj([vim.VirtualMachine], vm_name)
            if not vm:
                self.logger.error(f"VM {vm_name} not found.")
                return False
            
            device_changes = []
            for device in vm.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualCdrom) and device.deviceInfo.label == drive_name:
                    cd_spec = vim.vm.device.VirtualDeviceSpec()
                    cd_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                    cd_spec.device = device

                    if iso_type == "Client Device":
                        cd_spec.device.backing = vim.vm.device.VirtualCdrom.RemoteAtapiBackingInfo(
                            deviceName=""
                        )
                    elif iso_type == "Datastore ISO file":
                        datastore = self.get_obj([vim.Datastore], datastore_name)
                        if not datastore:
                            self.logger.error(f"Datastore {datastore_name} not found.")
                            return False
                        cd_spec.device.backing = vim.vm.device.VirtualCdrom.IsoBackingInfo(
                            fileName=f"[{datastore_name}] {iso_path}"
                        )
                    else:
                        self.logger.error(f"Invalid ISO type: {iso_type}")
                        return False

                    cd_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo(
                        startConnected=connected,
                        allowGuestControl=True
                    )

                    device_changes.append(cd_spec)
                    break

            if not device_changes:
                self.logger.error(f"CD/DVD drive {drive_name} not found in VM {vm_name}.")
                return False

            spec = vim.vm.ConfigSpec()
            spec.deviceChange = device_changes
            task = vm.ReconfigVM_Task(spec=spec)
            self.wait_for_task(task)
            self.logger.debug(f"Successfully modified the CD/DVD drive {drive_name} in VM {vm_name}.")
            return True

        except vmodl.MethodFault as error:
            self.logger.error(f"Failed to modify CD/DVD drive: {error}")
            return False
        
    def connect_networks_to_vm(self, vm_name, network_dict):
        """
        Connects the networks in the provided network_dict to the specified VM, 
        including setting the 'Connected at Power On' attribute.

        :param vm_name: The name of the VM.
        :param network_dict: A dictionary containing network names, MAC addresses, 
                            and 'connected_at_power_on' flag.
                            Expected format:
                            {
                                "Network adapter 1": {
                                    "network_name": "NewNetwork",
                                    "mac_address": "00:50:56:XX:XX:XX",
                                    "connected_at_power_on": True
                                },
                                ...
                            }
        :return: True if all networks were connected successfully, False otherwise.
        """
        try:
            vm = self.get_obj([vim.VirtualMachine], vm_name)
            if not vm:
                self.logger.error(f"VM '{vm_name}' not found.")
                return False

            device_change = []
            for adapter_label, network_info in network_dict.items():
                network_name = network_info.get('network_name')
                mac_address = network_info.get('mac_address')
                connected_at_power_on = network_info.get('connected_at_power_on', False)

                network = self.get_obj([vim.Network], network_name)
                if not network:
                    self.logger.error(f"Network '{network_name}' not found.")
                    continue

                for device in vm.config.hardware.device:
                    if isinstance(device, vim.vm.device.VirtualEthernetCard) and device.macAddress == mac_address:
                        if not device.connectable.connected or device.connectable.startConnected != connected_at_power_on:
                            device_spec = vim.vm.device.VirtualDeviceSpec()
                            device_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                            device_spec.device = device

                            # Ensure connectable attributes are properly handled
                            if hasattr(device, "connectable") and device.connectable:
                                device_spec.device.connectable.startConnected = connected_at_power_on
                                device_spec.device.connectable.connected = True  # Ensure the adapter is connected
                            else:
                                device_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
                                device_spec.device.connectable.startConnected = connected_at_power_on
                                device_spec.device.connectable.allowGuestControl = True
                                device_spec.device.connectable.connected = True

                            # Update network backing info
                            device_spec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                            device_spec.device.backing.network = network
                            device_spec.device.backing.deviceName = network_name

                            device_change.append(device_spec)
                        break  # Stop searching once the correct adapter is found

            if device_change:
                spec = vim.vm.ConfigSpec()
                spec.deviceChange = device_change
                task = vm.ReconfigVM_Task(spec=spec)
                if self.wait_for_task(task):
                    self.logger.debug(f"All specified networks connected successfully to VM '{vm_name}'.")
                    return True
                else:
                    self.logger.error(f"Failed to connect networks to VM '{vm_name}'.")
                    return False
            else:
                self.logger.debug(f"No networks required connection changes for VM '{vm_name}'.")
                return True

        except Exception as e:
            self.logger.error(f"Failed to connect networks to VM '{vm_name}': {e}")
            return False

        
    def reconfigure_vm_resources(self, vm_name, new_cpu_count=None, new_memory_size_mb=None):
        try:
            # Find the VM by name
            vm = self.get_obj([vim.VirtualMachine], vm_name)
            if not vm:
                self.logger.error(f"VM '{vm_name}' not found.")
                return False
            
            # Prepare the configuration specification
            config_spec = vim.vm.ConfigSpec()
            
            # Update CPU count if provided
            if new_cpu_count:
                config_spec.numCPUs = new_cpu_count
            
            # Update memory size if provided
            if new_memory_size_mb:
                config_spec.memoryMB = new_memory_size_mb
            
            # Reconfigure the VM
            task = vm.ReconfigVM_Task(config_spec)
            
            # Wait for the task to complete
            while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
                time.sleep(1)
            
            if task.info.state == vim.TaskInfo.State.success:
                self.logger.debug(f"VM '{vm_name}' reconfigured successfully.")
                return True
            else:
                self.logger.error(f"VM '{vm_name}' reconfiguration failed: {task.info.error}")
                return False
        
        except Exception as e:
            self.logger.error(f"Error reconfiguring VM '{vm_name}': {e}")
            return False