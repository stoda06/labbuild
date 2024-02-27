from pyVmomi import vim, vmodl
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor, as_completed

class VmManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger

    def create_vm(self, vm_name, resource_pool_name, datastore_name, network_name, num_cpus=1, memory_mb=1024, guest_id='otherGuest'):
        """
        Creates a new virtual machine within a specified resource pool.

        :param vm_name: Name of the virtual machine.
        :param resource_pool_name: Name of the resource pool where the VM will be created.
        :param datastore_name: Name of the datastore for the VM's files.
        :param network_name: Name of the network for the VM.
        :param num_cpus: Number of CPUs allocated to the VM.
        :param memory_mb: Amount of memory (in MB) allocated to the VM.
        :param guest_id: Identifier for the guest OS type.
        """
        try:
            resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
            if not resource_pool:
                self.logger.error(f"Resource pool '{resource_pool_name}' not found.")
                return

            datastore = self.get_obj([vim.Datastore], datastore_name)
            network = self.get_obj([vim.Network], network_name)

            # Assuming vm_folder is determined by your infrastructure setup
            vm_folder = self.connection.content.rootFolder  # Adjust as necessary

            vm_config_spec = vim.vm.ConfigSpec(
                name=vm_name,
                memoryMB=memory_mb,
                numCPUs=num_cpus,
                guestId=guest_id,
                files=vim.vm.FileInfo(vmPathName=f'[{datastore.name}]'),
                # Additional configuration such as network adapter, disk, etc.
            )

            # Example: Adding a network adapter to the VM configuration
            nic_spec = vim.vm.device.VirtualDeviceSpec()
            nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
            nic_spec.device = vim.vm.device.VMXNET3()
            nic_spec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
            nic_spec.device.backing.network = network
            nic_spec.device.backing.deviceName = network_name
            nic_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
            nic_spec.device.connectable.startConnected = True

            vm_config_spec.deviceChange = [nic_spec]

            # Create the VM
            create_vm_task = vm_folder.CreateVM_Task(config=vm_config_spec, pool=resource_pool, host=None)
            self.wait_for_task(create_vm_task)
            self.logger.info(f"VM '{vm_name}' created successfully in resource pool '{resource_pool_name}'.")
        except Exception as e:
            self.logger.error(f"Failed to create VM '{vm_name}': {e}")

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
            self.logger.info(f"VM '{vm_name}' is powered off. Attempting to power on")
            power_on_task = vm.PowerOnVM_Task()
            self.wait_for_task(power_on_task)
            self.logger.info(f"VM '{vm_name}' powered on successfully.")


    def clone_vm(self, base_name, clone_name, resource_pool_name, directory_name, datastore_name=None, power_on=False):
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
            base_vm = self.get_obj([vim.VirtualMachine], base_name)
            if not base_vm:
                self.logger.error(f"Base VM '{base_name}' not found.")
                return

            resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
            if not resource_pool:
                self.logger.error(f"Resource pool '{resource_pool_name}' not found.")
                return

            vm_folder = self.get_obj([vim.Folder], directory_name)
            if not vm_folder:
                self.logger.error(f"VM Folder '{directory_name}' not found.")
                return

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

            task = base_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            self.wait_for_task(task)
            self.logger.info(f"VM '{clone_name}' cloned successfully from base VM '{base_name}' into folder '{directory_name}'.")
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
            print(f"Starting folder: {starting_folder}")
        
        return self.search_for_folder(starting_folder, folder_name)

    def search_for_folder(self, folder, folder_name):
        """
        Recursively searches for a folder with the specified name starting from the given folder.

        :param folder: The folder to start the search from.
        :param folder_name: The name of the folder to search for.
        :return: The folder if found, None otherwise.
        """
        print(f"Current folder name: {folder.name}")
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
                print(f"VM Name: {vm.name}, Power State: {vm.runtime.powerState}")
        except Exception as e:
            print(f"Failed to list VMs: {e}")

    def get_network_adapters(self, vm_name):
        """
        Fetches all network adapters for a given VM.

        :param vm_name: The name of the VM to retrieve network adapters from.
        :return: A list of network adapter devices.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            print(f"VM '{vm_name}' not found.")
            return []

        network_adapters = []
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                network_adapters.append(device)

        return network_adapters
    
    def update_mac_address(self, vm_name, adapter_label, new_mac_address):
        """
        Updates the MAC address of a specified network adapter on a VM.

        :param vm_name: The name of the VM to update.
        :param adapter_label: The label of the network adapter (e.g., "Network adapter 1").
        :param new_mac_address: The new MAC address to assign to the adapter.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            raise ValueError(f"VM '{vm_name}' not found.")
        
        # Find the specified network adapter
        nic_spec = None
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard) and device.deviceInfo.label == adapter_label:
                nic_spec = vim.vm.device.VirtualDeviceSpec()
                nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                nic_spec.device = device
                nic_spec.device.macAddress = new_mac_address
                nic_spec.device.addressType = 'manual'  # Important for setting custom MAC
                break

        if not nic_spec:
            raise ValueError(f"Network adapter '{adapter_label}' not found on VM '{vm_name}'.")

        # Apply the configuration change
        config_spec = vim.vm.ConfigSpec(deviceChange=[nic_spec])
        try:
            task = vm.ReconfigVM_Task(config_spec)
            self.wait_for_task(task)
            print(f"MAC address of '{adapter_label}' on VM '{vm_name}' updated to '{new_mac_address}'.")
        except vmodl.MethodFault as error:
            raise Exception(f"Error updating MAC address: {error.msg}")

    def delete_vm(self, vm_name):
        """
        Deletes a virtual machine (VM) by its name.

        :param vm_name: The name of the VM to be deleted.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            print(f"VM '{vm_name}' not found.")
            return

        # Check if the VM is powered on. If so, power it off first.
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            print(f"VM '{vm_name}' is powered on. Attempting to power off before deletion.")
            power_off_task = vm.PowerOffVM_Task()
            self.wait_for_task(power_off_task)
            print(f"VM '{vm_name}' powered off successfully.")

        # Proceed to delete the VM
        try:
            delete_task = vm.Destroy_Task()
            self.wait_for_task(delete_task)
            print(f"VM '{vm_name}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete VM '{vm_name}': {e}")

    def get_vm_max_resources(self, vm_name):
        """
        Retrieves the maximum allocated resources (CPU, memory, and storage) for a VM.

        :param vm_name: The name of the VM.
        :return: A dictionary with the maximum allocated CPU (in cores), memory (in MB), and storage (in GB).
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            print(f"VM '{vm_name}' not found.")
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
            print(f"VM '{vm_name}' not found.")
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
            return f"Folder '{folder_name}' not found."
        
        if folder.childEntity and not force:
            return f"Folder '{folder_name}' is not empty. Cannot delete without enabling the force option."

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
                            print(f'VM {vm.name} power off generated an exception: {exc}')

        try:
            delete_task = folder.Destroy_Task()
            self.wait_for_task(delete_task)
            return f"Folder '{folder_name}' and its contents were deleted successfully."
        except Exception as e:
            return f"Failed to delete folder '{folder_name}': {str(e)}"

        
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
            print(f"Host '{host_name}' not found.")
            return None

        # Filter port groups for those associated with the specified vSwitch
        print("Fetching associated port groups")
        associated_portgroups = [pg for pg in host.config.network.portgroup if pg.spec.vswitchName == vswitch_name]
        print("Done")

        if not associated_portgroups:
            print(f"No port groups found for vSwitch '{vswitch_name}' on host '{host_name}'.")
            return None

        # Create a dictionary mapping network names to network objects for the host
        print("Creating Network Dict")
        network_dict = {network.name: network for network in host.network if vswitch_name in network.name}
        # network_dict = {network.name: network for network in host.network}
        print("Done")

        # Retrieve the network objects corresponding to the filtered port groups
        print("Retrieve Network objects")
        network_objects = [network_dict.get(pg.spec.name) for pg in associated_portgroups if pg.spec.name in network_dict]
        print("Done")

        if not network_objects:
            print(f"No network objects found for port groups on vSwitch '{vswitch_name}'.")
            return None

        return network_objects

    def update_vm_networks(self, vm_name, folder_name, pod_number):
        """
        Updates the networks of an existing VM by changing the vSwitch number in the network names
        that contain a 'vs' pattern. Networks without 'vs' in their names are ignored.

        :param vm_name: The name of the VM to update.
        :param folder_name: The name of the folder containing the VM.
        :param new_vs_number: The new vSwitch number to apply to the VM's network interfaces.
        """
        vm = self.get_vm_by_name_and_folder(vm_name, folder_name)
        if not vm:
            raise ValueError(f"VM '{vm_name}' not found.")

        # Ensure VM is powered off for changes
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            print(f"Powering off VM '{vm_name}' for network update.")
            self.wait_for_task(vm.PowerOffVM_Task())

        device_changes = []
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                current_network_name = device.backing.deviceName
                # Check if 'vs' is in the current network name
                if 'vs' in current_network_name:
                    # Perform string manipulation to change only the 'vs' number part of the network name
                    prefix, suffix = current_network_name.rsplit('vs0', 1)
                    new_network_name = f"{prefix}vs{pod_number}{suffix}"

                    # Set up network backing with the new network name
                    network_backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                    network_backing.deviceName = new_network_name

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
                self.wait_for_task(task)
                print(f"Network interfaces on VM '{vm_name}' updated successfully.")
            else:
                print("No network interface changes detected or applicable.")
        except vmodl.MethodFault as error:
            raise Exception(f"Failed to update network interfaces for VM '{vm_name}': {error.msg}")
        except Exception as e:
            raise Exception(f"An unexpected error occurred while updating VM '{vm_name}': {str(e)}")


    
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
            print(f"Folder '{folder_name}' not found.")
            return None

        # Search for the VM within the folder's child entities
        for child in folder.childEntity:
            if isinstance(child, vim.VirtualMachine) and child.name == vm_name:
                return child
            elif isinstance(child, vim.Folder):  # Recursively search in sub-folders
                vm = self.get_vm_by_name_and_folder(vm_name, child.name)
                if vm:
                    return vm

        print(f"VM '{vm_name}' not found in folder '{folder_name}'.")
        return None

    def create_snapshot(self, vm_name, snapshot_name, description="", memory=False, quiesce=False):
        """
        Creates a snapshot of the specified virtual machine.

        :param vm_name: The name of the virtual machine.
        :param snapshot_name: The name for the new snapshot.
        :param description: An optional description for the snapshot.
        :param memory: Whether to include the virtual machine's memory in the snapshot.
        :param quiesce: Whether to quiesce the file system in the virtual machine.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if vm:
            try:
                task = vm.CreateSnapshot_Task(name=snapshot_name, description=description,
                                              memory=memory, quiesce=quiesce)
                self.wait_for_task(task)
                print(f"Snapshot '{snapshot_name}' created successfully for VM '{vm_name}'.")
            except Exception as e:
                print(f"Failed to create snapshot for VM '{vm_name}': {e}")
        else:
            print(f"VM '{vm_name}' not found.")

    



    





    """
    F5 bigip and prtg link iso image in cd/dvd [keg2 podiso/pod-54-a.iso]
    """

