from pyVmomi import vim, vmodl
from managers.vcenter import VCenter


class VmManager(VCenter):
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
            resource_pool = self.get_resource_pool_by_name(resource_pool_name)
            if not resource_pool:
                print(f"Resource pool '{resource_pool_name}' not found.")
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
            print(f"VM '{vm_name}' created successfully in resource pool '{resource_pool_name}'.")
        except Exception as e:
            print(f"Failed to create VM '{vm_name}': {e}")

    def clone_vm(self, template_name, clone_name, resource_pool_name, directory_name, datastore_name=None, power_on=False):
        """
        Clones a VM from an existing template into a specified directory (VM folder).

        :param template_name: The name of the template to clone from.
        :param clone_name: The name for the cloned VM.
        :param resource_pool_name: The name of the resource pool where the cloned VM will be located.
        :param directory_name: The name of the directory (VM folder) where the cloned VM will be placed.
        :param datastore_name: Optional. The name of the datastore where the cloned VM will be stored. Uses template's datastore if None.
        :param power_on: Whether to power on the cloned VM after creation.
        """
        try:
            template_vm = self.get_obj([vim.VirtualMachine], template_name)
            if not template_vm:
                print(f"Template '{template_name}' not found.")
                return

            resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
            if not resource_pool:
                print(f"Resource pool '{resource_pool_name}' not found.")
                return

            vm_folder = self.get_obj([vim.Folder], directory_name)
            if not vm_folder:
                print(f"VM Folder '{directory_name}' not found.")
                return

            if datastore_name:
                datastore = self.get_obj([vim.Datastore], datastore_name)
                if not datastore:
                    print(f"Datastore '{datastore_name}' not found.")
                    return
            else:
                datastore = template_vm.datastore[0]

            clone_spec = vim.vm.CloneSpec()
            clone_spec.location = vim.vm.RelocateSpec()
            clone_spec.location.pool = resource_pool
            clone_spec.location.datastore = datastore
            clone_spec.powerOn = power_on

            task = template_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            self.wait_for_task(task)
            print(f"VM '{clone_name}' cloned successfully from template '{template_name}' into folder '{directory_name}'.")
        except Exception as e:
            print(f"Failed to clone VM '{clone_name}': {e}")

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
    
    def update_network_adapter(self, vm_name, network_interface_label, new_mac_address=None):
        """
        Updates properties of a specified network adapter on a VM. Currently supports updating the MAC address.

        :param vm_name: The name of the VM.
        :param network_interface_label: The label/name of the network interface to update (e.g., "Network adapter 1").
        :param new_mac_address: Optional. The new MAC address to assign to the network interface.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            print(f"VM '{vm_name}' not found.")
            return

        # Check if the VM is powered off
        if vm.runtime.powerState != vim.VirtualMachine.PowerState.poweredOff:
            print(f"VM '{vm_name}' must be powered off to modify network adapters.")
            return

        network_adapters = self.get_network_adapters(vm_name)
        adapter_to_update = None

        # Find the network adapter by its label/name
        for adapter in network_adapters:
            if adapter.deviceInfo.label == network_interface_label:
                adapter_to_update = adapter
                break

        if not adapter_to_update:
            print(f"Network adapter '{network_interface_label}' not found in VM '{vm_name}'.")
            return

        def update_mac_method(adapter, new_mac):
            """Nested method to update the MAC address of a network adapter."""
            adapter.macAddress = new_mac
            adapter.addressType = 'manual'

        # Create a specification for reconfiguring the VM
        spec = vim.vm.ConfigSpec()
        nic_change_spec = vim.vm.device.VirtualDeviceSpec()
        nic_change_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
        nic_change_spec.device = adapter_to_update

        # Update the MAC address if provided
        if new_mac_address:
            update_mac_method(nic_change_spec.device, new_mac_address)

        spec.deviceChange = [nic_change_spec]

        # Execute the reconfiguration task
        task = vm.ReconfigVM_Task(spec=spec)
        self.wait_for_task(task)
        print(f"Updated network adapter '{network_interface_label}' in VM '{vm_name}'.")
