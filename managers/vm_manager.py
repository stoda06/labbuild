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

    # Implement get_obj, wait_for_task, and other necessary methods as shown in previous examples
    def clone_vm(self, template_name, clone_name, resource_pool_name, datastore_name=None, datacenter_name=None, power_on=False):
        """
        Clones a VM from an existing template.

        :param template_name: The name of the template to clone from.
        :param clone_name: The name for the cloned VM.
        :param resource_pool_name: The name of the resource pool where the cloned VM will be located.
        :param datastore_name: Optional. The name of the datastore where the cloned VM will be stored. Uses template's datastore if None.
        :param datacenter_name: Optional. The name of the datacenter where the cloned VM will be located. Used for VM folder lookup.
        :param power_on: Whether to power on the cloned VM after creation.
        """
        try:
            template_vm = self.get_obj([vim.VirtualMachine], template_name)
            if not template_vm:
                print(f"Template '{template_name}' not found.")
                return

            resource_pool = self.find_resource_pool_by_name(resource_pool_name)
            if not resource_pool:
                print(f"Resource pool '{resource_pool_name}' not found.")
                return

            # If a specific datastore is specified, find it; otherwise, use the template's datastore
            if datastore_name:
                datastore = self.get_obj([vim.Datastore], datastore_name)
                if not datastore:
                    print(f"Datastore '{datastore_name}' not found.")
                    return
            else:
                datastore = template_vm.datastore[0]  # Assuming the template is associated with a single datastore

            # Find the datacenter's VM folder if specified, otherwise use the template's folder
            if datacenter_name:
                datacenter = self.get_obj([vim.Datacenter], datacenter_name)
                vm_folder = datacenter.vmFolder if datacenter else None
            else:
                vm_folder = template_vm.parent

            if not vm_folder:
                print("VM folder not found.")
                return

            clone_spec = vim.vm.CloneSpec()
            clone_spec.location = vim.vm.RelocateSpec()
            clone_spec.location.pool = resource_pool
            clone_spec.location.datastore = datastore
            clone_spec.powerOn = power_on

            # Execute the clone task
            clone_task = template_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            self.wait_for_task(clone_task)
            print(f"VM '{clone_name}' cloned successfully from template '{template_name}'.")
        except Exception as e:
            print(f"Failed to clone VM '{clone_name}': {e}")

    def list_vms(self):
        """Lists all virtual machines available in the connected vCenter."""
        try:
            vms = self.get_all_objects_by_type(vim.VirtualMachine)
            for vm in vms:
                print(f"VM Name: {vm.name}, Power State: {vm.runtime.powerState}")
        except Exception as e:
            print(f"Failed to list VMs: {e}")