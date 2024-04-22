import urllib3
import requests
from pyVmomi import vim, vmodl
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            if not base_vm:
                self.logger.error(f"Base VM '{base_name}' not found.")
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

            task = base_vm.CloneVM_Task(folder=vm_folder, name=clone_name, spec=clone_spec)
            self.logger.debug(f"VM '{clone_name}' cloning started from base VM '{base_name}' into folder '{directory_name}'.")
            if self.wait_for_task(task):
                self.logger.debug(f"VM '{clone_name}' cloned successfully from base VM '{base_name}' into folder '{directory_name}'.")
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
    
    def update_mac_address(self, vm_name, adapter_label, new_mac_address):
        """
        Updates the MAC address of a specified network adapter on a VM without altering its network connection,
        and logs the current network name along with the adapter label.

        :param vm_name: The name of the VM to update.
        :param adapter_label: The label of the network adapter (e.g., "Network adapter 1").
        :param new_mac_address: The new MAC address to assign to the adapter.
        :return: True if the MAC address was successfully updated, False otherwise.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            return False
        
        # Find the specified network adapter
        nic_spec = None
        current_network_name = None  # Initialize variable to store current network name
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard) and device.deviceInfo.label == adapter_label:
                # Attempt to extract current network name
                if hasattr(device.backing, 'network'):
                    network = device.backing.network
                    if network:
                        current_network_name = network.name if hasattr(network, 'name') else 'Unknown Network'
                
                nic_spec = vim.vm.device.VirtualDeviceSpec()
                nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
                nic_spec.device = device
                nic_spec.device.macAddress = new_mac_address
                nic_spec.device.addressType = 'manual'  # Set custom MAC
                nic_spec.device.wakeOnLanEnabled = device.wakeOnLanEnabled
                nic_spec.device.backing = device.backing  # Preserve network connection
                break

        if not nic_spec:
            self.logger.error(f"Network adapter '{adapter_label}' not found on VM '{vm_name}'.")
            return False

        # Apply the configuration change
        config_spec = vim.vm.ConfigSpec(deviceChange=[nic_spec])
        task = vm.ReconfigVM_Task(config_spec)
        if self.wait_for_task(task):
            self.logger.debug(f"MAC address of '{adapter_label}' on VM '{vm_name}' updated to '{new_mac_address}'. Current network: {current_network_name}.")
            return True
        else:
            self.logger.error(f"Failed to change MAC address for '{adapter_label}' on VM '{vm_name}'.")
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

    def update_vm_networks(self, vm_name, pod_number):
        """
        Updates the networks of an existing VM by changing the vSwitch number in the network names
        that contain a 'vs' pattern. Networks without 'vs' in their names are ignored.

        :param vm_name: The name of the VM to update.
        :param folder_name: The name of the folder containing the VM.
        :param new_vs_number: The new vSwitch number to apply to the VM's network interfaces.
        """
        vm = self.get_obj([vim.VirtualMachine], vm_name)
        if not vm:
            self.logger.error(f"VM '{vm_name}' not found.")
            raise ValueError(f"VM '{vm_name}' not found.")

        # Ensure VM is powered off for changes
        if vm.runtime.powerState == vim.VirtualMachine.PowerState.poweredOn:
            self.logger.warning(f"Powering off VM '{vm_name}' for network update.")
            self.wait_for_task(vm.PowerOffVM_Task())

        device_changes = []
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                current_network_name = device.backing.deviceName
                network_backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                # Check if 'vs' is in the current network name
                if 'vs' in current_network_name:
                    # Perform string manipulation to change only the 'vs' number part of the network name
                    prefix, suffix = current_network_name.rsplit('vs0', 1)
                    new_network_name = f"{prefix}vs{pod_number}{suffix}"

                    # Set up network backing with the new network name
                    network_backing.deviceName = new_network_name

                if 'internal' in current_network_name:
                    new_network_name = f"pa-internal-cortex-{pod_number}"
                    # Set up network backing with the new network name
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
                self.logger.debug(f"Network interfaces on VM '{vm_name}' updated successfully.")
            else:
                self.logger.error(f"No network interface changes detected or applicable on VM {vm_name}.")
        except vmodl.MethodFault as error:
            self.logger.error(f"Failed to update network interfaces for VM '{vm_name}': {error.msg}")
            raise Exception(f"Failed to update network interfaces for VM '{vm_name}': {error.msg}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while updating VM '{vm_name}': {str(e)}")
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
            self.logger.warning(f"VM '{vm_name}' is powered on. Attempting to power off before deletion.")
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
        
    def create_linked_clone(self, base_vm_name, clone_name, snapshot_name, resource_pool_name, datastore_name=None):
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
            base_vm = self.get_obj([vim.VirtualMachine], base_vm_name)
            if not base_vm:
                self.logger.error(f"Base VM '{base_vm_name}' not found.")
                return False

            if not base_vm.snapshot:
                self.logger.error(f"No snapshots found for VM '{base_vm_name}'.")
                return False

            snapshot = self.find_snapshot_in_tree(base_vm, snapshot_name)
            if not snapshot:
                self.logger.error(f"Snapshot '{snapshot_name}' not found in VM '{base_vm_name}'.")
                return False

            resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
            if not resource_pool:
                self.logger.error(f"Resource pool '{resource_pool_name}' not found.")
                return False

            datastore = self.get_obj([vim.Datastore], datastore_name) if datastore_name else base_vm.datastore[0]

            clone_spec = vim.vm.CloneSpec()
            clone_spec.location = vim.vm.RelocateSpec()
            clone_spec.location.pool = resource_pool
            clone_spec.location.datastore = datastore
            clone_spec.location.diskMoveType = 'createNewChildDiskBacking'
            clone_spec.snapshot = snapshot

            task = base_vm.CloneVM_Task(folder=base_vm.parent, name=clone_name, spec=clone_spec)
            if self.wait_for_task(task):
                self.logger.info(f"Linked clone '{clone_name}' created successfully.")
                return True
        except Exception as e:
            self.logger.error(f"Failed to create linked clone: {e}")
            return False