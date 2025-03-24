from concurrent.futures import ThreadPoolExecutor, as_completed
from managers.vcenter import VCenter
from pyVmomi import vim


class ResourcePoolManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger

    def create_resource_pool_under_host(self, host_name, rp_name, cpu_allocation, memory_allocation):
        """Creates a new resource pool under the specified host's default resource pool."""
        try:
            host = self.get_obj([vim.HostSystem], host_name)
            if host is None:
                self.logger.error(f"Host '{host_name}' not found.")
                return None

            # Use the host's default resource pool as the parent
            parent_rp = host.parent.resourcePool

            return self.create_resource_pool(parent_rp, rp_name, cpu_allocation, memory_allocation)
        except Exception as e:
            self.logger.error(f"Failed to create resource pool under host '{host_name}': {self.extract_error_message(e)}")
            return None

    def create_resource_pool(self, parent_resource_pool, rp_name, host_fqdn=None):
        """
        Creates a new resource pool under the specified parent resource pool and host.

        Returns True if the resource pool is created successfully or already exists;
        otherwise, returns False.

        :param parent_resource_pool: Name of the parent resource pool.
        :param rp_name: Name for the new resource pool.
        :param host_fqdn: (Optional) Fully Qualified Domain Name of the host to locate the parent resource pool.
        :return: bool
        """
        # Define default CPU and memory allocation configurations.
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
        
        try:
            # Retrieve the parent resource pool based on host_fqdn availability.
            if host_fqdn:
                # Locate the ESXi host using its FQDN.
                host = self.get_obj([vim.HostSystem], host_fqdn)
                if not host:
                    self.logger.error(f"Host '{host_fqdn}' not found.")
                    return False

                # Search for the specified parent resource pool under the host.
                parent_rp = None
                for rp in host.parent.resourcePool.resourcePool:
                    if rp.name == parent_resource_pool:
                        parent_rp = rp
                        break

                if not parent_rp:
                    self.logger.error(f"Resource pool '{parent_resource_pool}' not found on host '{host_fqdn}'.")
                    return False
            else:
                # Retrieve the parent resource pool globally if no host is specified.
                parent_rp = self.get_obj([vim.ResourcePool], parent_resource_pool)
                if not parent_rp:
                    self.logger.error(f"Parent Resource pool '{parent_resource_pool}' not found.")
                    return False

            # Check if the resource pool already exists under the parent.
            for rp in parent_rp.resourcePool:
                if rp.name == rp_name:
                    self.logger.warning(f"Resource pool '{rp_name}' already exists. Skipping creation.")
                    return True

            # Configure resource allocation specifications.
            resource_config_spec = vim.ResourceConfigSpec()

            # CPU Allocation Configuration
            cpu_alloc = vim.ResourceAllocationInfo()
            cpu_alloc.limit = cpu_allocation['limit']
            cpu_alloc.reservation = cpu_allocation['reservation']
            cpu_alloc.expandableReservation = cpu_allocation['expandable_reservation']
            cpu_alloc.shares = vim.SharesInfo(level=vim.SharesInfo.Level.normal, shares=cpu_allocation['shares'])
            resource_config_spec.cpuAllocation = cpu_alloc

            # Memory Allocation Configuration
            memory_alloc = vim.ResourceAllocationInfo()
            memory_alloc.limit = memory_allocation['limit']
            memory_alloc.reservation = memory_allocation['reservation']
            memory_alloc.expandableReservation = memory_allocation['expandable_reservation']
            memory_alloc.shares = vim.SharesInfo(level=vim.SharesInfo.Level.normal, shares=memory_allocation['shares'])
            resource_config_spec.memoryAllocation = memory_alloc

            # Create the resource pool under the parent with the defined specification.
            parent_rp.CreateResourcePool(name=rp_name, spec=resource_config_spec)
            self.logger.debug(f"Resource pool '{rp_name}' created successfully under {parent_rp.name}.")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create resource pool: {self.extract_error_message(e)}")
            return False

    def delete_resource_pool(self, rp_name):
        """Deletes the specified resource pool and all its child components,
        ensuring that all VMs in the resource pool and its children are deleted first."""
        try:
            rp = self.get_obj([vim.ResourcePool], rp_name)
            if rp is None:
                self.logger.error(f"Resource pool '{rp_name}' not found.")
                return False

            # First, recursively delete all child resource pools.
            for child_rp in rp.resourcePool:
                child_rp_name = child_rp.name
                self.logger.debug(f"Deleting child resource pool '{child_rp_name}' within '{rp_name}'.")
                if not self.delete_resource_pool(child_rp_name):
                    self.logger.error(f"Failed to delete child resource pool '{child_rp_name}' within '{rp_name}'.")
                    return False

            # Define a function to delete a single VM.
            def delete_vm(vm):
                vm_name = vm.name
                self.logger.debug(f"Deleting VM '{vm_name}' within resource pool '{rp_name}'.")
                try:
                    task = vm.Destroy_Task()
                    self.wait_for_task(task)
                    self.logger.debug(f"VM '{vm_name}' deleted successfully.")
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to delete VM '{vm_name}': {self.extract_error_message(e)}")
                    return False

            # Delete all VMs within the current resource pool concurrently and wait for all to finish.
            deletion_successful = True
            if rp.vm:
                with ThreadPoolExecutor(max_workers=len(rp.vm)) as executor:
                    future_to_vm = {executor.submit(delete_vm, vm): vm for vm in rp.vm}
                    for future in as_completed(future_to_vm):
                        if not future.result():
                            deletion_successful = False

                if not deletion_successful:
                    self.logger.error(f"One or more VMs failed to delete in resource pool '{rp_name}'. Aborting deletion of resource pool.")
                    return False

            # Only after all VMs are confirmed deleted, delete the resource pool itself.
            task = rp.Destroy_Task()
            self.wait_for_task(task)
            self.logger.debug(f"Resource pool '{rp_name}' deleted successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete resource pool '{rp_name}': {self.extract_error_message(e)}")
            return False
        
    def assign_role_to_resource_pool(self, resource_pool_name, user_name, role_name, propagate=True):
        """
        Assigns a specified role to a user on a given resource pool.
        
        Returns True if the role is assigned successfully (or already assigned) 
        and False if any error occurs during the assignment.

        :param resource_pool_name: The name of the resource pool.
        :param user_name: The identifier of the user (e.g., "DOMAIN\\User").
        :param role_name: The name of the role to assign.
        :param propagate: Boolean indicating whether the permission should propagate to child objects.
        :return: bool
        """
        # Find the role by name.
        role_id = None
        for role in self.connection.content.authorizationManager.roleList:
            if role.name == role_name:
                role_id = role.roleId
                break

        if role_id is None:
            self.logger.error(f"Role '{role_name}' not found.")
            return False

        # Retrieve the resource pool object by its name.
        resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
        if resource_pool is None:
            self.logger.error(f"Resource pool '{resource_pool_name}' not found.")
            return False

        # Retrieve current (non-inherited) permissions for the resource pool.
        current_permissions = self.connection.content.authorizationManager.RetrieveEntityPermissions(
            entity=resource_pool, inherited=False)

        # Check if the user already has the specified role with the same propagation setting.
        for perm in current_permissions:
            if perm.principal == user_name and perm.roleId == role_id and perm.propagate == propagate:
                self.logger.debug(f"User '{user_name}' already has role '{role_name}' on resource pool '{resource_pool_name}' with identical propagation setting.")
                return True  # Role assignment already exists.

        # Create the permission specification.
        permission = vim.AuthorizationManager.Permission(
            principal=user_name,
            group=False,  # Set to True if assigning permissions to a user group.
            roleId=role_id,
            propagate=propagate  # Determines if the permission should propagate to child objects.
        )

        # Attempt to set the permission on the resource pool.
        try:
            self.connection.content.authorizationManager.SetEntityPermissions(entity=resource_pool, permission=[permission])
            self.logger.debug(f"Assigned role '{role_name}' to user '{user_name}' on resource pool '{resource_pool_name}'.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to assign role to user on resource pool: {self.extract_error_message(e)}")
            return False
    
    def poweroff_all_vms(self, resource_pool_name):
        """
        Powers off all VMs in the specified resource pool and all its nested resource pools.
        
        :param resource_pool_name: Name of the resource pool.
        """
        # Get the resource pool object
        resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
        if not resource_pool:
            self.logger.error(f"Resource pool {resource_pool_name} not found.")
            return

        def traverse_resource_pool(pool):
            """
            Recursively traverse the resource pool and collect all VMs.
            :param pool: Resource pool object
            :return: List of VMs in the resource pool and its nested resource pools
            """
            vm_list = list(pool.vm)  # Get all VMs in the current resource pool
            for nested_pool in pool.resourcePool:  # Recursively traverse nested pools
                vm_list.extend(traverse_resource_pool(nested_pool))
            return vm_list

        # Get all VMs in the resource pool and nested resource pools
        all_vms = traverse_resource_pool(resource_pool)

        # Power off all VMs
        for vm in all_vms:
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                try:
                    task = vm.PowerOffVM_Task()
                    self.logger.debug(f"Powering off VM: {vm.name}")
                    self.wait_for_task(task)
                    self.logger.debug(f"VM {vm.name} powered off successfully.")
                except Exception as e:
                    self.logger.error(f"Failed to power off VM {vm.name}: {e}")
            else:
                self.logger.debug(f"VM {vm.name} is already powered off.")
