from managers.vcenter import VCenter
from pyVmomi import vim


class ResourcePoolManager(VCenter):

    def create_resource_pool_under_host(self, host_name, rp_name, cpu_allocation, memory_allocation):
        """Creates a new resource pool under the specified host's default resource pool."""
        try:
            host = self.get_obj([vim.HostSystem], host_name)
            if host is None:
                print(f"Host '{host_name}' not found.")
                return None

            # Use the host's default resource pool as the parent
            parent_rp = host.parent.resourcePool

            return self.create_resource_pool(parent_rp, rp_name, cpu_allocation, memory_allocation)
        except Exception as e:
            print(f"Failed to create resource pool under host '{host_name}': {e}")
            return None

    def create_resource_pool(self, parent_resource_pool, rp_name, cpu_allocation, memory_allocation):
        """Creates a new resource pool under the specified parent resource pool."""
        try:
            parent_rp = self.get_obj([vim.ResourcePool], parent_resource_pool)
            if parent_rp is None:
                print(f"Parent Resource pool '{parent_resource_pool} not found.")

            resource_config_spec = vim.ResourceConfigSpec()

            # CPU Allocation
            cpu_alloc = vim.ResourceAllocationInfo()
            cpu_alloc.limit = cpu_allocation['limit']
            cpu_alloc.reservation = cpu_allocation['reservation']
            cpu_alloc.expandableReservation = cpu_allocation['expandable_reservation']
            cpu_alloc.shares = vim.SharesInfo(level=vim.SharesInfo.Level.normal, shares=cpu_allocation['shares'])
            resource_config_spec.cpuAllocation = cpu_alloc

            # Memory Allocation
            memory_alloc = vim.ResourceAllocationInfo()
            memory_alloc.limit = memory_allocation['limit']
            memory_alloc.reservation = memory_allocation['reservation']
            memory_alloc.expandableReservation = memory_allocation['expandable_reservation']
            memory_alloc.shares = vim.SharesInfo(level=vim.SharesInfo.Level.normal, shares=memory_allocation['shares'])
            resource_config_spec.memoryAllocation = memory_alloc
            resource_pool = parent_rp.CreateResourcePool(name=rp_name, spec=resource_config_spec)
            

            print(f"Resource pool '{rp_name}' created successfully under {parent_rp.name}.")
            return resource_pool
        except Exception as e:
            print(f"Failed to create resource pool: {e}")
            return None

    def list_resource_pools(self, parent_resource_pool_name):
        """Lists all resource pools under the specified parent resource pool."""
        parent_rp = self.get_obj([vim.ResourcePool], parent_resource_pool_name)
        if parent_rp is None:
            print(f"Resource pool '{parent_resource_pool_name}' not found.")
            return []
        return self._list_rps(parent_rp)
    
    def _list_rps(self, resource_pool, rps=None):
        """Recursively lists all resource pools starting from a given resource pool."""
        if rps is None:
            rps = []
        rps.append(resource_pool.name)
        for rp in resource_pool.resourcePool:
            self._list_rps(rp, rps)
        return rps
    
    def delete_resource_pool(self, rp_name):
        """Deletes the specified resource pool."""
        try:
            rp = self.get_obj([vim.ResourcePool], rp_name)
            if rp is None:
                print(f"Resource pool '{rp_name}' not found.")
                return False

            task = rp.Destroy_Task()
            self.wait_for_task(task)
            print(f"Resource pool '{rp_name}' deleted successfully.")
            return True
        except Exception as e:
            print(f"Failed to delete resource pool '{rp_name}': {e}")
            return False

    def assign_role_to_resource_pool(self, resource_pool_name, user_name, role_name):
        """Assigns a specified role to a user on a given resource pool.

        :param resource_pool_name: The name of the resource pool.
        :param user_name: The identifier of the user (e.g., "DOMAIN\\User").
        :param role_name: The name of the role to assign.
        """
        # Find the role by name
        role_id = None
        for role in self.connection.content.authorizationManager.roleList:
            if role.name == role_name:
                role_id = role.roleId
                break

        if role_id is None:
            print(f"Role '{role_name}' not found.")
            return

        # Find the resource pool by name
        resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
        if resource_pool is None:
            print(f"Resource pool '{resource_pool_name}' not found.")
            return

        # Create the permission spec
        permission = vim.AuthorizationManager.Permission(
            principal=user_name,
            group=False,  # Change to True if assigning permissions to a user group
            roleId=role_id,
            propagate=True  # Whether or not the permission should propagate to child objects
        )

        # Set the permission on the resource pool
        try:
            self.connection.content.authorizationManager.SetEntityPermissions(entity=resource_pool, permission=[permission])
            print(f"Assigned role '{role_name}' to user '{user_name}' on resource pool '{resource_pool_name}'.")
        except Exception as e:
            print(f"Failed to assign role to user on resource pool: {e}")
