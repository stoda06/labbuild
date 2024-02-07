from managers.vcenter import VCenter
from pyVmomi import vim
# from common import auto_requires_connection

# @auto_requires_connection
class ResourcePoolManager(VCenter):
    def create_resource_pool_under_host(self, host_name, rp_name, cpu_allocation, memory_allocation):
        """Creates a new resource pool under the specified host's default resource pool."""
        try:
            host = self.get_host_by_name(host_name)
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
            parent_rp = self.get_resource_pool_by_name(parent_resource_pool)
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

    def get_host_by_name(self, host_name):
        """Retrieve a host by its name."""
        content = self.get_content()
        host_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
        for host in host_view.view:
            if host.name == host_name:
                return host
        return None

    def list_resource_pools(self, parent_resource_pool_name):
        """Lists all resource pools under the specified parent resource pool."""
        parent_rp = self.get_resource_pool_by_name(parent_resource_pool_name)
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

    def get_resource_pool_by_name(self, rp_name):
        """Retrieve a resource pool by its name."""
        content = self.get_content()
        rp_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
        for rp in rp_view.view:
            if rp.name == rp_name:
                return rp
        return None