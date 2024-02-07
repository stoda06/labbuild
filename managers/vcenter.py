from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import atexit
import functools

def requires_connection(func):
    """Decorator to ensure a vCenter connection is established before calling the method."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.connection:
            print("Not connected to vCenter. Please establish a connection first.")
            return None
        return func(self, *args, **kwargs)
    return wrapper

class VCenter:
    def __init__(self, host, user, password, port=443):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        """Establishes a secure connection to the vCenter server."""
        try:
            # For secure connection, SSL verification is enabled by default
            self.connection = SmartConnect(host=self.host,
                                           user=self.user,
                                           pwd=self.password,
                                           port=self.port)
            atexit.register(Disconnect, self.connection)
            print("Connected to vCenter server securely.")
        except ssl.SSLError as ssl_error:
            print(f"SSL Error encountered: {ssl_error}")
            print("Check your SSL certificate or connection settings.")
        except Exception as e:
            print(f"Failed to connect to vCenter: {e}")
            self.connection = None

    @requires_connection
    def get_content(self):
        """Retrieves the root folder from vCenter."""
        return self.connection.RetrieveContent()

    @requires_connection
    def get_hosts(self):
        content = self.get_content()
        host_view = content.viewManager.CreateContainerView(content.rootFolder,[vim.HostSystem], True)
        hosts = host_view.view
        host_view.Destroy() # Clean up the view
        
        return hosts
    
    @requires_connection
    def get_datacenters(self):
        """Retrieves all datacenters from vCenter."""
        content = self.get_content()
        datacenter_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Datacenter], True)
        datacenters = datacenter_view.view
        datacenter_view.Destroy()  # Clean up the view
        
        return datacenters
    
    @requires_connection
    def get_resource_pool(self, host_name):
        """Retrieves all resource pools for the given host name, including nested pools."""
        hosts = self.get_hosts()  # Retrieve all hosts
        host = next((h for h in hosts if h.name == host_name), None)

        if not host:
            print(f"Host '{host_name}' not found.")
            return []

        # Navigate from the host to its parent compute resource to find the root resource pool
        if hasattr(host, 'parent') and isinstance(host.parent, vim.ComputeResource):
            root_resource_pool = host.parent.resourcePool
            if root_resource_pool:
                return self._get_nested_rps(root_resource_pool)
            else:
                print(f"No resource pool found for host '{host_name}'.")
                return []
        else:
            print(f"Host '{host_name}' is not part of a cluster with resource pools.")
            return []

    def _get_nested_rps(self, resource_pool):
        """Recursively retrieves all nested resource pools starting from a given resource pool."""
        rps = [resource_pool]
        for rp in resource_pool.resourcePool:
            rps.extend(self._get_nested_rps(rp))
        return rps