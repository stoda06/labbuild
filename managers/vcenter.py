from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import atexit


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

    def get_content(self):
        """Retrieves the root folder from vCenter."""
        return self.connection.RetrieveContent()
    
    def get_host_by_name(self, host_name):
        """Retrieve a host by its name."""
        content = self.connection.RetrieveContent()
        host_container = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
        host_view = host_container.view
        host_container.Destroy()
        for host in host_view:
            if host.name == host_name:
                return host
        print(f"Host '{host_name}' not found.")
        return None
    
    def get_resource_pool_by_name(self, rp_name):
        """Retrieve a resource pool by its name."""
        content = self.get_content()
        rp_container = content.viewManager.CreateContainerView(content.rootFolder, [vim.ResourcePool], True)
        rp_view = rp_container.view
        rp_container.Destroy()
        for rp in rp_view.view:
            if rp.name == rp_name:
                return rp
        print(f"Resource '{rp_name}' not found.")
        return None