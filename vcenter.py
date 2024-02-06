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
        if self.connection:
            return self.connection.RetrieveContent()
        else:
            print("Not connected to vCenter.")
            return None

    # You can add more methods here to interact with vCenter
    def get_hosts(self, content):
        container = content.viewManager.CreateContainerView(content.rootFolder,[vim.HostSystem], True)

        hosts_view = container.view
        hosts = []
        for host in hosts_view:
            hosts.append(host.name)
        
        return hosts