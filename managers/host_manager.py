from pyVmomi import vim, vmodl
from managers.vcenter import VCenter
# from concurrent.futures import ThreadPoolExecutor, as_completed

class HostManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger

    def get_host(self, host_name):

        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            error_message = f"Host {host_name} not found."
            self.logger.error(error_message)
            raise Exception(error_message)
        
        return host