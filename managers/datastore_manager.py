from pyVmomi import vim
from managers.vcenter import VCenter


class DatastoreManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.is_connected():
            raise ValueError("VCenter instance is not connected. Please establish a connection first.")
        self.service_instance = vcenter_instance.connection
    
    def list_datastores(self):
        """Lists all datastores available in the connected vCenter."""
        try:
            datastores = self.get_all_objects_by_type(vim.Datastore)
            for ds in datastores:
                print(f"Datastore Name: {ds.name}, Capacity: {ds.summary.capacity}, Free Space: {ds.summary.freeSpace}")
        except Exception as e:
            print(f"Failed to list datastores: {e}")