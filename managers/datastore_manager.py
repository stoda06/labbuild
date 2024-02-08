from pyVmomi import vim
from managers.vcenter import VCenter


class DatastoreManager(VCenter):
    
    def list_datastores(self):
        """Lists all datastores available in the connected vCenter."""
        try:
            datastores = self.get_all_objects_by_type(vim.Datastore)
            for ds in datastores:
                print(f"Datastore Name: {ds.name}, Capacity: {ds.summary.capacity}, Free Space: {ds.summary.freeSpace}")
        except Exception as e:
            print(f"Failed to list datastores: {e}")