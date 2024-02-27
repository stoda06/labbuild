from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask
from logger.log_config import setup_logger
import ssl
import atexit
import time


class VCenter:
    def __init__(self, host, user, password, port=443):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.logger = setup_logger(__name__)

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
    
    def is_connected(self):
        """Checks if the service instance is connected."""
        return self.connection is not None

    def get_content(self):
        """Retrieves the root folder from vCenter."""
        return self.connection.RetrieveContent()
    
    def get_obj(self, vimtype, name):
        """
        Retrieves an object by name from vCenter using a more efficient search mechanism.
        """
        content = self.get_content()
        try:
            container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
            # Preparing the property collector's filter specs
            property_spec = vmodl.query.PropertyCollector.PropertySpec(type=vimtype[0], pathSet=["name"], all=False)
            traversal_spec = vmodl.query.PropertyCollector.TraversalSpec(name='traverseEntities', path='view', skip=False, type=vim.view.ContainerView)
            object_spec = vmodl.query.PropertyCollector.ObjectSpec(obj=container, skip=True, selectSet=[traversal_spec])
            filter_spec = vmodl.query.PropertyCollector.FilterSpec(objectSet=[object_spec], propSet=[property_spec])
            
            # Retrieving properties
            collector = content.propertyCollector
            props = collector.RetrieveContents([filter_spec])

            # Searching for the object by name
            for obj in props:
                if obj.propSet[0].val == name:
                    return obj.obj
        finally:
            if 'container' in locals():
                container.Destroy()

        return None
    
    def get_all_objects_by_type(self, vimtype):
        """
        Retrieves all objects of a given type from vCenter using Property Collector for efficiency.

        :param vimtype: The vim type to search for (e.g., vim.ResourcePool).
        :return: A list of all objects of the specified type.
        """
        content = self.connection.RetrieveContent()
        property_collector = content.propertyCollector
        view_manager = content.viewManager

        # Create a container view for the desired vimtype
        container_view = view_manager.CreateContainerView(content.rootFolder, [vimtype], True)

        # Create a traversal spec to navigate from the container view to its view property
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec(
            name='traverseView',
            path='view',
            skip=False,
            type=vim.view.ContainerView
        )

        # Specify which properties we want to retrieve (name is used as an example, can be adjusted)
        property_spec = vmodl.query.PropertyCollector.PropertySpec(
            type=vimtype,
            pathSet=['name'],  # Specify more properties if needed
            all=False
        )

        # Combine traversal spec and property spec into a filter spec
        object_spec = vmodl.query.PropertyCollector.ObjectSpec(
            obj=container_view,
            skip=True,
            selectSet=[traversal_spec]
        )

        filter_spec = vmodl.query.PropertyCollector.FilterSpec(
            objectSet=[object_spec],
            propSet=[property_spec]
        )

        # Retrieve all objects of the specified type
        retrieved_objects = property_collector.RetrieveContents([filter_spec])

        # Cleanup the container view
        container_view.Destroy()

        # Extract and return the actual objects from the retrieved properties
        objects = [obj.obj for obj in retrieved_objects]
        return objects
    
    def wait_for_task(self, task):
        """
        Waits for a vCenter task to finish using the WaitForTask method from pyVim.task.

        :param task: The vCenter task to wait on.
        """
        try:
            WaitForTask(task)  # This will block until the task is complete
            print("Operation completed successfully.")
            return True
        except Exception as e:
            print(f"Operation failed: {e}")
            return False