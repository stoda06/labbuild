from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim, vmodl
from pyVim.task import WaitForTask
from logger.log_config import setup_logger
import ssl
import atexit

class VCenter:
    def __init__(self, host, user, password, port=443):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.logger = setup_logger()

    def connect(self):
        """Establishes a secure connection to the vCenter server."""
        try:
            # For secure connection, SSL verification is enabled by default
            self.connection = SmartConnect(host=self.host,
                                           user=self.user,
                                           pwd=self.password,
                                           port=self.port)
            atexit.register(Disconnect, self.connection)
            self.logger.debug("Connected to vCenter server securely.")
        except ssl.SSLError as ssl_error:
            self.logger.error(f"SSL Error encountered: {ssl_error}")
            self.logger.error("Check your SSL certificate or connection settings.")
        except Exception as e:
            self.logger.error(f"Failed to connect to vCenter: {e}")
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
    
    def extract_error_message(self, exception):
        """
        Extracts a detailed error message from a vSphere API exception or falls back to the default string
        representation of the exception.

        :param exception: The exception object to extract the message from.
        :return: A detailed error message if available, or the string representation of the exception.
        """
        try:
            # Attempt to extract detailed error information
            if hasattr(exception, 'localizedMessage') and exception.localizedMessage:
                return exception.localizedMessage
            elif hasattr(exception, 'msg') and exception.msg:
                return exception.msg
            elif hasattr(exception, 'reason') and exception.reason:
                return str(exception.reason)
            elif hasattr(exception, 'faultCause') and exception.faultCause:
                return f"Fault cause: {exception.faultCause}"
            else:
                # Fall back to default string representation
                return str(exception)
        except Exception as e:
            # If any unexpected error occurs during extraction, log it and return the original exception string
            self.logger.error(f"Error extracting message from exception: {str(e)}")
            return str(exception)
    
    def wait_for_task(self, task):
        """
        Waits for a vCenter task to finish using the WaitForTask method from pyVim.task.

        :param task: The vCenter task to wait on.
        :return: True if the task completes successfully, False otherwise.
        """
        try:
            # Blocking call to wait for the task to complete
            WaitForTask(task)
            self.logger.debug("Operation completed successfully.")
            return True
        except vim.fault.VimFault as vim_error:
            # Handle known vCenter faults specifically
            error_message = vim_error.msg if hasattr(vim_error, 'msg') else str(vim_error)
            self.logger.error(f"Operation failed due to a known vCenter fault: {error_message}")
            return False
        except Exception as e:
            # Handle generic exceptions
            task_info = task.info if hasattr(task, 'info') else None
            error_details = (
                f"{task_info.error.localizedMessage}" if task_info and task_info.error else self.extract_error_message(e)
            )
            self.logger.error(f"Operation failed: {error_details}")
            return False

        
        