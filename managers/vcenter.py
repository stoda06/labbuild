from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
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
    
    def get_obj(self, vimtype, name):
        """
        Retrieves an object by name from vCenter.

        :param vimtype: A list of the vim type(s) to search for (e.g., [vim.VirtualMachine]).
        :param name: The name of the object to find.
        :return: The found object, or None if not found.
        """
        content = self.get_content()
        container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
        container_view = container.view
        container.Destroy()
        for c in container_view:
            if c.name == name:
                return c
        return None
    
    def get_all_objects_by_type(self, vimtype):
        """
        Helper function to retrieve all objects of a given type from vCenter.

        :param vimtype: The vim type to search for (e.g., vim.ResourcePool).
        :return: A list of all objects of the specified type.
        """
        container = self.connection.content.viewManager.CreateContainerView(self.connection.content.rootFolder, [vimtype], True)
        objects = list(container.view)
        container.Destroy()
        return objects
    
    def wait_for_task(self, task):
        """
        Waits for a vCenter task to finish and displays a progress bar.

        :param task: The vCenter task to wait on.
        """
        while task.info.state in [vim.TaskInfo.State.running, vim.TaskInfo.State.queued]:
            # Calculate the progress as a percentage
            progress = task.info.progress if task.info.progress else 0
            self.print_progress_bar(progress, 100, prefix = 'Progress:', suffix = 'Complete', length = 50)
            time.sleep(2)  # Sleep for a bit to avoid spamming updates

        if task.info.state == vim.TaskInfo.State.success:
            print("Operation completed successfully.")
            return True
        else:
            print("Operation failed.")
            if task.info.error:
                print(task.info.error)
            return False

    @staticmethod
    def print_progress_bar(iteration, total, prefix='', suffix='', length=100, fill='█', print_end="\r"):
        """
        Call in a loop to create terminal progress bar

        :param iteration: Current iteration
        :param total: Total iterations
        :param prefix: Prefix string
        :param suffix: Suffix string
        :param length: Character length of bar
        :param fill: Bar fill character
        :param print_end: End character (e.g. "\r", "\r\n")
        """
        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filled_length = int(length * iteration // total)
        bar = fill * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
        # Print New Line on Complete
        if iteration == total: 
            print()
