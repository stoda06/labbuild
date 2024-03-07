from pyVmomi import vim, vmodl
from managers.vcenter import VCenter

class FolderManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger

    def create_folder(self, parent_folder_name, new_folder_name):
        """
        Creates a new folder under a specified parent folder.

        :param parent_folder_name: The name of the parent folder.
        :param new_folder_name: The name of the new folder to be created.
        :return: A message indicating success or failure.
        """
        parent_folder = self.get_obj([vim.Folder], parent_folder_name)
        if not parent_folder:
            self.logger.error(f"Parent folder '{parent_folder_name}' not found.")
            raise ValueError(f"Parent folder '{parent_folder_name}' not found.")

        for child in parent_folder.childEntity:
            if isinstance(child, vim.Folder) and child.name == new_folder_name:
                self.logger.info(f"Folder '{new_folder_name}' already exists under '{parent_folder_name}'.")
                return None

        try:
            new_folder = parent_folder.CreateFolder(name=new_folder_name)
            self.logger.info(f"Folder '{new_folder_name}' created successfully under '{parent_folder_name}'.")
            return new_folder
        except vim.fault.DuplicateName:
            self.logger.error(f"A folder with the name '{new_folder_name}' already exists.")
            return None
        except vim.fault.InvalidName:
            self.logger.error(f"The folder name '{new_folder_name}' is invalid.")
            return None
        except Exception as e:
            self.logger.error(f"Failed to create folder '{new_folder_name}': {e}")
            return None

    def assign_user_to_folder(self, folder_name, user_name, role_name, propagate=True):
        """
        Assigns a user to a folder with a specified role.

        :param folder_name: The name of the folder to assign the user to.
        :param user_name: The name of the user to assign.
        :param role_name: The name of the role to assign to the user.
        :param propagate: Whether the role should propagate down to child objects.
        """
        folder = self.get_obj([vim.Folder], folder_name)
        if not folder:
            self.logger.error(f"Folder '{folder_name}' not found.")
            raise ValueError(f"Folder '{folder_name}' not found.")

        role_id = self.get_role_id(role_name)
        if role_id is None:
            self.logger.error(f"Role '{role_name}' not found.")
            raise ValueError(f"Role '{role_name}' not found.")

        permission = vim.AuthorizationManager.Permission()
        permission.principal = user_name
        permission.group = False
        permission.roleId = role_id
        permission.propagate = propagate

        try:
            self.connection.content.authorizationManager.SetEntityPermissions(entity=folder, permission=[permission])
            self.logger.info(f"Assigned role '{role_name}' to user '{user_name}' on folder '{folder_name}'.")
        except vmodl.MethodFault as error:
            self.logger.error(f"Failed to assign user to folder: {error.msg}")
            raise

    def get_role_id(self, role_name):
        """
        Retrieve the role ID for a given role name.
        """
        role_manager = self.connection.content.authorizationManager
        for role in role_manager.roleList:
            if role.name == role_name:
                return role.roleId
        self.logger.error(f"Role '{role_name}' not found.")
        return None