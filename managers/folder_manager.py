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

        Returns True if the folder is created successfully or already exists;
        otherwise, returns False.

        :param parent_folder_name: The name of the parent folder.
        :param new_folder_name: The name of the new folder to be created.
        :return: bool
        """
        # Retrieve the parent folder object.
        parent_folder = self.get_obj([vim.Folder], parent_folder_name)
        if not parent_folder:
            self.logger.error(f"Parent folder '{parent_folder_name}' not found.")
            return False

        # Check if the folder already exists under the parent.
        for child in parent_folder.childEntity:
            if isinstance(child, vim.Folder) and child.name == new_folder_name:
                self.logger.debug(f"Folder '{new_folder_name}' already exists under '{parent_folder_name}'.")
                return True

        try:
            # Attempt to create the new folder.
            parent_folder.CreateFolder(name=new_folder_name)
            self.logger.debug(f"Folder '{new_folder_name}' created successfully under '{parent_folder_name}'.")
            return True
        except vim.fault.DuplicateName:
            self.logger.error(f"A folder with the name '{new_folder_name}' already exists.")
            return False
        except vim.fault.InvalidName:
            self.logger.error(f"The folder name '{new_folder_name}' is invalid.")
            return False
        except Exception as e:
            self.logger.error(f"Failed to create folder '{new_folder_name}': {e}")
            return False

    def assign_user_to_folder(self, folder_name, user_name, role_name, propagate=True):
        """
        Assigns a user to a folder with a specified role. Returns True if the assignment is successful or already set, False otherwise.

        :param folder_name: The name of the folder to assign the user to.
        :param user_name: The name of the user to assign.
        :param role_name: The name of the role to assign to the user.
        :param propagate: Whether the role should propagate down to child objects.
        :return: True if the role is successfully assigned or already correctly assigned, False if an error occurs.
        """
        folder = self.get_obj([vim.Folder], folder_name)
        if not folder:
            self.logger.error(f"Folder '{folder_name}' not found.")
            return False

        role_id = self.get_role_id(role_name)
        if role_id is None:
            self.logger.error(f"Role '{role_name}' not found.")
            return False

        # Retrieve current permissions of the folder
        try:
            current_permissions = self.connection.content.authorizationManager.RetrieveEntityPermissions(entity=folder, inherited=False)
            # Check if the user already has the specified role assigned with the exact permissions
            for perm in current_permissions:
                if perm.principal == user_name and perm.roleId == role_id and perm.propagate == propagate:
                    self.logger.debug(f"User '{user_name}' already has role '{role_name}' on folder '{folder_name}' with identical propagation setting.")
                    return True  # Skip the assignment since it's already correctly set
        except Exception as e:
            self.logger.error(f"Error retrieving permissions for folder '{folder_name}': {e}")
            return False

        # Define new permission
        permission = vim.AuthorizationManager.Permission()
        permission.principal = user_name
        permission.group = False
        permission.roleId = role_id
        permission.propagate = propagate

        # Try to assign the permission
        try:
            self.connection.content.authorizationManager.SetEntityPermissions(entity=folder, permission=[permission])
            self.logger.debug(f"Assigned role '{role_name}' to user '{user_name}' on folder '{folder_name}'.")
            return True
        except vmodl.MethodFault as error:
            self.logger.error(f"Failed to assign user to folder: {error.msg}")
            return False

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