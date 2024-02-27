from pyVmomi import vim, vmodl
from managers.vcenter import VCenter

class PermissionManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger
        
    def create_role(self, role_name, privileges):
        """Creates a custom role with a set of privileges."""
        try:
            auth_manager = self.connection.content.authorizationManager
            role_id = auth_manager.AddAuthorizationRole(name=role_name, privIds=privileges)
            print(f"Role '{role_name}' created with ID: {role_id}")
            return role_id
        except vmodl.fault.AlreadyExists:
            print(f"Role '{role_name}' already exists.")
        except Exception as e:
            print(f"Error creating role '{role_name}': {e}")
    
    def delete_role(self, role_name):
        """Deletes a custom role."""
        try:
            auth_manager = self.connection.content.authorizationManager
            role_list = auth_manager.roleList
            for role in role_list:
                if role.name == role_name:
                    auth_manager.RemoveAuthorizationRole(roleId=role.roleId, failIfUsed=False)
                    print(f"Role '{role_name}' deleted.")
                    return
            print(f"Role '{role_name}' not found.")
        except Exception as e:
            print(f"Error deleting role '{role_name}': {e}")

    def set_entity_permissions(self, entity, user, role_name):
        """Assigns a role to a user or group on a specific entity."""
        try:
            auth_manager = self.connection.content.authorizationManager
            role_id = self.get_role_id_by_name(role_name)
            if role_id is None:
                print(f"Role '{role_name}' not found.")
                return
            
            permission = vim.AuthorizationManager.Permission(
                entity=entity,
                principal=user,
                groupId=False,  # False for users, True for groups
                roleId=role_id,
                propagate=True
            )
            
            auth_manager.SetEntityPermissions(entity=entity, permission=[permission])
            print(f"Permissions set for user '{user}' on entity with role '{role_name}'.")
        except Exception as e:
            print(f"Error setting permissions for user '{user}': {e}")

    def get_role_id_by_name(self, role_name):
        """Helper function to find a role ID by its name."""
        auth_manager = self.connection.content.authorizationManager
        for role in auth_manager.roleList:
            if role.name == role_name:
                return role.roleId
        return None
    
    def add_permissions_to_resource_pool(self, resource_pool_name, user, role_name):
        """
        Adds permissions for a specified user or group to a resource pool.

        :param resource_pool_name: The name of the resource pool.
        :param user: The user or group to whom the permissions will be assigned.
        :param role_name: The name of the role defining the permissions.
        """
        resource_pool = self.get_obj([vim.ResourcePool], resource_pool_name)
        if resource_pool is None:
            print(f"Resource pool '{resource_pool_name}' not found.")
            return

        self.set_entity_permissions(entity=resource_pool, user=user, role_name=role_name)

    def add_permissions_to_folder(self, folder_name, user, role_name):
        """
        Adds permissions for a specified user or group to a resource pool.

        :param resource_pool_name: The name of the resource pool.
        :param user: The user or group to whom the permissions will be assigned.
        :param role_name: The name of the role defining the permissions.
        """
        folder = self.get_obj([vim.Folder], folder_name)
        if folder is None:
            print(f"Resource pool '{folder_name}' not found.")
            return

        self.set_entity_permissions(entity=folder, user=user, role_name=role_name)
