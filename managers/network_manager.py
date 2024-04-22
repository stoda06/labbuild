from pyVmomi import vim, vmodl
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor

class NetworkManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger
    
    def create_port_group(self, host_network_system, switch_name, port_group_spec):
        """
        Create a single port group on the specified switch.
        """
        try:
            host_network_system.AddPortGroup(portgrp=port_group_spec)
            self.logger.debug(f"Port group '{port_group_spec.name}' created successfully on switch '{switch_name}'.")
        except vim.fault.AlreadyExists:
            self.logger.warning(f"Port group '{port_group_spec.name}' already exists on switch '{switch_name}'. Skipping.")
        except Exception as e:
            self.logger.error(f"Failed to create port group '{port_group_spec.name}': {e}")
    
    def create_vm_port_groups(self, host_name, switch_name, port_groups, pod_number=None):
        """
        Creates multiple virtual machine port groups on a specified standard switch concurrently for a given host.

        :param hostname: Name of the host where the standard switch resides.
        :param switch_name: Name of the standard switch to create the port groups on.
        :param port_groups: A list of dictionaries, each containing port group properties (e.g., name and VLAN ID).
        """
        # Use the get_obj method to fetch the host by its name
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Failed to retrieve host '{host_name}'.")
            return

        # Access the HostNetworkSystem directly from the retrieved host
        host_network_system = host.configManager.networkSystem

        with ThreadPoolExecutor() as executor:
            futures = []
            for pg in port_groups:
                port_group_spec = vim.host.PortGroup.Specification()
                port_group_spec.name = pg["port_group_name"]
                if "pa" in switch_name:
                    port_group_spec.vlanId = pg.get('vlan_id', 0)+pod_number  # Default VLAN ID is 0 if not specified
                else:
                    port_group_spec.vlanId = pg.get('vlan_id', 0)  # Default VLAN ID is 0 if not specified
                port_group_spec.vswitchName = switch_name
                port_group_spec.policy = vim.host.NetworkPolicy()

                # Schedule the port group creation task
                future = executor.submit(self.create_port_group, host_network_system, switch_name, port_group_spec)
                futures.append(future)

            # Optionally, wait for all tasks to complete and handle their results
            for future in futures:
                try:
                    future.result()  # This will re-raise any exceptions caught in the task
                except Exception as e:
                    self.logger.error(f"Failed to create one or more port groups: {e}")
    
    def create_vswitch(self, host_name, vswitch_name, num_ports=128, mtu=1500):
        """
        Creates a new virtual switch on the specified host.

        :param host_name: The name of the host where the vSwitch will be created.
        :param vswitch_name: The name for the new virtual switch.
        :param num_ports: The number of ports that the virtual switch will have.
        :param mtu: The MTU size for the virtual switch.
        """
        try:
            host = self.get_obj([vim.HostSystem], host_name)
            if not host:
                self.logger.error(f"Resource pool '{host}' not found.")
                return

            network_system = host.configManager.networkSystem
            
            vswitch_spec = vim.host.VirtualSwitch.Specification()
            vswitch_spec.numPorts = num_ports
            vswitch_spec.mtu = mtu

            network_system.AddVirtualSwitch(vswitchName=vswitch_name, spec=vswitch_spec)
            self.logger.debug(f"Virtual switch '{vswitch_name}' created successfully on host '{host_name}'.")
        except vim.fault.AlreadyExists:
            self.logger.warning(f"Virtual switch '{vswitch_name}' already exists on host '{host_name}'.")
        except vim.fault.NotFound:
            self.logger.error(f"Host '{host_name}' not found.")
        except vim.fault.ResourceInUse:
            self.logger.error(f"Virtual switch '{vswitch_name}' is in use and cannot be created.")
        except Exception as e:
            self.logger.error(f"Failed to create virtual switch '{vswitch_name}' on host '{host_name}': {e}")

    def delete_vswitch(self, vswitch_name):
        """
        Deletes a specified vSwitch from all hosts managed by the connected vCenter using the Property Collector for efficiency.

        :param vswitch_name: The name of the vSwitch to delete.
        """
        content = self.get_content()
        property_collector = content.propertyCollector
        container = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)

        # Property Specification
        prop_spec = vmodl.query.PropertyCollector.PropertySpec(type=vim.HostSystem, pathSet=["configManager.networkSystem"])
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec(obj=container, skip=False)
        filter_spec = vmodl.query.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec])

        # Retrieve data
        retrieved_data = property_collector.RetrieveContents([filter_spec])

        for data in retrieved_data:
            host_system = data.obj
            network_system = data.propSet[0].val

            try:
                if any(vswitch for vswitch in network_system.networkInfo.vswitch if vswitch.name == vswitch_name):
                    network_system.RemoveVirtualSwitch(vswitchName=vswitch_name)
                    self.logger.debug(f"vSwitch '{vswitch_name}' has been successfully deleted from host '{host_system.name}'.")
            except vim.fault.NotFound:
                self.logger.error(f"vSwitch '{vswitch_name}' not found on host '{host_system.name}'.")
            except vim.fault.ResourceInUse:
                self.logger.error(f"vSwitch '{vswitch_name}' is in use and cannot be deleted on host '{host_system.name}'.")
            except Exception as e:
                self.logger.error(f"An error occurred while deleting vSwitch '{vswitch_name}' on host '{host_system.name}': {str(e)}")

        container.Destroy()
    
    def set_user_role_on_network(self, user_domain_name, role_name, network, propagate=True):
        """
        Helper function to set a user role on a single network.
        """
        if network is None:
            self.logger.error("Network not found")
            return False
        
        # Retrieve the AuthorizationManager and the RoleManager
        auth_manager = self.connection.content.authorizationManager
        role_list = auth_manager.roleList

        # Retrieve current permissions of the folder
        current_permissions = self.connection.content.authorizationManager.RetrieveEntityPermissions(entity=network, inherited=False)

        # Check if the user already has the specified role assigned
        for perm in current_permissions:
            if perm.principal == user_domain_name and perm.roleId == role_id and perm.propagate == propagate:
                self.logger.debug(f"User '{user_domain_name}' already has role '{role_name}' on network '{network.name}' with identical propagation setting.")
                return  # Skip the assignment
        
        # Find the specified role ID
        role_id = None
        for role in role_list:
            if role.name == role_name:
                role_id = role.roleId
                break
        
        if role_id is None:
            self.logger.error(f"Role '{role_name}' not found.")
            return False
        
        # Construct the permission spec and apply it
        permission = vim.AuthorizationManager.Permission()
        permission.principal = user_domain_name
        permission.group = False
        permission.roleId = role_id
        permission.propagate = propagate
        
        try:
            auth_manager.SetEntityPermissions(entity=network, permission=[permission])
            self.logger.debug(f"Assigned role '{role_name}' to user '{user_domain_name}' on network '{network.name}'.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to assign role to network '{network.name}': {e}")
            return False
    
    def apply_user_role_to_networks(self, user_domain_name, role_name, network_names):
        """
        Applies a specified user and role to multiple networks concurrently.

        :param user_domain_name: The domain and username to whom the role will be assigned.
        :param role_name: The name of the role to assign.
        :param network_names: A list of network names to assign the role to.
        """
        with ThreadPoolExecutor() as executor:
            futures = []
            for network_name in network_names:
                network = self.get_obj([vim.Network],network_name)
                future = executor.submit(self.set_user_role_on_network, user_domain_name, role_name, network)
                futures.append(future)
            
            # Processing results
            for future in futures:
                try:
                    result = future.result()  # This will re-raise any exceptions caught in the task
                    # Handle successful cloning result
                    self.logger.debug(result)
                except Exception as e:
                    # Handle cloning failure
                    self.logger.error(f"Assigning user and role failed: {e}")
    
    def enable_promiscuous_mode(self, host_name, network_names):
        """
        Enables promiscuous mode for a list of network names on a specified host using ThreadPoolExecutor for parallel execution.

        :param host_name: Name of the host where the networks reside.
        :param network_names: A list of network (port group) names to enable promiscuous mode on.
        """
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Host '{host_name}' not found.")
            return

        def task(network_name):
            network_system = host.configManager.networkSystem
            port_group = None
            for pg in network_system.networkConfig.portgroup:
                if pg.spec.name == network_name:
                    port_group = pg
                    break

            if not port_group:
                self.logger.warning(f"Port group '{network_name}' not found on host '{host_name}'.")
                return

            port_group_spec = port_group.spec
            port_group_spec.policy = vim.host.NetworkPolicy()
            port_group_spec.policy.security = vim.host.NetworkPolicy.SecurityPolicy(allowPromiscuous=True)

            try:
                network_system.UpdatePortGroup(network_name, port_group_spec)
                self.logger.debug(f"Promiscuous mode enabled for port group '{network_name}' on host '{host_name}'.")
            except vmodl.MethodFault as e:
                self.logger.error(f"Failed to enable promiscuous mode for port group '{network_name}' on host '{host_name}': {e.msg}")

        # Using ThreadPoolExecutor to enable promiscuous mode concurrently on multiple port groups
        with ThreadPoolExecutor() as executor:
            executor.map(task, network_names)