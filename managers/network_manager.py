from pyVmomi import vim, vmodl
from managers.vcenter import VCenter
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import re

class NetworkManager(VCenter):

    def __init__(self, vcenter_instance):
        if not vcenter_instance.connection:
            raise ValueError("VCenter instance is not connected.")
        self.vcenter = vcenter_instance
        self.connection = vcenter_instance.connection
        self.logger = vcenter_instance.logger
    
    def create_port_group(self, host_network_system, switch_name, port_group_spec):
        """
        Create a single port group on the specified switch, ensuring it does not already exist.

        :param host_network_system: The network system of the host where the port group will be added.
        :param switch_name: The name of the vSwitch where the port group will be created.
        :param port_group_spec: Specification of the port group to be created.
        """
        try:
            # Retrieve the current list of port groups to check if the port group already exists
            existing_port_groups = host_network_system.networkConfig.portgroup
            if any(pg.spec.name == port_group_spec.name for pg in existing_port_groups):
                self.logger.warning(f"Port group '{port_group_spec.name}' already exists on switch '{switch_name}'. Skipping.")
                return

            # Proceed with creating the port group since it does not exist
            host_network_system.AddPortGroup(portgrp=port_group_spec)
            self.logger.debug(f"Port group '{port_group_spec.name}' created successfully on switch '{switch_name}'.")
        except Exception as e:
            self.logger.error(f"Failed to create port group '{port_group_spec.name}': {e}")
    
    def create_vm_port_groups(self, host_name, switch_name, port_groups, pod_number=None):
        """
        Creates multiple virtual machine port groups concurrently on a specified standard switch
        for a given host. Returns True if all port groups are created successfully; otherwise,
        returns False.

        :param host_name: The name of the host where the standard switch resides.
        :param switch_name: The name of the standard switch on which to create the port groups.
        :param port_groups: A list of dictionaries, each containing port group properties (e.g., "port_group_name" and "vlan_id").
        :param pod_number: Optional pod number to adjust VLAN IDs for specific conditions.
        :return: bool - True if successful, False otherwise.
        """
        # Retrieve the host object using the provided host_name.
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Failed to retrieve host '{host_name}'.")
            return False

        # Access the HostNetworkSystem from the retrieved host.
        host_network_system = host.configManager.networkSystem
        all_success = True  # Flag to track overall success.

        # Create port groups concurrently using ThreadPoolExecutor.
        with ThreadPoolExecutor() as executor:
            futures = []
            for pg in port_groups:
                # Prepare the specification for the port group.
                port_group_spec = vim.host.PortGroup.Specification()
                port_group_spec.name = pg["port_group_name"]

                # Adjust the VLAN ID for certain switch naming conventions.
                if "pa" in switch_name and "pa-vswitch" not in switch_name:
                    port_group_spec.vlanId = pg.get('vlan_id', 0) + (pod_number or 0)
                else:
                    port_group_spec.vlanId = pg.get('vlan_id', 0)

                port_group_spec.vswitchName = switch_name
                port_group_spec.policy = vim.host.NetworkPolicy()

                # Schedule the task to create the port group.
                future = executor.submit(self.create_port_group, host_network_system, switch_name, port_group_spec)
                futures.append(future)

            # Wait for all tasks to complete and check for errors.
            for future in futures:
                try:
                    future.result()  # Will raise any exceptions from the task.
                except Exception as e:
                    self.logger.error(f"Failed to create port group: {e}")
                    all_success = False

        return all_success

    def create_vswitch(self, host_name, vswitch_name, num_ports=128, mtu=1500):
        """
        Creates a new virtual switch on the specified host.

        Returns True if the vSwitch is created successfully (or already exists), 
        otherwise returns False.

        :param host_name: The name of the host where the vSwitch will be created.
        :param vswitch_name: The name for the new virtual switch.
        :param num_ports: The number of ports that the virtual switch will have.
        :param mtu: The MTU size for the virtual switch.
        :return: bool
        """
        try:
            host = self.get_obj([vim.HostSystem], host_name)
            if not host:
                self.logger.error(f"Host '{host_name}' not found.")
                return False

            network_system = host.configManager.networkSystem
            
            vswitch_spec = vim.host.VirtualSwitch.Specification()
            vswitch_spec.numPorts = num_ports
            vswitch_spec.mtu = mtu

            network_system.AddVirtualSwitch(vswitchName=vswitch_name, spec=vswitch_spec)
            self.logger.debug(f"Virtual switch '{vswitch_name}' created successfully on host '{host_name}'.")
            return True
        except vim.fault.AlreadyExists:
            self.logger.warning(f"Virtual switch '{vswitch_name}' already exists on host '{host_name}'.")
            return True
        except vim.fault.NotFound:
            self.logger.error(f"Host '{host_name}' not found.")
            return False
        except vim.fault.ResourceInUse:
            self.logger.error(f"Failed to create virtual switch '{vswitch_name}' on host '{host_name}' \
                              due to resource limitation. Please find an alternate vswitch.")
            return "RESOURCE_LIMIT"
        except vim.fault.PlatformConfigFault as e:
            resource_limit_detected = False
            # Inspect the faultMessage attribute if available
            if hasattr(e, "faultMessage") and e.faultMessage:
                for fm in e.faultMessage:
                    if hasattr(fm, "message") and ("Out of resources" in fm.message or "VSI_NODE_net_create" in fm.message):
                        resource_limit_detected = True
                        break
            # Fallback to checking e.msg if faultMessage is not available
            elif "Out of resources" in e.msg or "VSI_NODE_net_create" in e.msg:
                resource_limit_detected = True

            if resource_limit_detected:
                self.logger.error(f"Failed to create virtual switch '{vswitch_name}' on host '{host_name}' due to resource limitation. Please find an alternate vswitch.")
                return "RESOURCE_LIMIT"
            else:
                self.logger.error(f"Failed to create virtual switch '{vswitch_name}' on host '{host_name}': {e}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to create virtual switch '{vswitch_name}' on host '{host_name}': {e}")
            return False

    def delete_vswitch(self, host_name, vswitch_name):
        """
        Deletes a specified vSwitch from a host.

        :param host_name: The name of the host from which to delete the vSwitch.
        :param vswitch_name: The name of the vSwitch to delete.
        """
        # Find the host system by name
        host_system = self.get_obj([vim.HostSystem], host_name)

        if not host_system:
            self.logger.error(f"Host '{host_name}' not found.")
            return False

        # Get the host's network system
        host_network_system = host_system.configManager.networkSystem

        # Check if the vSwitch exists
        vswitch_exists = any(vswitch for vswitch in host_network_system.networkInfo.vswitch if vswitch.name == vswitch_name)
        if not vswitch_exists:
            self.logger.error(f"vSwitch '{vswitch_name}' not found on host '{host_name}'.")
            return False

        # Remove the vSwitch
        try:
            host_network_system.RemoveVirtualSwitch(vswitchName=vswitch_name)
            self.logger.debug(f"vSwitch '{vswitch_name}' has been successfully deleted from host '{host_name}'.")
        except vim.fault.NotFound:
            self.logger.error(f"vSwitch '{vswitch_name}' could not be found.")
            return False
        except vim.fault.ResourceInUse:
            self.logger.error(f"vSwitch '{vswitch_name}' is in use and cannot be deleted.")
            return False
        except Exception as e:
            self.logger.error(f"An error occurred while deleting vSwitch '{vswitch_name}': {str(e)}")
            return False

        return True
    
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
        
        Returns True if the role assignment is successful for all networks; otherwise, returns False.

        :param user_domain_name: The domain and username to whom the role will be assigned.
        :param role_name: The name of the role to assign.
        :param network_names: A list of network names to assign the role to.
        :return: bool
        """
        all_success = True  # Flag to track overall success

        with ThreadPoolExecutor() as executor:
            futures = []
            for network_name in network_names:
                # Retrieve the network object based on its name
                network = self.get_obj([vim.Network], network_name)
                # Schedule the task to assign the user role on the network
                future = executor.submit(self.set_user_role_on_network, user_domain_name, role_name, network)
                futures.append(future)
            
            # Process the results of all scheduled tasks
            for future in futures:
                try:
                    # This will raise any exception caught during the task
                    result = future.result()
                    self.logger.debug(result)
                except Exception as e:
                    self.logger.error(f"Assigning user and role to network failed: {e}")
                    all_success = False

        return all_success
    
    def enable_promiscuous_mode(self, host_name, network_names):
        """
        Enables promiscuous mode for a list of network (port group) names on a specified host concurrently.

        Returns True if promiscuous mode is enabled successfully on all specified networks; otherwise, returns False.

        :param host_name: Name of the host where the networks reside.
        :param network_names: A list of network (port group) names to enable promiscuous mode on.
        :return: bool
        """
        # Retrieve the host object using its name.
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Host '{host_name}' not found.")
            return False

        def task(network_name):
            """
            Task to enable promiscuous mode for a single network.
            Returns True if successful; otherwise, returns False.
            """
            network_system = host.configManager.networkSystem
            port_group = None
            
            # Locate the port group by its name.
            for pg in network_system.networkConfig.portgroup:
                if pg.spec.name == network_name:
                    port_group = pg
                    break

            if not port_group:
                self.logger.warning(f"Port group '{network_name}' not found on host '{host_name}'.")
                return False

            # Set up the port group specification with security policy.
            port_group_spec = port_group.spec
            port_group_spec.policy = vim.host.NetworkPolicy()
            port_group_spec.policy.security = vim.host.NetworkPolicy.SecurityPolicy(allowPromiscuous=True)
            
            # For specific networks, enable additional security options.
            if "EXT-CCVS" in network_name:
                port_group_spec.policy.security = vim.host.NetworkPolicy.SecurityPolicy(
                    allowPromiscuous=True, macChanges=True, forgedTransmits=True)

            try:
                network_system.UpdatePortGroup(network_name, port_group_spec)
                self.logger.debug(f"Promiscuous mode enabled for port group '{network_name}' on host '{host_name}'.")
                return True
            except vmodl.MethodFault as e:
                self.logger.error(f"Failed to enable promiscuous mode for port group '{network_name}' on host '{host_name}': {e.msg}")
                return False

        # Use ThreadPoolExecutor to process all networks concurrently.
        all_success = True
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(task, network_name) for network_name in network_names]

            # Process the results of each concurrent task.
            for future in futures:
                try:
                    result = future.result()  # Retrieve result from the task.
                    if not result:
                        all_success = False
                except Exception as e:
                    self.logger.error(f"Exception occurred while enabling promiscuous mode: {e}")
                    all_success = False

        return all_success

    def delete_port_groups(self, host_name, switch_name, port_groups):
        """
        Deletes multiple virtual machine port groups from a specified standard switch on a given host.

        :param host_name: Name of the host where the standard switch resides.
        :param switch_name: Name of the standard switch to delete the port groups from.
        :param port_groups: A list of dictionaries, each containing port group properties (e.g., name).
        :return: True if all specified port groups are deleted successfully, False otherwise.
        """
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Failed to retrieve host '{host_name}'.")
            return False

        host_network_system = host.configManager.networkSystem

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._delete_port_group, host_network_system, switch_name, pg["port_group_name"])
                for pg in port_groups
            ]

        return all(future.result() for future in futures)

    def _delete_port_group(self, host_network_system, switch_name, port_group_name):
        """
        Deletes a single port group from a vSwitch.

        :param host_network_system: The HostNetworkSystem object from the host.
        :param switch_name: The name of the vSwitch to delete the port group from.
        :param port_group_name: The name of the port group to delete.
        :return: True if the port group is deleted successfully, False otherwise.
        """
        try:
            host_network_system.RemovePortGroup(pgName=port_group_name)
            self.logger.debug(f"Port group '{port_group_name}' deleted successfully from switch '{switch_name}'.")
            return True
        except vim.fault.NotFound:
            self.logger.warning(f"Port group '{port_group_name}' not found on switch '{switch_name}'. Skipping.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete port group '{port_group_name}' from switch '{switch_name}': {e}")
            return False
    

    def create_vswitch_portgroups(self, hostname_fqdn, vswitch_name, port_groups, resource_limit=None):
        """
        Creates port groups with specified VLAN IDs on the given vSwitch for the provided host.
        
        If resource_limit is set to "RESOURCE_LIMIT", then it will search for an alternate vswitch 
        (based on substrings extracted from vswitch_name) and check that none of the VLAN IDs in 
        the provided port_groups are already in use on that vswitch. If a candidate is found, it 
        proceeds with port group creation on that vswitch. If all candidate vswitches are exhausted 
        (i.e., for each candidate, at least one VLAN ID is already in use), then the operation is deemed failed.
        
        Returns True if all operations are successful, or False if any error occurs.
        
        :param hostname_fqdn: Fully qualified domain name of the host.
        :param vswitch_name: Name of the vSwitch where the port groups should be created.
        :param port_groups: List of dictionaries, each containing 'port_group_name' and 'vlan_id'.
        :param resource_limit: Optional flag. If set to "RESOURCE_LIMIT", alternate vswitches will be searched.
        """
        # Fetch the host object using its fully qualified domain name
        host = self.get_obj([vim.HostSystem], hostname_fqdn)
        if not host:
            self.logger.error(f"Failed to retrieve host '{hostname_fqdn}'.")
            return False

        # Directly fetch the HostNetworkSystem object
        host_network_system = host.configManager.networkSystem

        if resource_limit == "RESOURCE_LIMIT":
            # Use regex to split the vswitch_name into substrings.
            # For example, "vs10-cp" -> ["vs", "-cp"]
            substrings = [s for s in re.split(r'\d+', vswitch_name) if s]
            self.logger.debug(f"Extracted substrings from '{vswitch_name}': {substrings}")

            # Find candidate vswitches that contain all the substrings in their names.
            candidate_vswitches = [
                vs for vs in host_network_system.networkConfig.vswitch
                if all(sub in vs.name for sub in substrings)
            ]
            if not candidate_vswitches:
                self.logger.error("No candidate vswitch found for alternate port groups creation.")
                return False

            # Iterate over candidate vswitches
            for candidate in candidate_vswitches:
                candidate_name = candidate.name
                if "vs0" in candidate_name:
                    continue
                self.logger.debug(f"Evaluating candidate vswitch '{candidate_name}'.")
                # Retrieve all portgroups on the candidate vswitch
                candidate_portgroups = [
                    pg.spec for pg in host_network_system.networkConfig.portgroup
                    if pg.spec.vswitchName == candidate_name
                ]
                # Build a set of VLAN IDs already in use on the candidate vswitch
                candidate_vlan_ids = {pg.vlanId for pg in candidate_portgroups}
                self.logger.debug(f"Candidate vswitch '{candidate_name}' has VLAN IDs: {candidate_vlan_ids}")

                # Check if any of the requested VLAN IDs are already in use
                conflict = False
                for pg in port_groups:
                    new_vlan_id = pg["vlan_id"]+100
                    if new_vlan_id in candidate_vlan_ids:
                        conflict = True
                        self.logger.info(f"Candidate vswitch '{candidate_name}' already has VLAN id {new_vlan_id} in use.")
                        break

                # If there is a conflict, try the next candidate vswitch
                if conflict:
                    continue

                # If candidate is valid, attempt to create the port groups on this candidate vswitch
                overall_success = True
                for pg in port_groups:
                    port_group_name = pg["port_group_name"]
                    vlan_id = pg["vlan_id"] + 100
                    print(f"{port_group_name} vlandId: {vlan_id}")

                    # Check if the port group already exists on the candidate vswitch
                    existing_pg_names = [
                        pg_obj.spec.name for pg_obj in host_network_system.networkConfig.portgroup
                        if pg_obj.spec.vswitchName == candidate_name
                    ]
                    if port_group_name in existing_pg_names:
                        self.logger.warning(f"Port group '{port_group_name}' already exists on switch '{candidate_name}'. Skipping.")
                        continue

                    port_group_spec = vim.host.PortGroup.Specification()
                    port_group_spec.name = port_group_name
                    port_group_spec.vlanId = vlan_id
                    port_group_spec.vswitchName = candidate_name
                    port_group_spec.policy = vim.host.NetworkPolicy()

                    try:
                        host_network_system.AddPortGroup(portgrp=port_group_spec)
                        self.logger.info(f"Port group '{port_group_name}' created successfully on switch '{candidate_name}'.")
                    except vim.fault.AlreadyExists:
                        self.logger.warning(f"Port group '{port_group_name}' already exists on '{candidate_name}'.")
                    except vim.fault.NotFound as e:
                        self.logger.error(f"Error: {e.msg}. The vSwitch '{candidate_name}' might not exist.")
                        overall_success = False
                    except Exception as e:
                        self.logger.error(f"An unexpected error occurred while creating port group '{port_group_name}' on switch '{candidate_name}': {e}")
                        overall_success = False

                if overall_success:
                    return True

            # If we have exhausted all candidate vswitches without success, return False.
            self.logger.error("All candidate vswitches are exhausted. Failed to create port groups due to resource limitations.")
            return False

        else:
            # Standard logic if resource_limit is not provided.
            vswitch_exists = next((vs for vs in host_network_system.networkConfig.vswitch if vs.name == vswitch_name), None)
            if not vswitch_exists:
                self.logger.error(f"vSwitch '{vswitch_name}' does not exist on the host '{hostname_fqdn}'.")
                return False

            # Retrieve the list of port groups for the specific vSwitch only
            existing_port_groups = [
                pg.spec.name for pg in host_network_system.networkConfig.portgroup
                if pg.spec.vswitchName == vswitch_name
            ]

            overall_success = True

            for pg in port_groups:
                port_group_name = pg["port_group_name"]
                vlan_id = pg["vlan_id"]

                if port_group_name in existing_port_groups:
                    self.logger.warning(f"Port group '{port_group_name}' already exists on switch '{vswitch_name}'. Skipping.")
                    continue

                port_group_spec = vim.host.PortGroup.Specification()
                port_group_spec.name = port_group_name
                port_group_spec.vlanId = vlan_id
                port_group_spec.vswitchName = vswitch_name
                port_group_spec.policy = vim.host.NetworkPolicy()

                try:
                    host_network_system.AddPortGroup(portgrp=port_group_spec)
                    self.logger.info(f"Port group '{port_group_name}' created successfully on switch '{vswitch_name}'.")
                except vim.fault.AlreadyExists:
                    self.logger.warning(f"Port group '{port_group_name}' already exists.")
                except vim.fault.NotFound as e:
                    self.logger.error(f"Error: {e.msg}. The vSwitch '{vswitch_name}' might not exist.")
                    overall_success = False
                except Exception as e:
                    self.logger.error(f"An unexpected error occurred while creating port group '{port_group_name}': {e}")
                    overall_success = False

            return overall_success
        
    def find_vswitch_by_name_substring(self, host_name: str, substring: str) -> Optional[str]:
        """
        Finds the first vSwitch on a host whose name contains a given substring.

        :param host_name: The name of the host to search.
        :param substring: The substring to look for in the vSwitch name.
        :return: The name of the found vSwitch, or None if not found.
        """
        host = self.get_obj([vim.HostSystem], host_name)
        if not host:
            self.logger.error(f"Host '{host_name}' not found for vSwitch search.")
            return None

        try:
            network_system = host.configManager.networkSystem
            for vswitch in network_system.networkConfig.vswitch:
                if substring in vswitch.name:
                    self.logger.info(f"Found suitable vSwitch '{vswitch.name}' on host '{host_name}' containing '{substring}'.")
                    return vswitch.name
            
            self.logger.error(f"No vSwitch found on host '{host_name}' with the substring '{substring}' in its name.")
            return None
        except Exception as e:
            self.logger.error(f"Error searching for vSwitch on host '{host_name}': {e}")
            return None