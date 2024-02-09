from pyVmomi import vim
from managers.vcenter import VCenter

class NetworkManager(VCenter):
    # def __init__(self, vcenter_instance=None):
        # if vcenter_instance:
        #     # Assume the connection and other necessary properties
        #     self.connection = vcenter_instance.connection
        # else:
        #     # Normal initialization process
        #     super().__init__(vcenter_instance.host, vcenter_instance.user, vcenter_instance.password, vcenter_instance.port)
    
    def create_vm_port_groups(self, switch_name, port_groups):
        """
        Creates multiple virtual machine port groups on a specified standard switch, ignoring existing port groups.

        :param switch_name: Name of the standard switch to create the port groups on.
        :param port_groups: A dictionary where keys are port group names and values are dictionaries with port group properties (e.g., VLAN ID).
        """
        try:
            host_network_system = self.get_host_network_system()
            if not host_network_system:
                print("Failed to retrieve HostNetworkSystem.")
                return

            for pg_name, pg_props in port_groups.items():
                # Check if the port group already exists
                # if any(pg.name == pg_name for pg in host_network_system.networkInfo.portgroup):
                #     print(f"Port group '{pg_name}' already exists on switch '{switch_name}', ignoring.")
                #     continue  # Skip to the next port group

                vlan_id = pg_props.get('vlan_id', 0)  # Default VLAN ID is 0 if not specified
                port_group_spec = vim.host.PortGroup.Specification()
                port_group_spec.name = pg_name
                port_group_spec.vlanId = vlan_id
                port_group_spec.vswitchName = switch_name
                port_group_spec.policy = vim.host.NetworkPolicy()

                try:
                    host_network_system.AddPortGroup(portgrp=port_group_spec)
                    print(f"Port group '{pg_name}' created successfully on switch '{switch_name}'.")
                except vim.fault.AlreadyExists:
                    print(f"Port group '{pg_name}' already exists on switch '{switch_name}', this should not happen.")
                    continue  # This is a safeguard; the initial check should prevent this from occurring
        except vim.fault.NotFound:
            print(f"Switch '{switch_name}' not found.")
        except vim.fault.ResourceInUse:
            print(f"Resource is in use and cannot be modified.")
        except Exception as e:
            print(f"Failed to create port groups on switch '{switch_name}': {e}")

    
    def get_host_network_system(self):
        """
        Retrieves the HostNetworkSystem of the first host found.
        This example assumes a single host or uses the first host found; adjust as needed.
        """
        content = self.connection.RetrieveContent()
        for datacenter in content.rootFolder.childEntity:
            if hasattr(datacenter, 'hostFolder'):
                host_folder = datacenter.hostFolder
                host_system = self.get_first_object_by_type(host_folder, vim.HostSystem)
                if host_system:
                    return host_system.configManager.networkSystem
        return None

    def get_first_object_by_type(self, starting_point, obj_type):
        """
        Helper method to get the first object of a specific type from a starting point.
        """
        view = self.connection.content.viewManager.CreateContainerView(starting_point, [obj_type], True)
        obj_list = list(view.view)
        view.Destroy()
        return obj_list[0] if obj_list else None
    
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
                print(f"Resource pool '{host}' not found.")
                return

            network_system = host.configManager.networkSystem
            
            vswitch_spec = vim.host.VirtualSwitch.Specification()
            vswitch_spec.numPorts = num_ports
            vswitch_spec.mtu = mtu

            network_system.AddVirtualSwitch(vswitchName=vswitch_name, spec=vswitch_spec)
            print(f"Virtual switch '{vswitch_name}' created successfully on host '{host_name}'.")
        except vim.fault.AlreadyExists:
            print(f"Virtual switch '{vswitch_name}' already exists on host '{host_name}'.")
        except vim.fault.NotFound:
            print(f"Host '{host_name}' not found.")
        except vim.fault.ResourceInUse:
            print(f"Virtual switch '{vswitch_name}' is in use and cannot be created.")
        except Exception as e:
            print(f"Failed to create virtual switch '{vswitch_name}' on host '{host_name}': {e}")