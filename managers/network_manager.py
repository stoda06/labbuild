from pyVmomi import vim
from managers.vcenter import VCenter

class NetworkManager(VCenter):
    def __init__(self, vcenter_instance=None):
        if vcenter_instance:
            # Assume the connection and other necessary properties
            self.connection = vcenter_instance.connection
        else:
            # Normal initialization process
            super().__init__(vcenter_instance.host, vcenter_instance.user, vcenter_instance.password, vcenter_instance.port)
    
    def create_vm_port_group(self, switch_name, port_group_name, vlan_id=0):
        """
        Creates a virtual machine port group on a specified standard switch.

        :param switch_name: Name of the standard switch to create the port group on.
        :param port_group_name: Name for the new port group.
        :param vlan_id: VLAN ID for the port group (default is 0, for no VLAN).
        """
        try:
            # Find the specified standard switch by name
            host_network_system = self.get_host_network_system()
            if not host_network_system:
                print("Failed to retrieve HostNetworkSystem.")
                return
            
            vswitch = None
            for switch in host_network_system.networkConfig.vswitch:
                if switch.name == switch_name:
                    vswitch = switch
                    break

            if not vswitch:
                print(f"Standard switch '{switch_name}' not found.")
                return

            # Create the port group specification
            port_group_spec = vim.host.PortGroup.Specification()
            port_group_spec.name = port_group_name
            port_group_spec.vlanId = vlan_id
            port_group_spec.vswitchName = switch_name
            port_group_spec.policy = vim.host.NetworkPolicy()

            # Create the port group
            host_network_system.AddPortGroup(portgrp=port_group_spec)
            print(f"Port group '{port_group_name}' created on switch '{switch_name}'.")
        except vim.fault.NotFound:
            print(f"Switch '{switch_name}' not found.")
        except vim.fault.DuplicateName:
            print(f"Port group '{port_group_name}' already exists.")
        except vim.fault.InvalidState:
            print("The operation is not allowed in the current state.")
        except vim.fault.HostConfigFault as e:
            print(f"Host configuration error: {e.msg}")
        except Exception as e:
            print(f"General error: {e}")
    
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
            host_system = self.get_host_by_name(host_name)
            if not host_system:
                print(f"Host '{host_name}' not found.")
                return

            network_system = host_system.configManager.networkSystem
            
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