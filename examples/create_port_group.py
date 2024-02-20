from managers.network_manager import NetworkManager
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = os.getenv("VC_HOST"); print(vc_host)
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443
    network_manager = NetworkManager(vc_host, vc_user, vc_password)
    network_manager.connect()

    switch_name = "vs54-cp"
    port_group_name = "cp-inta-vs54-cp"
    vlan_id = 402  # Example VLAN ID, use 0 if no VLAN is needed


switch_name = "vs54-cp"
port_groups = {
        "cp-mgt-" + switch_name: {"vlan_id": 401},
        "cp-inta-" + switch_name: {"vlan_id": 402},
        "cp-dmz-" + switch_name: {"vlan_id": 403},
        "cp-wifi-" + switch_name: {"vlan_id": 404},
        "cp-sync-" + switch_name: {"vlan_id": 405},
        "cp-ext-" + switch_name: {"vlan_id": 406},
        "cp-intb-" + switch_name: {"vlan_id": 407},
        "cp-none1-" + switch_name: {"vlan_id": 408},
        "cp-none2-" + switch_name: {"vlan_id": 409},
        "cp-mgmtb-" + switch_name: {"vlan_id": 410},
        "cp-none3-" + switch_name: {"vlan_id": 411},
        "cp-ext-b-" + switch_name: {"vlan_id": 412},
        # Add more port groups as needed
    }
network_manager.create_vm_port_groups(switch_name, port_groups)