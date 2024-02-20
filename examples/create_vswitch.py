from managers.network_manager import NetworkManager
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = "vcenter-appliance-2.rededucation.com"
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    nm = NetworkManager(vc_host, vc_user, vc_password, vc_port)
    nm.connect()
    nm_content = nm.get_content()

    switch_name = "vs54-cp"
    host_name = "ultramagnus.rededucation.com"

    nm.create_vswitch(host_name, switch_name)