from managers.vcenter import VCenter
from managers.vm_manager import VmManager
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = "vcenter-appliance-2.rededucation.com"
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    vc = VCenter(vc_host, vc_user, vc_password, vc_port)
    vc.connect()

    manager = VmManager(vc)
    # manager.create_folder("cp-pod54-folder","cp-pod54-folder-1")
    manager.delete_folder("cp-pod54-folder-1")