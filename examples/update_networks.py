from managers.vm_manager import VmManager
from managers.vcenter import VCenter
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = os.getenv("VC_HOST"); print(vc_host)
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443
    vm_manager = VmManager(vc_host, vc_user, vc_password)
    vm_manager.connect()

    host_name = "ultramagnus.rededucation.com"
    switch_name = "vs54-cp"
    vm_name = "cp-R81-vr-54"
    folder_name = "cp-pod54-folder"
    network_map = {
        "Network adapter 2": "cp-ext-" + switch_name,
        "Network adapter 3": "cp-inta-" + switch_name,
        "Network adapter 4": "cp-mgt-" + switch_name,
        "Network adapter 5": "cp-dmz-" + switch_name,
        "Network adapter 6": "cp-intb-" + switch_name,
        "Network adapter 7": "cp-ext-b-" + switch_name,
        "Network adapter 8": "cp-none2-" + switch_name,
        "Network adapter 9": "cp-ext-b-" + switch_name,
        "Network adapter 10": "cp-mgmtb-" + switch_name,
    }

    vm_manager.update_vm_networks(vm_name, folder_name, network_map)
