from managers.vm_manager import VmManager
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


    # vm_manager.delete_vm("cp-R81-vr-54")

    # vm_usage_details = vm_manager.get_vm_current_usage("cp-R81.20-vr")
    # vm_usage_details_1 = vm_manager.get_vm_current_usage("cp-R81-vr-54")

    # print(vm_usage_details, vm_usage_details_1)

    # vm_manager.delete_folder("cp-pod54-folder-1",force=True)
    # vm_manager.create_folder("cp-pod54-folder", "cp-pod54-folder-1")
    vm_manager.clone_vm("cp-R81.20-vr", "cp-R81-vr-54","cp-pod54", "cp-pod54-folder", datastore_name="vms")