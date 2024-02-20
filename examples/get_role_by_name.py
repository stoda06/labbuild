from managers.permission_manager import PermissionManager
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = os.getenv("VC_HOST"); print(vc_host)
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443
    pm_manager = PermissionManager(vc_host, vc_user, vc_password)
    pm_manager.connect()

    role = pm_manager.get_role_id_by_name("labcp-role")

    print(role)