from managers.resource_pool_manager import ResourcePoolManager
import os
from dotenv import load_dotenv

load_dotenv()


if __name__ == "__main__":
    vc_host = os.getenv("VC_HOST"); print(vc_host)
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443 
    manager = ResourcePoolManager(vc_host, vc_user, vc_password)
    manager.connect()

    resource_pool_name = "cp-pod54"
    domain = "vcenter.rededucation.com"
    user = "labcp-54"
    role_name = "labcp-0-role"

    manager.assign_role_to_resource_pool(resource_pool_name, domain + "\\" + user, role_name)