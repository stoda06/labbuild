from managers.vcenter import VCenter
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = os.getenv("VC_HOST")
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    vc_port = 443  # Default port for vCenter connection

    vc = VCenter(vc_host, vc_user, vc_password, vc_port)
    vc.connect()
    vc_content = vc.get_content()
    if vc_content:
        print("Successfully retrieved content from vCenter.")
        hosts = vc.get_hosts(vc_content)
        print(hosts)
    else:
        print("Failed to retrieve content.")