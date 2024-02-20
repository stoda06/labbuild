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

    # Create a resource pool under a specific host
    cpu_allocation = {
        'limit': -1,
        'reservation': 0,
        'expandable_reservation': True,
        'shares': 4000
    }
    memory_allocation = {
        'limit': -1,
        'reservation': 0,
        'expandable_reservation': True,
        'shares': 163840
    }
    # manager.create_resource_pool("cp-ultramagnus", "cp-pod55", cpu_allocation, memory_allocation)


    # List resource pools under a specific parent resource pool
    # rps = manager.list_resource_pools("cp-ultramagnus")
    # print("Resource Pools under ParentResourcePoolName:", rps)

    