from managers.datastore_manager import DatastoreManager
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    vc_host = os.getenv("VC_HOST"); print(vc_host)
    vc_user = os.getenv("VC_USER")
    vc_password = os.getenv("VC_PASS")
    ds_manager = DatastoreManager(vc_host, vc_user, vc_password)
    ds_manager.connect()

    ds_manager.list_datastores()
