from dotenv import load_dotenv
import purestorage
import urllib3
import os

load_dotenv()
urllib3.disable_warnings()

def get_usage_percentage(array):
    # Retrieve the array capacity information
    capacity_info = array.get(space=True)[0]
    total_capacity = capacity_info['capacity']
    used_capacity = capacity_info['total']

    # Calculate the usage percentage
    usage_percentage = (used_capacity / total_capacity) * 100
    return usage_percentage

def is_overutilized(datastore):
    overutilized = False
    if 'm20' in datastore:
        storage_host = "pureiq-rededucation-m20"
        token = os.getenv("M20_API_TOKEN")
    elif 'vms' in datastore:
        storage_host = "pureiq-rededucation-405"
        token = os.getenv("VMS_API_TOKEN")
    else:   # If the host is 
        return False
    # Connect to the FlashArray
    array = purestorage.FlashArray(storage_host, api_token=token)
    try:
        # Get usage percentage
        usage_percentage = get_usage_percentage(array)

        # Return True if usage is more than 95%, else False
        if usage_percentage > 95:
            overutilized = True

    except purestorage.PureError as e:
        print(f"An exception occurred {e}")
    finally:
        # Logout from the FlashArray
        array.invalidate_cookie()
    return overutilized