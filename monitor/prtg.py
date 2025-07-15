import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET
import logging
from urllib.parse import quote_plus, urlparse, parse_qs

# Disable InsecureRequestWarning if not verifying SSL certificates.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import logging
logger = logging.getLogger(__name__) # Or logging.getLogger('VmManager')

class PRTGManager:
    """
    A manager class for interacting with a PRTG server via its API.

    This class provides functionality to search for, clone, configure, and enable devices
    on a PRTG server. It also allows retrieval of sensor counts and device statuses.
    """

    def __init__(self, prtg_url, api_token):
        """
        Initializes a new instance of the PRTGManager class.

        This method sets up the API URL, API token, and a requests session configured with
        retry logic to handle transient HTTP errors.

        Args:
            prtg_url (str): The base URL of the PRTG server.
            api_token (str): The API token for authenticating with the PRTG server.
        """
        self.prtg_url = prtg_url
        self.api_token = api_token
        self.session = requests.Session()
        
        # Configure the session with retry logic for certain HTTP status codes.
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def create_session(self):
        """
        Creates and returns a new requests session with retry logic.

        This session is configured to retry HTTP requests in case of transient errors.

        Returns:
            requests.Session: A new requests session configured with retry logic.
        """
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def search_device(self, container_id, clone_name):
        """
        Searches for a device by its name within a specified container (group).

        It queries the PRTG API for devices that match the given filter within the provided container.

        Args:
            container_id (str): The ID of the container (group) where the device is located.
            clone_name (str): The name of the device to search for.

        Returns:
            str: The device ID (objid) if a matching device is found.
            None: If no matching device is found.
        """
        search_url = f"{self.prtg_url}/api/table.json"
        search_params = {
            'content': 'devices',
            'output': 'json',
            'columns': 'objid,device',
            'id': container_id,
            'filter_name': clone_name,
            'apitoken': self.api_token
        }
        # Send a GET request to the PRTG API.
        response = self.session.get(search_url, params=search_params, verify=False, timeout=60)
        response.raise_for_status()
        devices = response.json().get('devices', [])
        
        # Loop through the devices and return the objid of the device matching clone_name.
        for device in devices:
            if device['device'] == clone_name:
                return device['objid']
        return None
    
    def search_device_globally(self, device_name):
        """
        Searches for all devices matching a name across the entire PRTG instance.

        This is used for teardown operations where the parent container ID is not known.

        Args:
            device_name (str): The exact name of the device to search for.

        Returns:
            list: A list of device IDs (objid) that match the name.
        """
        search_url = f"{self.prtg_url}/api/table.json"
        search_params = {
            'content': 'devices',
            'output': 'json',
            'columns': 'objid',  # We only need the ID for deletion
            'filter_name': device_name,  # Exact match on the device name
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(search_url, params=search_params, verify=False, timeout=60)
            response.raise_for_status()
            devices = response.json().get('devices', [])
            # Return a list of all found object IDs
            return [device['objid'] for device in devices]
        except Exception as e:
            self.logger.error(f"Failed to perform global search for device '{device_name}' on {self.prtg_url}: {e}")
            return []

    def clone_device(self, obj_id, container_id, clone_name):
        """
        Clones an existing device template to create a new device.

        This method uses the PRTG API to duplicate a device (template) into the specified container
        with a new name.

        Args:
            obj_id (str): The object ID of the template device to clone.
            container_id (str): The ID of the container (group) where the new device will reside.
            clone_name (str): The name to assign to the newly cloned device.

        Returns:
            str: The device ID of the cloned device if successful.
            None: If cloning fails.
        """
        clone_url = f"{self.prtg_url}/api/duplicateobject.htm"
        clone_params = {
            'id': obj_id,
            'targetid': container_id,
            'name': clone_name,
            'apitoken': self.api_token
        }
        # Attempt to clone the device using the duplicateobject API endpoint.
        response = self.session.get(clone_url, params=clone_params, verify=False, timeout=60)
        response.raise_for_status()
        
        # After cloning, search for the new device and return its ID.
        return self.search_device(container_id, clone_name)

    def set_device_ip(self, device_id, ip_address):
        """
        Sets the IP address of a device on the PRTG server.

        This method calls the PRTG API to update the 'host' property of the device.

        Args:
            device_id (str): The ID of the device to update.
            ip_address (str): The IP address to assign to the device.

        Returns:
            bool: True if the IP address was successfully set; False otherwise.
        """
        set_ip_url = f"{self.prtg_url}/api/setobjectproperty.htm"
        set_ip_params = {
            'id': device_id,
            'name': 'host',
            'value': ip_address,
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(set_ip_url, params=set_ip_params, verify=False, timeout=60)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            # Optionally, log the exception here.
            return False

    def get_device_status(self, device_id):
        """
        Retrieves the status of a specified device from the PRTG server.

        It checks whether the device is enabled and reporting as "Up".

        Args:
            device_id (str): The ID of the device to check.

        Returns:
            bool: True if the device status is "Up" (enabled); False otherwise.
        """
        status_url = f"{self.prtg_url}/api/getobjectstatus.htm"
        status_params = {
            'id': device_id,
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(status_url, params=status_params, verify=False, timeout=60)
            response.raise_for_status()
            # Determine the device status by examining the response text.
            if "status=Up" in response.text:
                return True
            else:
                return False
        except requests.exceptions.RequestException as e:
            # In case of an error, return False indicating the device is not "Up".
            return False

    def enable_device(self, device_id):
        """
        Enables a device on the PRTG server.

        This method calls the API endpoint to unpause (enable) a device.

        Args:
            device_id (str): The ID of the device to enable.

        Returns:
            bool: True if the device was successfully enabled; False otherwise.
        """
        enable_url = f"{self.prtg_url}/api/pause.htm"
        enable_params = {
            'id': device_id,
            'action': '1',  # '1' indicates enabling the device.
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(enable_url, params=enable_params, verify=False, timeout=60)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            # Optionally, log the exception.
            return False

    def get_up_sensor_count(self):
        """
        Retrieves the count of sensors that are not paused.

        This method first queries the PRTG API to get the total sensor count, then 
        queries to get the count of paused sensors. The difference between these two 
        values represents the number of sensors that are not paused.

        Returns:
            int: The number of sensors that are not paused.
        """
        sensors_url = f"{self.prtg_url}/api/table.json"
        
        # Get the total sensor count.
        total_params = {
            'content': 'sensors',
            'output': 'json',
            'count': 'true',
            'apitoken': self.api_token
        }
        total_response = self.session.get(sensors_url, params=total_params, verify=False, timeout=60)
        total_response.raise_for_status()
        total_count = total_response.json().get('treesize', 0)
        
        # Get the count of paused sensors.
        paused_params = [
            ('content', 'sensors'),
            ('output', 'json'),
            ('count', 'true'),
            ('apitoken', self.api_token),
            ('filter_status', '7'),
            ('filter_status', '9'),
            ('filter_status', '8'),
            ('filter_status', '12'),
            ('filter_status', '11')
        ]
        paused_response = self.session.get(sensors_url, params=paused_params, verify=False, timeout=60)
        paused_response.raise_for_status()
        paused_count = paused_response.json().get('treesize', 0)
        
        # Return the count of sensors that are not paused.
        return total_count - paused_count

    
    def get_device_ip(self, device_id):
        """
        Retrieves the IP address of the specified device.

        This method calls the PRTG API to get the 'host' property and parses the XML response
        to extract the IP address.

        Args:
            device_id (str): The ID of the device to query.

        Returns:
            str: The IP address of the device, or None if it cannot be retrieved.
        """
        ip_url = f"{self.prtg_url}/api/getobjectproperty.htm"
        ip_params = {
            'id': device_id,
            'name': 'host',
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(ip_url, params=ip_params, verify=False, timeout=60)
            response.raise_for_status()
            # Parse the XML response to extract the IP address.
            root = ET.fromstring(response.text)
            ip_address = root.find('result').text if root.find('result') is not None else None
            return ip_address
        except (requests.exceptions.RequestException, ET.ParseError) as e:
            # Return None if there is an error during the request or parsing.
            return None
    
    def delete_device(self, container_id, device_name):
        """
        Deletes a device from the PRTG server.

        This method first searches for the device by its name within the specified container,
        and if found, calls the API to delete it.

        Args:
            container_id (str): The ID of the container (group) where the device resides.
            device_name (str): The name of the device to be deleted.

        Returns:
            bool: True if the device was successfully deleted; False otherwise.
        """
        # First, find the device ID by its name.
        device_id = self.search_device(container_id, device_name)
        if not device_id:
            # Optionally, log a warning that the device was not found.
            return False

        delete_url = f"{self.prtg_url}/api/deleteobject.htm"
        delete_params = {
            'id': device_id,
            'approve': '1',  # '1' indicates approval for deletion.
            'apitoken': self.api_token
        }

        try:
            response = self.session.get(delete_url, params=delete_params, verify=False, timeout=60)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            # Optionally, log the exception.
            return False
        
    def delete_monitor_by_id(self, device_id):
        """
        Deletes a monitor (device) by its device ID on the PRTG server.

        This method directly calls the PRTG API endpoint for deletion using the device ID.

        Args:
            device_id (str): The ID of the device to be deleted.

        Returns:
            bool: True if the device was successfully deleted; False otherwise.
        """
        delete_url = f"{self.prtg_url}/api/deleteobject.htm"
        delete_params = {
            'id': device_id,
            'approve': '1',  # '1' indicates approval for deletion.
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(delete_url, params=delete_params, verify=False, timeout=60)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            return False
    
    def get_template_sensor_count(self, template_obj_id):
        """
        Retrieves the sensor count for the specified template device.

        This method queries the PRTG API to count all sensors associated with the template device,
        identified by its object ID.

        Args:
            template_obj_id (str): The object ID of the template device.

        Returns:
            int: The number of sensors for the template device. Returns 0 if the count cannot be retrieved.
        """
        sensor_url = f"{self.prtg_url}/api/table.json"
        sensor_params = {
            'content': 'sensors',
            'output': 'json',
            'count': 'true',
            'id': template_obj_id,
            'apitoken': self.api_token  # Added API token for authentication.
        }
        try:
            response = self.session.get(sensor_url, params=sensor_params, verify=False, timeout=60)
            response.raise_for_status()
            return response.json().get('treesize', 0)
        except Exception as e:
            # Optionally, log the error here using your logger.
            return 0

    @staticmethod
    def add_monitor(pod_config, db_client):
        """
        Adds a monitoring device to PRTG based on the provided pod configuration.

        This static method retrieves available PRTG server configurations from the database,
        selects one with available monitoring capacity, and then attempts to find or create a
        monitoring device for the specified pod. It also sets the device's IP address based on the
        template object's IP plus the pod number, and enables the device. If successful, the method
        returns the URL of the newly configured PRTG monitor.

        Args:
            pod_config (dict): Configuration dictionary containing pod details and PRTG settings.
                Expected keys include:
                    - "pod_number": The unique identifier for the pod.
                    - "container_id": The PRTG container (group) ID where the device should reside.
                    - "prtg": A dictionary with keys:
                        - "name": The name to assign to the device.
                        - "object": The object ID of the template to clone if the device does not already exist.
            db_client (MongoClient): A MongoDB client instance for accessing the PRTG server collection.

        Returns:
            str: The URL of the newly created or updated PRTG monitor if successful.
            None: If the device could not be added.
        """
        
        # Retrieve the pod number from the configuration.
        pod = pod_config.get("pod_number")
        if not pod:
            logger.error("Pod number not specified in pod_config.")
            return None

        # Connect to the database and access the 'prtg' collection.
        db = db_client["labbuild_db"]
        collection = db["prtg"]

        # Retrieve the PRTG server details for vendor.
        server_data = (collection.find_one({"vendor_shortcode": pod_config["vendor_shortcode"]}) or 
                       collection.find_one({"vendor_shortcode": "ot"}))

        if not server_data or "servers" not in server_data:
            logger.error("No PRTG server configuration found for vendor 'cp'.")
            return None

        # Iterate over the available PRTG servers.
        for server in server_data["servers"]:
            # Instantiate a new PRTGManager for the current server configuration.
            prtg_obj = PRTGManager(server["url"], server["apitoken"])

            # Extract the container ID from the configuration.
            container_id = pod_config.get("prtg", {}).get("container")
            if not container_id:
                logger.error("Container ID not specified in pod_config.")
                continue

            # Determine the device name to use.
            clone_name = pod_config.get("prtg", {}).get("name")
            
            # Look for an existing device with the specified name.
            device_id = prtg_obj.search_device(container_id, clone_name)
            if not device_id:
                # If no existing device is found, attempt to clone a new device from a template.
                template_obj_id = pod_config.get("prtg", {}).get("object")
                if not template_obj_id:
                    logger.error("Template object ID not specified in pod_config.")
                    continue
                device_id = prtg_obj.clone_device(template_obj_id, container_id, clone_name)
                if not device_id:
                    logger.error(f"Failed to clone device for {clone_name}.")
                    continue

            # Instead of reading the IP from pod_config, fetch the template object's IP,
            # add the pod number to its last octet, and set the result as the new device's IP.
            template_obj_id = pod_config.get("prtg", {}).get("object")
            if not template_obj_id:
                logger.error("Template object ID not specified in pod_config.")
                continue

            template_ip = prtg_obj.get_device_ip(template_obj_id)
            if not template_ip:
                logger.error(f"Failed to retrieve IP from template object {template_obj_id}.")
                continue

            try:
                pod_number = int(pod)
            except ValueError:
                logger.error(f"Pod number '{pod}' is not a valid integer.")
                continue

            ip_parts = template_ip.split('.')
            if len(ip_parts) != 4:
                logger.error(f"Template IP '{template_ip}' is not a valid IPv4 address.")
                continue

            try:
                last_octet = int(ip_parts[3])
                if '11.1-vr' in clone_name:
                    last_octet = 101
            except ValueError:
                logger.error(f"Last octet of template IP '{template_ip}' is not a valid integer.")
                continue

            new_last_octet = last_octet + pod_number - 1
            if new_last_octet > 255:
                logger.error(f"Resulting IP's last octet {new_last_octet} exceeds 255.")
                continue

            new_ip = '.'.join(ip_parts[:3] + [str(new_last_octet)])
            if not prtg_obj.set_device_ip(device_id, new_ip):
                logger.error(f"Failed to set IP address {new_ip} for device {device_id}.")
                continue

            # Ensure that the device is enabled; if not, attempt to enable it.
            if not prtg_obj.get_device_status(device_id):
                if not prtg_obj.enable_device(device_id):
                    logger.error(f"Failed to enable device {device_id}.")
                    continue

            # Build the URL for the newly configured monitor and return it.
            monitor_url = f"{server['url']}/device.htm?id={device_id}"
            logger.info(f"Device added successfully: {monitor_url}")
            return monitor_url

        # If no available server was able to successfully add the device, log an error.
        logger.error("Failed to add device to any available PRTG server.")
        return None

    @staticmethod
    def delete_monitor(monitor_name: str, vendor_shortcode: str, db_client) -> bool:
        """
        Deletes all PRTG monitors with a specific name across all servers configured
        for a given vendor.

        This method queries the database to find all PRTG servers associated with the
        vendor_shortcode, then connects to each server to find and delete any device
        matching the monitor_name.

        Args:
            monitor_name (str): The name of the monitor(s) to delete.
            vendor_shortcode (str): The vendor code (e.g., 'cp', 'pa', 'f5', 'ot') to look up servers.
            db_client (MongoClient): A MongoDB client instance for accessing the PRTG server configuration.

        Returns:
            bool: True if all found monitors were deleted successfully or if none were found.
                  False if any error occurred during the process.
        """
        if not monitor_name or not vendor_shortcode or not db_client:
            logger.error("delete_monitor called with invalid arguments (name, vendor, or db_client missing).")
            return False

        # --- 1. Fetch PRTG server configurations for the specified vendor ---
        try:
            db = db_client["labbuild_db"]
            collection = db["prtg"]
            prtg_conf = collection.find_one({"vendor_shortcode": vendor_shortcode})
            if not prtg_conf or not prtg_conf.get("servers"):
                logger.warning(f"No PRTG server configuration found for vendor '{vendor_shortcode}'. Cannot delete monitor '{monitor_name}'.")
                # Return True because the desired state (monitor doesn't exist) is met.
                return True
            all_vendor_servers = prtg_conf["servers"]
        except Exception as e:
            logger.error(f"Database error fetching PRTG config for vendor '{vendor_shortcode}': {e}", exc_info=True)
            return False

        # --- 2. Iterate through each configured server for the vendor and delete monitors ---
        overall_success = True
        total_deleted_count = 0
        logger.info(f"Starting deletion process for monitor name '{monitor_name}' across all '{vendor_shortcode}' servers.")

        for server in all_vendor_servers:
            server_url = server.get("url")
            api_token = server.get("apitoken")
            server_name_log = server.get("name", server_url)

            if not server_url or not api_token:
                logger.warning(f"Skipping server {server_name_log}: configuration is incomplete (missing URL or API token).")
                continue

            try:
                prtg_mgr = PRTGManager(server_url, api_token)
                # Use the new global search to find all instances of the monitor on this server
                device_ids = prtg_mgr.search_device_globally(monitor_name)

                if not device_ids:
                    logger.info(f"Monitor '{monitor_name}' not found on server {server_name_log}.")
                    continue

                logger.info(f"Found {len(device_ids)} instance(s) of '{monitor_name}' on server {server_name_log}. Deleting...")
                for device_id in device_ids:
                    if prtg_mgr.delete_monitor_by_id(device_id):
                        logger.info(f"  - Successfully deleted monitor ID {device_id} from {server_name_log}.")
                        total_deleted_count += 1
                    else:
                        logger.error(f"  - FAILED to delete monitor ID {device_id} from {server_name_log}.")
                        overall_success = False  # Mark failure if any single deletion fails
            except Exception as e:
                logger.error(f"An error occurred while processing server {server_name_log} for monitor '{monitor_name}': {e}")
                overall_success = False

        logger.info(f"Deletion process for '{monitor_name}' complete. Total monitors deleted: {total_deleted_count}.")
        return overall_success