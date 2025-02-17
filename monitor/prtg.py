import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET
import logging
from urllib.parse import quote_plus, urlparse, parse_qs

# Disable InsecureRequestWarning if not verifying SSL certificates.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_logger():
    """
    Sets up and returns a logger for the PRTGManager class.

    This function configures a logger with a specific format (including timestamp,
    logger name, level, and message) and sets the logging level to DEBUG. It ensures that
    multiple handlers are not added if the logger already exists.

    Returns:
        logging.Logger: A configured logger instance for debugging and status tracking.
    """
    logger = logging.getLogger("PRTGManager")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger

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
        Retrieves the count of sensors that are currently reporting as "Up".

        This method queries the PRTG API to count sensors that have a status indicating they are operational.

        Returns:
            int: The number of sensors with a status of "Up".
        """
        sensors_url = f"{self.prtg_url}/api/table.json"
        sensors_params = {
            'content': 'sensors',
            'output': 'json',
            'count': 'true',
            'filter_status': '3',  # Status 3 corresponds to "Up".
            'apitoken': self.api_token
        }
        response = self.session.get(sensors_url, params=sensors_params, verify=False, timeout=60)
        response.raise_for_status()
        return response.json().get('treesize', 0)
    
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
        logger = setup_logger()
        
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
    def delete_monitor(prtg_monitor_url, db_client):
        """
        Deletes a PRTG monitor based on the provided monitor URL.

        This method extracts the server base URL and the device ID from the given monitor URL.
        It then queries the database (using the provided MongoDB client) to retrieve the PRTG server
        configuration (and associated API token) based on the server URL. Finally, it instantiates a
        PRTGManager for that server and deletes the monitor (device) using its device ID.

        Args:
            prtg_monitor_url (str): The full URL of the PRTG monitor 
                (e.g., "https://server/device.htm?id=1234").
            db_client (MongoClient): A MongoDB client instance for accessing the PRTG server configuration.

        Returns:
            bool: True if the monitor was successfully deleted; False otherwise.
        """
        logger = setup_logger()

        # Parse the monitor URL to extract its components.
        parsed_url = urlparse(prtg_monitor_url)
        # Reconstruct the base server URL from the scheme and network location.
        server_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        # Extract the query parameters from the URL.
        query_params = parse_qs(parsed_url.query)
        # Extract the device ID from the query parameters (key 'id').
        device_id = query_params.get('id', [None])[0]
        if not device_id:
            logger.error("Device ID not found in the provided monitor URL.")
            return False

        # Access the 'prtg' collection in the labbuild_db.
        db = db_client["labbuild_db"]
        collection = db["prtg"]

        # Query for a record that contains a server with the matching URL.
        server_record = collection.find_one({"servers.url": server_url})
        if not server_record:
            logger.error("No PRTG server configuration found for server URL: %s", server_url)
            return False

        # Find the API token from the matching server in the servers list.
        api_token = None
        for server in server_record.get("servers", []):
            if server.get("url") == server_url:
                api_token = server.get("apitoken")
                break

        if not api_token:
            logger.error("API token not found for server URL: %s", server_url)
            return False

        # Instantiate a PRTGManager with the found API token.
        prtg_manager = PRTGManager(server_url, api_token)
        # Delete the monitor (device) using the extracted device ID.
        return prtg_manager.delete_monitor_by_id(device_id)
