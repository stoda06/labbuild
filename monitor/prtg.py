import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET

# Disable InsecureRequestWarning if not verifying SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class PRTGManager:
    def __init__(self, prtg_url, api_token):
        self.prtg_url = prtg_url
        self.api_token = api_token
        self.session = requests.Session()
        
        # Set up retries for the session
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
    
    def create_session(self):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def search_device(self, container_id, clone_name):
        search_url = f"{self.prtg_url}/api/table.json"
        search_params = {
            'content': 'devices',
            'output': 'json',
            'columns': 'objid,device',
            'id': container_id,
            'filter_name': clone_name,
            'apitoken': self.api_token
        }
        response = self.session.get(search_url, params=search_params, verify=False, timeout=60)
        response.raise_for_status()
        devices = response.json().get('devices', [])
        
        # Return the device ID if the device is found
        for device in devices:
            if device['device'] == clone_name:
                return device['objid']
        return None

    def clone_device(self, obj_id, container_id, clone_name):
        clone_url = f"{self.prtg_url}/api/duplicateobject.htm"
        clone_params = {
            'id': obj_id,
            'targetid': container_id,
            'name': clone_name,
            'apitoken': self.api_token
        }
        response = self.session.get(clone_url, params=clone_params, verify=False, timeout=60)
        response.raise_for_status()
        
        # Return the ID of the cloned device
        return self.search_device(container_id, clone_name)
        return response.text.strip()

    def set_device_ip(self, device_id, ip_address):
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
            return False

    def get_device_status(self, device_id):
        status_url = f"{self.prtg_url}/api/getobjectstatus.htm"
        status_params = {
            'id': device_id,
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(status_url, params=status_params, verify=False, timeout=60)
            response.raise_for_status()

            # Check if the status indicates the device is enabled
            if "status=Up" in response.text:  # Or use the specific key/value that indicates "Enabled" in the response
                return True
            else:
                return False

        except requests.exceptions.RequestException as e:
            return False

    def enable_device(self, device_id):
        enable_url = f"{self.prtg_url}/api/pause.htm"
        enable_params = {
            'id': device_id,
            'action': '1',  # '1' means enable the object
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(enable_url, params=enable_params, verify=False, timeout=60)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            return False

    def get_up_sensor_count(self):
        sensors_url = f"{self.prtg_url}/api/table.json"
        sensors_params = {
            'content': 'sensors',
            'output': 'json',
            'count': 'true',
            'filter_status': '3',  # Status 3 corresponds to "Up"
            'apitoken': self.api_token
        }
        response = self.session.get(sensors_url, params=sensors_params, verify=False, timeout=60)
        response.raise_for_status()
        return response.json().get('treesize', 0)
    
    def get_device_ip(self, device_id):
        """
        Retrieves the IP address of the specified device.
        
        :param device_id: The ID of the PRTG device.
        :return: The IP address of the device or None if not found.
        """
        ip_url = f"{self.prtg_url}/api/getobjectproperty.htm"
        ip_params = {
            'id': device_id,
            'name': 'host',  # 'host' is the property name for IP addresses in PRTG
            'apitoken': self.api_token
        }
        try:
            response = self.session.get(ip_url, params=ip_params, verify=False, timeout=60)
            response.raise_for_status()
            
            # Parse the XML response to get the IP address
            root = ET.fromstring(response.text)
            ip_address = root.find('result').text if root.find('result') is not None else None
            return ip_address
        except (requests.exceptions.RequestException, ET.ParseError) as e:
            return None
    
    def delete_device(self, container_id, device_name):
        """
        Deletes a device from the specified PRTG server.

        :param container_id: The ID of the container or group where the device is located.
        :param device_name: The name of the device to be deleted.
        :return: True if the deletion was successful, False otherwise.
        """
        # Search for the device by name to get its ID
        device_id = self.search_device(container_id, device_name)
        if not device_id:
            self.logger.warning(f"Device '{device_name}' not found in container '{container_id}'.")
            return False

        delete_url = f"{self.prtg_url}/api/deleteobject.htm"
        delete_params = {
            'id': device_id,
            'approve': '1',  # '1' means approve the deletion
            'apitoken': self.api_token
        }

        try:
            response = self.session.get(delete_url, params=delete_params, verify=False, timeout=60)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            return False


checkpoint_server_info = {
    "servers": [
        {
            "url": "https://prtg-ps.rededucation.com",
            "apitoken": "XMXSVSUCTMZYJGR6FV3CAHUECGGXJDKKWC2QJ63X7Q======"
        },
        {
            "url": "https://prtg-ps-2.rededucation.com",
            "apitoken": "HH2DXXUPO5PKAE52ZNPV3GWGGPO2E7FKZIAOYR3264======"
        },
        {
            "url": "https://prtg-ps-3.rededucation.com",
            "apitoken": "5TE4V67ZSUHFV6JVJOUH7YDHCQU5TIOHXQ2NVY24PA======"
        },
        {
            "url": "https://prtg-ps-4.rededucation.com",
            "apitoken": "KOS55L55NCW6VFUY4B3GYYXMMFEAPWA6LN5XAAP6IM======"
        }
    ]
}
palo_server_info= {
    "servers": [
        {
            "url": "https://prtg-pa.rededucation.com",
            "apitoken": "DHWNH42WSRWRXAMICFC4VRMTTWALOK72YDP6L7OJNE======"
        },
        {
            "url": "https://prtg-pa-2.rededucation.com",
            "apitoken": "K6FVEYZZ2KXXYI6MHCO7W5IDWRVBUPVPKY4CKBOQNM======"
        }
    ]
}
others_server_info = {
    "servers": [
        {
            "url": "https://prtg-lab.rededucation.com",
            "apitoken": "UEJHLXCFZB2TI5IGRIKWLBWO3QLID5KQ2KRB3YP2TQ======",
        },
        {
            "url": "https://prtg-lab-2.rededucation.com",
            "apitoken": "ESAMZYQLTIDP3JW3MCIZZJLUXDHKD3DHDJXD4XGFTA======",
        }
    ]
}