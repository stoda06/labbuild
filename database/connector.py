from sshtunnel import SSHTunnelForwarder
import pymongo
import atexit

class MongoConnector:
    def __init__(self, ssh_host, ssh_port, ssh_user, ssh_password,
                 mongo_host, mongo_db_name, mongo_port=27017):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.mongo_host = mongo_host
        self.mongo_db_name = mongo_db_name
        self.mongo_port = mongo_port
        self.tunnel = None
        self.client = None
        self.db = None
        self._connect()

    def _connect(self):
        self.tunnel = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_password=self.ssh_password,
            remote_bind_address=(self.mongo_host, self.mongo_port)
        )
        self.tunnel.start()
        self.client = pymongo.MongoClient('127.0.0.1', self.tunnel.local_bind_port)
        self.db = self.client[self.mongo_db_name]
        
        # Register cleanup function with atexit
        atexit.register(self._cleanup)

    def _cleanup(self):
        if self.client:
            self.client.close()
        if self.tunnel:
            self.tunnel.stop()
