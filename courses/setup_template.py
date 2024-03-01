class SetupTemplate:
    def __init__(self, setup_dict):
        self.setup_dict = setup_dict
    
    def replace_placeholders(self, placeholder, replacement):
        """
        Replace placeholders in the setup dictionary.
        """
        self.setup_dict = self._replace_placeholders_recursive(self.setup_dict, placeholder, replacement)
    
    @staticmethod
    def _replace_placeholders_recursive(data, placeholder, replacement):
        if isinstance(data, dict):
            return {k: SetupTemplate._replace_placeholders_recursive(v, placeholder, replacement) for k, v in data.items()}
        elif isinstance(data, list):
            return [SetupTemplate._replace_placeholders_recursive(item, placeholder, replacement) for item in data]
        elif isinstance(data, str):
            return data.replace(placeholder, replacement)
        else:
            return data
    
    def get_component_details(self, component_name):
        """
        Retrieve details for a specific component by name.
        """
        components = self.setup_dict.get("components", [])
        for component in components:
            if component.get("component_name") == component_name:
                return component
        return None
    
    def get_network_configuration(self):
        """
        Retrieve the network configuration for the setup.
        """
        return self.setup_dict.get("network", {})
