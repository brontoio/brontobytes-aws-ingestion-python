import json
import base64
import os
import tempfile


class Config:

    def __init__(self, event):
        self.filepath = tempfile.NamedTemporaryFile().name
        self.event = event


class DestinationConfig:

    def __init__(self):
        b64_destination_config = os.environ.get('destination_config')
        self.destination_config = json.loads(base64.b64decode(b64_destination_config))
        self.bronto_api_key = os.environ.get('bronto_api_key')
        self.bronto_endpoint = os.environ.get('bronto_endpoint')

    def _get_attribute_value(self, key, attribute_name):
        return self.destination_config[key][attribute_name]

    def get_log_name(self, key):
        return self._get_attribute_value(key, 'logname')

    def get_log_set(self, key):
        return self._get_attribute_value(key, 'logset')

    def get_log_type(self, key):
        return self._get_attribute_value(key, 'log_type')
