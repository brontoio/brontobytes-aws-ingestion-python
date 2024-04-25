import json
import base64
import os
import tempfile


class Config:

    def __init__(self, event):
        self.filepath = tempfile.NamedTemporaryFile().name
        self.event = event


class DestinationConfig:

    ONE_MB = 1000000

    def __init__(self):
        b64_destination_config = os.environ.get('destination_config')
        self.destination_config = json.loads(base64.b64decode(b64_destination_config))
        self.bronto_api_key = os.environ.get('bronto_api_key')
        self.bronto_endpoint = os.environ.get('bronto_endpoint')
        self.max_batch_size = int(os.environ.get('max_batch_size', DestinationConfig.ONE_MB))

    def _get_attribute_value(self, key, attribute_name):
        return self.destination_config.get(key, {}).get(attribute_name)

    def get_log_name(self, key):
        log_name = self._get_attribute_value(key, 'logname')
        return log_name if log_name is not None else 'default'

    def get_log_set(self, key):
        log_set = self._get_attribute_value(key, 'logset')
        return log_set if log_set is not None else 'Default'

    def get_log_type(self, key):
        return self._get_attribute_value(key, 'log_type')
