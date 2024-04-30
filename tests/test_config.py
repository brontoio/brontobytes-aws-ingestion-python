import base64
import json

from config import DestinationConfig


def test_destination_config(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'logname': 'my_log_name', 'logset': 'my_log_set', 'log_type': 'my_log_type'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_log_type(key) == raw_config[key]['log_type']
    assert dest_config.get_log_name(key) == raw_config[key]['logname']
    assert dest_config.get_log_set(key) == raw_config[key]['logset']
