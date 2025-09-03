import base64
import json
import os.path
import tempfile

from config import DestinationConfig, Config


def test_destination_config(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'dataset': 'my_dataset', 'collection': 'my_collection', 'log_type': 'my_log_type'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_log_type(key) == raw_config[key]['log_type']
    assert dest_config.get_dataset(key) == raw_config[key]['dataset']
    assert dest_config.get_collection(key) == raw_config[key]['collection']
    assert dest_config.get_keys() == [key]
    assert dest_config.get_client_type(key) is None

def test_legacy_destination_config(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'logname': 'my_dataset', 'logset': 'my_collection', 'log_type': 'my_log_type'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_log_type(key) == raw_config[key]['log_type']
    assert dest_config.get_dataset(key) == raw_config[key]['logname']
    assert dest_config.get_collection(key) == raw_config[key]['logset']
    assert dest_config.get_keys() == [key]

def test_destination_config_is_optional():
    key = 'some key'
    dest_config = DestinationConfig()
    assert dest_config.get_log_type(key) is None
    assert dest_config.get_dataset(key) is None
    assert dest_config.get_collection(key) is None

def test_destination_config_dataset_not_defined(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'collection': 'my_log_set', 'log_type': 'my_log_type'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_dataset(key) is None

def test_destination_config_collection_not_defined(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'log_type': 'my_log_type'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_collection(key) is None

def test_destination_config_logtype_not_defined(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'dataset': 'my_dataset', 'collection': 'my_collection'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_log_type(key) is None

def test_attributes_config(monkeypatch):
    monkeypatch.setenv('attributes', 'attr1=value1,attr2=value2')
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        assert config.get_resource_attributes() == {'attr1': 'value1', 'attr2': 'value2'}

def test_attributes_config_not_defined():
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        assert config.get_resource_attributes() == {}

def test_attributes_config_malformed(monkeypatch):
    monkeypatch.setenv('attributes', 'attr1=val=ue1,attr2=value2')
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        assert config.get_resource_attributes() == {'attr2': 'value2'}

def test_get_keys_no_config():
    dest_config = DestinationConfig()
    assert dest_config.get_keys() == []

def test_destination_config_client_type(monkeypatch):
    key = 'some_id'
    raw_config = {key: {'dataset': 'my_dataset', 'collection': 'my_collection', 'log_type': 'my_log_type',
        'client_type': 'my_client_type'}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_client_type(key) == raw_config[key]['client_type']

def test_paths_regex_config(monkeypatch):
    raw_config = [{'pattern': 'my expression', 'type': 'SomeLogType'},
                  {'pattern': 'my other expression', 'type': 'SomeOtherLogType'}]
    monkeypatch.setenv('paths_regex', base64.b64encode(json.dumps(raw_config).encode()).decode())
    dest_config = DestinationConfig()
    assert dest_config.get_paths_regex() == raw_config


def test_file_is_deleted_on_close():
    with tempfile.NamedTemporaryFile() as f:
        filepath = f.name
        config = Config({}, f.name)
        assert config.get_resource_attributes() == {}
    assert filepath is not None and not os.path.exists(filepath)
