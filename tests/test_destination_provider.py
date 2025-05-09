import json
import base64
from exceptions import LogTypeMissingException

from config import DestinationConfig, Config, CLOUDWATCH_LOG_TYPE
from data_retriever import CloudwatchDataRetriever, DataRetriever, S3DataRetriever
from destination_provider import DestinationProvider

import pytest

def _get_data(data_retriever, log_group_name):
    data_retriever.log_group_name = log_group_name


def test_s3_requires_log_type_in_config(monkeypatch):
    bucket_name = 'my_bucket'
    s3_key = 'my_key'
    config = Config({'Records': [{'s3': {'bucket': {'name': bucket_name}, 'object': {'key': s3_key}}}]})
    data_id = 'some string not in dest_config'
    dest_config = DestinationConfig()
    data_retriever = S3DataRetriever(config, bucket_name, s3_key)
    destination_provider = DestinationProvider(dest_config, data_retriever)
    with pytest.raises(LogTypeMissingException) as _:
        destination_provider.get_type(data_id)

def test_cloudwatch_no_config(monkeypatch):
    log_group_name = 'whatever'
    config = Config({})
    dest_config = DestinationConfig()
    data_retriever = CloudwatchDataRetriever(config)
    monkeypatch.setattr(DataRetriever, 'get_data', lambda: _get_data(data_retriever, log_group_name))
    destination_provider = DestinationProvider(dest_config, data_retriever)
    assert destination_provider.get_type(log_group_name) == CLOUDWATCH_LOG_TYPE
    assert destination_provider.get_dataset(log_group_name) == log_group_name
    assert destination_provider.get_collection(log_group_name) is None


def test_cloudwatch_default_collection(monkeypatch):
    log_group_name = 'whatever'
    cw_default_collection = 'my_default_collection'
    monkeypatch.setenv('cloudwatch_default_collection', cw_default_collection)
    config = Config({})
    dest_config = DestinationConfig()
    data_retriever = CloudwatchDataRetriever(config)
    destination_provider = DestinationProvider(dest_config, data_retriever)
    assert destination_provider.get_collection(log_group_name) == cw_default_collection


def test_cloudwatch_config_takes_precedence(monkeypatch):
    log_group_name = 'whatever'
    log_group_name_from_config = f'not {log_group_name}'
    config = Config({})
    data_retriever = CloudwatchDataRetriever(config)
    log_set_from_config = f'not {data_retriever.get_name()}'
    raw_dest_config = {log_group_name: {'dataset': log_group_name_from_config, 'collection': log_set_from_config}}
    monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_dest_config).encode()).decode())
    dest_config = DestinationConfig()
    monkeypatch.setattr(DataRetriever, 'get_data', lambda: _get_data(data_retriever, log_group_name_from_config))
    destination_provider = DestinationProvider(dest_config, data_retriever)
    assert destination_provider.get_type(log_group_name) == CLOUDWATCH_LOG_TYPE
    assert destination_provider.get_dataset(log_group_name) == log_group_name_from_config
    assert destination_provider.get_collection(log_group_name) == log_set_from_config
