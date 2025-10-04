import json
import base64
import tempfile
from exceptions import LogTypeMissingException

from config import DestinationConfig, Config, CLOUDWATCH_LOG_TYPE
from data_retriever import CloudwatchDataRetriever, DataRetriever, S3DataRetriever, CustomS3Retriever, \
    DataRetrieverFactory, logger
from destination_provider import DestinationProvider

import pytest

def _get_data(data_retriever, log_group_name):
    data_retriever.log_group_name = log_group_name


def test_s3_requires_log_type_in_config():
    bucket_name = 'my_bucket'
    s3_key = 'my_key'
    with tempfile.NamedTemporaryFile() as f:
        config = Config({'Records': [{'s3': {'bucket': {'name': bucket_name}, 'object': {'key': s3_key}}}]}, f.name)
        data_id = 'some string not in dest_config'
        dest_config = DestinationConfig()
        data_retriever = S3DataRetriever(config, bucket_name, s3_key)
        destination_provider = DestinationProvider(dest_config, data_retriever)
        with pytest.raises(LogTypeMissingException) as _:
            destination_provider.get_type(data_id)


def test_s3_custom_path():
    bucket_name = 'my_bucket'
    expected_data_id = 'my_dest_config_id'
    s3_key = f'my_key/{expected_data_id}/other_values'
    with tempfile.NamedTemporaryFile() as f:
        config = Config({'Records': [{'s3': {'bucket': {'name': bucket_name}, 'object': {'key': s3_key}}}]}, f.name)
        dest_config = DestinationConfig()
        dest_config.paths_regex = [{'pattern': '[^/]*/(?P<dest_config_id>[^/]+)'}]
        data_retrievers = DataRetrieverFactory.get_data_retrievers(config, dest_config)
        assert len(data_retrievers) == 1
        data_retriever = data_retrievers[0]
        assert type(data_retriever) == CustomS3Retriever
        assert data_retriever.get_data_id() == expected_data_id

def test_cloudwatch_no_config(monkeypatch):
    log_group_name = 'whatever'
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        dest_config = DestinationConfig()
        data_retriever = CloudwatchDataRetriever(config)
        monkeypatch.setattr(DataRetriever, 'get_data', lambda: _get_data(data_retriever, log_group_name))
        destination_provider = DestinationProvider(dest_config, data_retriever)
        assert destination_provider.get_type(log_group_name) == CLOUDWATCH_LOG_TYPE
        assert destination_provider.get_dataset(log_group_name) == log_group_name
        assert destination_provider.get_collection(log_group_name) is None
        assert destination_provider.get_dataset_tags(log_group_name) == {'aws_log_type': 'cloudwatch_log'}


def test_cloudwatch_default_collection(monkeypatch):
    log_group_name = 'whatever'
    cw_default_collection = 'my_default_collection'
    monkeypatch.setenv('cloudwatch_default_collection', cw_default_collection)
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        dest_config = DestinationConfig()
        data_retriever = CloudwatchDataRetriever(config)
        destination_provider = DestinationProvider(dest_config, data_retriever)
        assert destination_provider.get_collection(log_group_name) == cw_default_collection


def test_cloudwatch_config_takes_precedence(monkeypatch):
    log_group_name = 'whatever'
    log_group_name_from_config = f'not {log_group_name}'
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        data_retriever = CloudwatchDataRetriever(config)
        log_set_from_config = f'not {data_retriever.get_name()}'
        raw_dest_config = {log_group_name: {'dataset': log_group_name_from_config, 'collection': log_set_from_config}}
        monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_dest_config).encode()).decode())
        dest_config = DestinationConfig()
        # mock _get_data in order to associate log group name to the data retriever
        monkeypatch.setattr(DataRetriever, 'get_data', lambda: _get_data(data_retriever, log_group_name_from_config))
        destination_provider = DestinationProvider(dest_config, data_retriever)
        assert destination_provider.get_type(log_group_name) == CLOUDWATCH_LOG_TYPE
        assert destination_provider.get_dataset(log_group_name) == log_group_name_from_config
        assert destination_provider.get_collection(log_group_name) == log_set_from_config


def test_custom_dataset_tags(monkeypatch):
    log_group_name = 'whatever'
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        data_retriever = CloudwatchDataRetriever(config)
        auto_set_tags = {'aws_log_type': 'cloudwatch_log'}
        dataset_custom_tags = {'key1': 'val1', 'key2': 'val2'}
        raw_dest_config = {log_group_name: {'tags': dataset_custom_tags}}
        monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_dest_config).encode()).decode())
        dest_config = DestinationConfig()
        destination_provider = DestinationProvider(dest_config, data_retriever)
        tags = auto_set_tags
        tags.update(dataset_custom_tags)
        assert destination_provider.get_dataset_tags(log_group_name) == tags


def test_custom_dataset_tags_not_set(monkeypatch):
    log_group_name = 'whatever'
    with tempfile.NamedTemporaryFile() as f:
        config = Config({}, f.name)
        data_retriever = CloudwatchDataRetriever(config)
        auto_set_tags = {'aws_log_type': 'cloudwatch_log'}
        raw_dest_config = {log_group_name: {}}
        monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_dest_config).encode()).decode())
        dest_config = DestinationConfig()
        destination_provider = DestinationProvider(dest_config, data_retriever)
        assert destination_provider.get_dataset_tags(log_group_name) == auto_set_tags


def test_custom_dataset_tags_overwrites_auto_set_tags(monkeypatch):
    log_group_name = 'whatever'
    with tempfile.NamedTemporaryFile() as f:
        auto_set_tag_key = 'aws_log_type'
        config = Config({}, f.name)
        data_retriever = CloudwatchDataRetriever(config)
        auto_set_tags = {auto_set_tag_key: 'cloudwatch_log'}
        dataset_custom_tags = {'key1': 'val1', auto_set_tag_key: 'val2'}
        raw_dest_config = {log_group_name: {'tags': dataset_custom_tags}}
        monkeypatch.setenv('destination_config', base64.b64encode(json.dumps(raw_dest_config).encode()).decode())
        dest_config = DestinationConfig()
        destination_provider = DestinationProvider(dest_config, data_retriever)
        tags = auto_set_tags
        tags.update(dataset_custom_tags)
        assert destination_provider.get_dataset_tags(log_group_name) == tags
