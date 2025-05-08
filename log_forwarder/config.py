import json
import base64
import os
import tempfile
import logging
from typing import List
import boto3

logger = logging.getLogger()

S3_ACCESS_LOG_TYPE = 's3_access_log'
ALB_ACCESS_LOG_TYPE = 'alb_access_log'
NLB_ACCESS_LOG_TYPE = 'nlb_access_log'
CLASSIC_LB_ACCESS_LOG_TYPE = 'clb_access_log'
CLOUDFRONT_REALTIME_ACCESS_LOG_TYPE = 'cf_realtime_access_log'
CLOUDFRONT_STANDARD_ACCESS_LOG_TYPE = 'cf_standard_access_log'
CLOUDTRAIL_LOG_TYPE = 'cloudtrail_log'
VPC_FLOW_LOG_TYPE = 'vpc_flow_log'
CLOUDWATCH_LOG_TYPE = 'cloudwatch_log'


class Config:

    def __init__(self, event):
        self.filepath = tempfile.NamedTemporaryFile().name
        self.event = event
        raw_attributes = os.environ.get('attributes')
        self.resource_attributes = {}
        if raw_attributes is not None and raw_attributes != '':
            for raw_kvp in raw_attributes.split(','):
                split_kvp = raw_kvp.split("=")
                if len(split_kvp) != 2:
                    logger.error('Malformed attribute. Skipping. attribute=%s', raw_kvp)
                    continue
                self.resource_attributes[split_kvp[0]] = split_kvp[1]

    def get_resource_attributes(self):
        return self.resource_attributes


class DestinationConfig:

    ONE_MB = 1000000

    def __init__(self):
        self.bronto_api_key = os.environ.get('bronto_api_key')
        self.bronto_endpoint = os.environ.get('bronto_endpoint')
        self.max_batch_size = int(os.environ.get('max_batch_size', DestinationConfig.ONE_MB))
        self.cloudwatch_default_collection = os.environ.get('cloudwatch_default_collection')
        b64_destination_config = os.environ.get('destination_config')
        if b64_destination_config is not None:
            try:
                self.destination_config = json.loads(base64.b64decode(b64_destination_config))
            except json.decoder.JSONDecodeError as e:
                logger.error('Cannot read destination config. Json format expected. error=%s', e)
                raise e
        else:
            self.destination_config = None
            self.config_s3_uri = os.environ.get('CONFIG_S3_URI')
            if self.config_s3_uri is not None:
                bucket_name_and_path = self.config_s3_uri.replace('s3://', '').split('/')
                if len(bucket_name_and_path) < 2:
                    raise Exception('Config S3 URI is malformed. Bucket name or path missing. s3_uri=%s',
                                    self.config_s3_uri)
                bucket_name = bucket_name_and_path[0]
                s3_key = '/'.join(bucket_name_and_path[1:])
                self.s3_client = boto3.client('s3')
                try:
                    response = self.s3_client.get_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                    )
                    self.destination_config = json.loads(response['Body'].read())
                except Exception as e:
                    logger.error('Cannot get destination config from S3. bucket_name=%s, s3_key=%s',
                                 bucket_name, s3_key)
                    raise e

    def _get_attribute_value(self, key, attribute_name):
        return self.destination_config.get(key, {}).get(attribute_name) if self.destination_config is not None else None

    def get_dataset(self, key):
        dataset_value = self._get_attribute_value(key, 'logname')
        if dataset_value is not None:
            return dataset_value
        return self._get_attribute_value(key, 'dataset')

    def get_collection(self, key):
        collection_value = self._get_attribute_value(key, 'logset')
        if collection_value is not None:
            return collection_value
        return self._get_attribute_value(key, 'collection')

    def get_log_type(self, key):
        return self._get_attribute_value(key, 'log_type')

    def get_keys(self) -> List[str]:
        return list(self.destination_config.keys()) if self.destination_config is not None else []

    def get_cloudwatch_default_collection(self):
        return self.cloudwatch_default_collection