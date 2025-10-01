import json
import base64
import os
import logging
from typing import List, Dict
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
BEDROCK_S3_LOG_TYPE = 'bedrock_s3'
CLOUDWATCH_LOG_TYPE = 'cloudwatch_log'


class Config:

    @staticmethod
    def _extract_kvps(property_value: str) -> Dict[str, str]:
        result = {}
        if property_value is not None and property_value != '':
            for raw_kvp in property_value.split(','):
                kvp = raw_kvp.split("=")
                if len(kvp) != 2:
                    logger.error('Malformed tag. Skipping. tag=%s', raw_kvp)
                    continue
                result[kvp[0].strip()] = kvp[1].strip()
        return result


    def __init__(self, event, filepath):
        self.filepath = filepath
        self.event = event
        self.path_regexes = os.environ.get('path_regexes')
        self.aggregator = os.environ.get('aggregator', 'default')
        raw_tags = os.environ.get('tags')
        self.tags = Config._extract_kvps(raw_tags)
        raw_attributes = os.environ.get('attributes')
        self.resource_attributes = Config._extract_kvps(raw_attributes)

    def get_resource_attributes(self):
        return self.resource_attributes

    def get_tags(self):
        return self.tags


class DestinationConfig:

    ONE_MB = 1000000

    @staticmethod
    def _get_json_config_from_s3(s3_uri, s3_client):
        bucket_name_and_path = s3_uri.replace('s3://', '').split('/')
        if len(bucket_name_and_path) < 2:
            raise Exception('Config S3 URI is malformed. Bucket name or path missing. s3_uri=%s', s3_uri)
        bucket_name = bucket_name_and_path[0]
        s3_key = '/'.join(bucket_name_and_path[1:])
        logger.info('Retrieving configuration from S3. config_s3_uri=%s, bucket_name=%s, s3_key=%s',
                    s3_uri, bucket_name, s3_key)
        try:
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=s3_key,
            )
            return json.loads(response['Body'].read())
        except Exception as e:
            logger.error('Cannot get destination config from S3. bucket_name=%s, s3_key=%s',
                         bucket_name, s3_key)
            raise e


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
                s3_client = boto3.client('s3')
                self.destination_config = DestinationConfig._get_json_config_from_s3(self.config_s3_uri, s3_client)
        self.paths_regex = []
        b64_paths_regex_config = os.environ.get('paths_regex')
        if b64_paths_regex_config is not None:
            try:
                self.paths_regex = json.loads(base64.b64decode(b64_paths_regex_config))
            except json.decoder.JSONDecodeError as e:
                logger.error('Cannot read paths regex config. Json format expected. error=%s', e)
                raise e
        else:
            self.config_paths_regex_s3_uri = os.environ.get('CONFIG_PATHS_REGEX_S3_URI')
            if self.config_paths_regex_s3_uri is not None:
                s3_client = boto3.client('s3')
                self.paths_regex = DestinationConfig._get_json_config_from_s3(self.config_paths_regex_s3_uri, s3_client)
        logger.info('Path regexes collected. paths_regex=%s', self.paths_regex)

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

    def get_client_type(self, key):
        return self._get_attribute_value(key, 'client_type')

    def get_paths_regex(self):
        return self.paths_regex
