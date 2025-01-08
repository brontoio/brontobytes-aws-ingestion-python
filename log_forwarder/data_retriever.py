import gzip
import json
import os
import base64
import logging

from config import Config, DestinationConfig
from clients import S3Client

logger = logging.getLogger()


class DataRetriever:

    def get_name(self):
        raise NotImplementedError()

    def get_collection_type(self):
        raise NotImplementedError()

    def get_data(self):
        raise NotImplementedError()

    def get_data_id(self):
        raise NotImplementedError()


class S3DataRetriever(DataRetriever):

    def __init__(self, config: Config, bucket_name, s3_key):
        self.src_bucket_name = bucket_name
        self.src_key = s3_key
        self.s3_client = S3Client(config.filepath)
        logger.debug('type=%s, bucket=%s, s3_key=%s', self.__class__, self.src_bucket_name, self.src_key)

    def get_collection_type(self):
        return 's3'

    def get_name(self):
        raise NotImplementedError()

    def get_data(self):
        self.s3_client.download(self.src_bucket_name, self.src_key)

    def get_data_id(self):
        raise NotImplementedError()


class S3AccessLogsRetriever(S3DataRetriever):

    def get_name(self):
        return 'S3AccessLogs'

    def get_data_id(self):
        return self.src_key.split('/')[1]


class VPCFlowLogsRetriever(S3DataRetriever):

    def get_name(self):
        return 'VPCFlowLogs'

    def get_data_id(self):
        return 'vpc_flow_log'


class LBAccessLogsRetriever(S3DataRetriever):

    def get_name(self):
        return 'LBAccessLogs'

    def get_data_id(self):
        return self.src_key.split('/')[-1].split('.')[1].split('_')[0]


class CloudtrailLogsRetriever(S3DataRetriever):

    def get_name(self):
        return 'Cloudtrail'

    def get_data_id(self):
        return 'cloudtrail'


class CloudfrontLogsRetriever(S3DataRetriever):

    def get_name(self):
        return 'Cloudfront'

    def get_data_id(self):
        return os.path.basename(self.src_key).split('.')[0]


class CloudwatchDataRetriever(DataRetriever):

    def get_name(self):
        return 'Cloudwatch'

    def get_collection_type(self):
        return 'cloudwatch'

    def __init__(self, config: Config):
        self.config = config
        self.log_group_name = None

    def get_data(self):
        data = json.loads(gzip.decompress(base64.b64decode(self.config.event['awslogs']['data'])).decode())
        self.log_group_name = data['logGroup']
        with open(self.config.filepath, 'wb') as f:
            for i in range(0, len(data['logEvents'])):
                line = data['logEvents'][i]['message'] + ('\n' if i < len(data['logEvents']) - 1 else '')
                f.write(line.encode())

    def get_data_id(self):
        return self.log_group_name


class DataRetrieverFactory:

    @staticmethod
    def get_data_retrievers(config: Config, dest_config: DestinationConfig) -> list[DataRetriever]:
        data_retrievers = []
        if 'Records' in config.event:
            for record in config.event['Records']:
                filename = os.path.basename(record['s3']['object']['key'])
                bucket_name = record['s3']['bucket']['name']
                s3_key = record['s3']['object']['key']
                if 'CloudTrail' in s3_key:
                    data_retrievers.append(CloudtrailLogsRetriever(config, bucket_name, s3_key))
                elif 'elasticloadbalancing' in s3_key:
                    data_retrievers.append(LBAccessLogsRetriever(config, bucket_name, s3_key))
                elif 'vpcflowlogs' in config.event['Records'][0]['s3']['object']['key']:
                    data_retrievers.append(VPCFlowLogsRetriever(config, bucket_name, s3_key))
                elif (filename.split('.')[0] in dest_config.get_keys() and
                        dest_config.get_log_type(filename.split('.')[0]) == 'cf_standard_access_log'):
                    data_retrievers.append(CloudfrontLogsRetriever(config, bucket_name, s3_key))
                else:
                    data_retrievers.append(S3AccessLogsRetriever(config, bucket_name, s3_key))
        if 'awslogs' in config.event:
            data_retrievers.append(CloudwatchDataRetriever(config))
        return data_retrievers
