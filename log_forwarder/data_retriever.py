import gzip
import json
import os
import base64
from typing import Union

from config import Config, DestinationConfig
from clients import S3Client


class DataRetriever:

    def get_data(self):
        raise NotImplementedError()

    def get_data_id(self):
        raise NotImplementedError()


class S3DataRetriever(DataRetriever):

    def __init__(self, config: Config, index: int):
        self.src_bucket_name = config.event['Records'][index]['s3']['bucket']['name']
        self.src_key = config.event['Records'][index]['s3']['object']['key']
        self.s3_client = S3Client(config.filepath)

    def get_data(self):
        self.s3_client.download(self.src_bucket_name, self.src_key)

    def get_data_id(self):
        raise NotImplementedError()


class S3AccessLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return self.src_key.split('/')[1]


class VPCFlowLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return 'vpc_flow_log'


class LBAccessLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return self.src_key.split('/')[-1].split('.')[1].split('_')[0]


class CloudtrailLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return 'cloudtrail'


class CloudfrontLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return os.path.basename(self.src_key).split('.')[0]


class CloudwatchDataRetriever(DataRetriever):

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
    def get_data_retrievers(config: Config, dest_config: DestinationConfig) -> list[Union[DataRetriever, None]]:
        if 'Records' in config.event:
            filename = os.path.basename(config.event['Records'][0]['s3']['object']['key'])
            if 'CloudTrail' in config.event['Records'][0]['s3']['object']['key']:
                for i in range(0, len(config.event['Records'])):
                    yield CloudtrailLogsRetriever(config, i)
            if 'elasticloadbalancing' in config.event['Records'][0]['s3']['object']['key']:
                for i in range(0, len(config.event['Records'])):
                    yield LBAccessLogsRetriever(config, i)
            if 'vpcflowlogs' in config.event['Records'][0]['s3']['object']['key']:
                for i in range(0, len(config.event['Records'])):
                    yield VPCFlowLogsRetriever(config, i)
            if (filename.split('.')[0] in dest_config.get_keys() and
                    dest_config.get_log_type(filename.split('.')[0]) == 'cf_standard_access_log'):
                for i in range(0, len(config.event['Records'])):
                    yield CloudfrontLogsRetriever(config, i)
            else:
                for i in range(0, len(config.event['Records'])):
                    yield S3AccessLogsRetriever(config, i)
        if 'awslogs' in config.event:
            yield CloudwatchDataRetriever(config)
        else:
            yield None
