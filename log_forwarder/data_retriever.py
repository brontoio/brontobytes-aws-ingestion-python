import gzip
import json
import base64

from config import Config
from clients import S3Client


class DataRetriever:

    def get_data(self):
        raise NotImplementedError()

    def get_data_id(self):
        raise NotImplementedError()


class S3DataRetriever(DataRetriever):

    def __init__(self, config: Config):
        self.src_bucket_name = config.event['Records'][0]['s3']['bucket']['name']
        self.src_key = config.event['Records'][0]['s3']['object']['key']
        self.s3_client = S3Client(config.filepath)

    def get_data(self):
        self.s3_client.download(self.src_bucket_name, self.src_key)

    def get_data_id(self):
        raise NotImplementedError()


class S3AccessLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return self.src_key.split('/')[1]


class LBAccessLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return self.src_key.split('/')[-1].split('.')[1]


class CloudtrailLogsRetriever(S3DataRetriever):

    def get_data_id(self):
        return 'cloudtrail'


class CloudwatchDataRetriever(DataRetriever):

    def __init__(self, config: Config):
        self.config = config
        self.log_group_name = None

    def get_data(self):
        data = json.loads(gzip.decompress(base64.b64decode(self.config.event['awslogs']['data'])).decode())
        self.log_group_name = data['logGroup']
        with open(self.config.filepath, 'wb') as f:
            for line in data['logEvents']:
                f.write((line['message']).encode())

    def get_data_id(self):
        return self.log_group_name


class DataRetrieverFactory:

    @staticmethod
    def get_data_retriever(event, config: Config):
        if 'Records' in event and 'CloudTrail' in event['Records'][0]['s3']['object']['key']:
            return CloudtrailLogsRetriever(config)
        if 'Records' in event and event['Records'][0]['s3']['object']['key'].split('/')[1] == 'AWSLogs':
            return LBAccessLogsRetriever(config)
        if 'Records' in event:
            return S3AccessLogsRetriever(config)
        if 'awslogs' in event:
            return CloudwatchDataRetriever(config)
        else:
            return None
