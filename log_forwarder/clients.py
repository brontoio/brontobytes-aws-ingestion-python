import boto3
import urllib.request


class S3Client:

    def __init__(self, filepath):
        self.client = boto3.client('s3')
        self.filepath = filepath

    def download(self, bucket, object_key):
        with open(self.filepath, 'wb') as f:
            self.client.download_fileobj(bucket, object_key, f)


class BrontoClient:

    def __init__(self, api_key, ingestion_endpoint, log_name, log_set):
        self.api_key = api_key
        self.log_name = log_name
        self.log_set = log_set
        self.ingestion_endpoint = ingestion_endpoint

    def send_data(self, filepath):
        headers = {
            'Content-Encoding': 'gzip',
            'x-bronto-api-key': self.api_key,
            'x-bronto-log-name': self.log_name,
            'x-bronto-logset': self.log_set
        }
        with open(filepath, 'r') as f:
            request = urllib.request.Request(self.ingestion_endpoint, data=f.buffer, headers=headers)
            response = urllib.request.urlopen(request)
            return response
