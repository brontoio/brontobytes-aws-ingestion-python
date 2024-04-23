import time

import boto3
import urllib.request
import gzip
import logging

logger = logging.getLogger()


class S3Client:

    def __init__(self, filepath):
        self.client = boto3.client('s3')
        self.filepath = filepath

    def download(self, bucket, object_key):
        with open(self.filepath, 'wb') as f:
            self.client.download_fileobj(bucket, object_key, f)


class Batch:

    def __init__(self):
        self.batch = []
        self.size = 0

    def add(self, line):
        self.batch.append(line)
        self.size += len(line)

    def get_batch_size(self) -> int:
        return self.size

    def get_data(self) -> list[str]:
        return self.batch


class BrontoClient:

    def __init__(self, api_key, ingestion_endpoint, log_name, log_set):
        self.api_key = api_key
        self.log_name = log_name
        self.log_set = log_set
        self.ingestion_endpoint = ingestion_endpoint
        self.headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'text/plain',
            'x-bronto-api-key': self.api_key,
            'x-bronto-log-name': self.log_name,
            'x-bronto-logset': self.log_set
        }

    def _send_batch(self, compressed_batch):
        request = urllib.request.Request(self.ingestion_endpoint, data=compressed_batch, headers=self.headers)
        attempt = 0
        max_attempts = 5
        with urllib.request.urlopen(request) as resp:
            if resp.status != 200 and attempt < max_attempts:
                attempt += 1
                delay_sec = attempt * 10
                logger.warning('Data sending failed. attempt=%s, max_attempts=%s, status=%s, reason=%s',
                               attempt, max_attempts, resp.status, resp.reason)
                time.sleep(delay_sec)
                self._send_batch(compressed_batch)

    def send_data(self, batch):
        data = '\n'.join(batch.get_data())
        compressed_data = gzip.compress(data.encode())
        logger.info('Batch compressed. batch_size=%s, compressed_batch_size=%s',batch.get_batch_size(),
                    len(compressed_data))
        self._send_batch(compressed_data)
