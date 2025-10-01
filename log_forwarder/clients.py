import time

import boto3
import urllib.request
import gzip
import logging
from typing import Dict
import json

logger = logging.getLogger()


class S3Client:

    def __init__(self, filepath):
        self.client = boto3.client('s3')
        self.filepath = filepath

    def download(self, bucket, object_key):
        with open(self.filepath, 'wb') as f:
            self.client.download_fileobj(bucket, object_key, f)


class Batch:

    def __init__(self, no_formatting=False):
        self.batch = []
        self.size = 0
        self.no_formatting = no_formatting

    def add(self, line):
        self.batch.append(line)
        self.size += len(line)

    def get_batch_size(self) -> int:
        return self.size

    def get_data(self) -> list[str]:
        return self.batch

    def get_formatted_data(self, attributes: Dict[str, str]):
        if self.no_formatting:
            return '\n'.join([line for line in self.batch])
        log_messages = [{'log': line} for line in self.batch]
        for log_message in log_messages:
            log_message.update(attributes)
        return '\n'.join([json.dumps(log_message) for log_message in log_messages])


class BrontoClient:

    def __init__(self, api_key, ingestion_endpoint, dataset, collection, client_type, tags: Dict[str, str]):
        self.api_key = api_key
        self.dataset = dataset
        self.collection = collection
        self.client_type = client_type
        self.ingestion_endpoint = ingestion_endpoint
        self.headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/json',
            'User-Agent': 'bronto-aws-integration',
            'x-bronto-api-key': self.api_key,
            'x-bronto-tags': ','.join([f'{key}={value}' for key, value in tags.items()])
        }
        if self.dataset is not None:
            self.headers.update({'x-bronto-service-name': self.dataset})
        if self.collection is not None:
            self.headers.update({'x-bronto-service-namespace': self.collection})
        if self.client_type is not None:
            self.headers.update({'x-bronto-client': self.client_type})

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
            elif resp.status == 200:
                logger.info('data sent successfully. collection=%s, dataset=%s', self.collection,
                            self.dataset)
            else:
                logger.error('max attempts reached. attempt=%s, max_attempts=%s', attempt, max_attempts)
                raise Exception('BrontoClientMaxAttemptReached')

    def send_data(self, batch, attributes=None):
        data = batch.get_formatted_data({} if attributes is None else attributes)
        compressed_data = gzip.compress(data.encode())
        logger.info('Batch compressed. batch_size=%s, compressed_batch_size=%s',batch.get_batch_size(),
                    len(compressed_data))
        self._send_batch(compressed_data)
