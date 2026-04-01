import time
import random

import boto3
import urllib.request
import urllib.error
import gzip
import logging
from typing import Dict
import json

logger = logging.getLogger()


class BrontoClientError(Exception):
    """Base exception for BrontoClient errors"""
    pass


class BrontoClientNonRetryableError(BrontoClientError):
    """Exception raised for HTTP errors that should not be retried"""
    def __init__(self, status_code: int, reason: str):
        self.status_code = status_code
        self.reason = reason
        super().__init__(f'Non-retryable HTTP error {status_code}: {reason}')


class BrontoClientMaxAttemptsError(BrontoClientError):
    """Exception raised when maximum retry attempts are reached"""
    def __init__(self, attempts: int, last_status: int = None):
        self.attempts = attempts
        self.last_status = last_status
        super().__init__(f'Max retry attempts ({attempts}) reached')


class S3Client:

    def __init__(self, filepath):
        self.client = boto3.client('s3')
        self.filepath = filepath

    def download(self, bucket, object_key):
        with open(self.filepath, 'wb') as f:
            self.client.download_fileobj(bucket, object_key, f)


class Batch:

    def __init__(self, max_size: int, no_formatting=False):
        self.batch = []
        self.size = 0
        self.max_size = max_size
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

    def reset(self):
        self.batch = []
        self.size = 0


class BrontoClient:

    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404, 405, 410, 422}
    DEFAULT_MAX_ATTEMPTS = 5
    DEFAULT_BASE_DELAY_SEC = 2
    DEFAULT_TIMEOUT_SEC = 30

    def __init__(self, api_key, ingestion_endpoint, dataset, collection, client_type, tags: Dict[str, str],
                 max_attempts: int = None, base_delay_sec: int = None, timeout_sec: int = None):
        self.api_key = api_key
        self.dataset = dataset
        self.collection = collection
        self.client_type = client_type
        self.ingestion_endpoint = ingestion_endpoint
        self.max_attempts = max_attempts if max_attempts is not None else self.DEFAULT_MAX_ATTEMPTS
        self.base_delay_sec = base_delay_sec if base_delay_sec is not None else self.DEFAULT_BASE_DELAY_SEC
        self.timeout_sec = timeout_sec if timeout_sec is not None else self.DEFAULT_TIMEOUT_SEC
        self.formatted_tags = ','.join([f'{key}={value}' for key, value in tags.items()])
        self.headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/json',
            'User-Agent': 'bronto-aws-integration',
            'x-bronto-api-key': self.api_key,
            'x-bronto-tags': self.formatted_tags
        }
        if self.dataset is not None:
            self.headers.update({'x-bronto-service-name': self.dataset})
        if self.collection is not None:
            self.headers.update({'x-bronto-service-namespace': self.collection})
        if self.client_type is not None:
            self.headers.update({'x-bronto-client': self.client_type})

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay with jitter"""
        base_delay = self.base_delay_sec * (2 ** (attempt - 1))
        jitter = random.uniform(0, base_delay * 0.1)
        return base_delay + jitter

    def _send_batch(self, compressed_batch):
        request = urllib.request.Request(self.ingestion_endpoint, data=compressed_batch, headers=self.headers)
        last_status = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_sec) as resp:
                    last_status = resp.status
                    if resp.status == 200:
                        logger.info('data sent successfully. collection=%s, dataset=%s, tags=%s',
                                    self.collection, self.dataset, self.formatted_tags)
                        return
                    elif resp.status in self.NON_RETRYABLE_STATUS_CODES:
                        logger.error('Non-retryable error encountered. status=%s, reason=%s',
                                     resp.status, resp.reason)
                        raise BrontoClientNonRetryableError(resp.status, resp.reason)
                    else:
                        logger.warning('Data sending failed. attempt=%s, max_attempts=%s, status=%s, reason=%s',
                                       attempt, self.max_attempts, resp.status, resp.reason)
            except urllib.error.HTTPError as e:
                last_status = e.code
                if e.code in self.NON_RETRYABLE_STATUS_CODES:
                    logger.error('Non-retryable HTTP error. status=%s, reason=%s', e.code, e.reason)
                    raise BrontoClientNonRetryableError(e.code, str(e.reason))
                logger.warning('HTTP error occurred. attempt=%s, max_attempts=%s, status=%s, reason=%s',
                               attempt, self.max_attempts, e.code, e.reason)
            except urllib.error.URLError as e:
                logger.warning('URL error occurred. attempt=%s, max_attempts=%s, reason=%s',
                               attempt, self.max_attempts, e.reason)
            except Exception as e:
                logger.warning('Unexpected error occurred. attempt=%s, max_attempts=%s, error=%s',
                               attempt, self.max_attempts, str(e))

            # If not the last attempt, wait before retrying
            if attempt < self.max_attempts:
                delay_sec = self._calculate_delay(attempt)
                logger.info('Retrying in %.2f seconds...', delay_sec)
                time.sleep(delay_sec)

        # All attempts exhausted
        logger.error('Max attempts reached. attempts=%s', self.max_attempts)
        raise BrontoClientMaxAttemptsError(self.max_attempts, last_status)

    def send_data(self, batch, attributes=None):
        data = batch.get_formatted_data({} if attributes is None else attributes)
        compressed_data = gzip.compress(data.encode())
        logger.info('Batch compressed. batch_size=%s, compressed_batch_size=%s',batch.get_batch_size(),
                    len(compressed_data))
        self._send_batch(compressed_data)
