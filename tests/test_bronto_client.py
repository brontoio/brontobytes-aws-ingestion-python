import unittest
from unittest.mock import Mock, patch, MagicMock
import urllib.error
import gzip

from clients import (
    BrontoClient,
    BrontoClientError,
    BrontoClientNonRetryableError,
    BrontoClientMaxAttemptsError,
    Batch
)


class TestBrontoClient(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures"""
        self.api_key = "test-api-key"
        self.ingestion_endpoint = "https://test.bronto.com/ingest"
        self.dataset = "test-dataset"
        self.collection = "test-collection"
        self.client_type = "test-client"
        self.tags = {"env": "test", "version": "1.0"}

        self.client = BrontoClient(
            api_key=self.api_key,
            ingestion_endpoint=self.ingestion_endpoint,
            dataset=self.dataset,
            collection=self.collection,
            client_type=self.client_type,
            tags=self.tags
        )

    def test_client_initialization(self):
        """Test that client initializes with correct default values"""
        self.assertEqual(self.client.api_key, self.api_key)
        self.assertEqual(self.client.ingestion_endpoint, self.ingestion_endpoint)
        self.assertEqual(self.client.dataset, self.dataset)
        self.assertEqual(self.client.collection, self.collection)
        self.assertEqual(self.client.client_type, self.client_type)
        self.assertEqual(self.client.max_attempts, BrontoClient.DEFAULT_MAX_ATTEMPTS)
        self.assertEqual(self.client.base_delay_sec, BrontoClient.DEFAULT_BASE_DELAY_SEC)
        self.assertEqual(self.client.timeout_sec, BrontoClient.DEFAULT_TIMEOUT_SEC)

    def test_client_initialization_with_custom_params(self):
        """Test that client can be initialized with custom retry parameters"""
        custom_client = BrontoClient(
            api_key=self.api_key,
            ingestion_endpoint=self.ingestion_endpoint,
            dataset=self.dataset,
            collection=self.collection,
            client_type=self.client_type,
            tags=self.tags,
            max_attempts=3,
            base_delay_sec=5,
            timeout_sec=60
        )
        self.assertEqual(custom_client.max_attempts, 3)
        self.assertEqual(custom_client.base_delay_sec, 5)
        self.assertEqual(custom_client.timeout_sec, 60)

    def test_headers_are_set_correctly(self):
        """Test that HTTP headers are constructed correctly"""
        self.assertEqual(self.client.headers['x-bronto-api-key'], self.api_key)
        self.assertEqual(self.client.headers['x-bronto-service-name'], self.dataset)
        self.assertEqual(self.client.headers['x-bronto-service-namespace'], self.collection)
        self.assertEqual(self.client.headers['x-bronto-client'], self.client_type)
        self.assertIn('env=test', self.client.headers['x-bronto-tags'])
        self.assertIn('version=1.0', self.client.headers['x-bronto-tags'])

    def test_calculate_delay_exponential_backoff(self):
        """Test that delay calculation uses exponential backoff"""
        # First attempt: base_delay * 2^0 = 2 * 1 = 2
        delay1 = self.client._calculate_delay(1)
        self.assertGreaterEqual(delay1, 2.0)
        self.assertLess(delay1, 3.0)  # 10% jitter

        # Second attempt: base_delay * 2^1 = 2 * 2 = 4
        delay2 = self.client._calculate_delay(2)
        self.assertGreaterEqual(delay2, 4.0)
        self.assertLess(delay2, 5.0)  # 10% jitter

        # Third attempt: base_delay * 2^2 = 2 * 4 = 8
        delay3 = self.client._calculate_delay(3)
        self.assertGreaterEqual(delay3, 8.0)
        self.assertLess(delay3, 9.0)  # 10% jitter

    @patch('clients.urllib.request.urlopen')
    def test_send_batch_success(self, mock_urlopen):
        """Test successful batch sending"""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.reason = 'OK'
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        batch = Batch(1000)
        batch.add("test log line")

        # Should not raise any exception
        self.client.send_data(batch)

        # Verify urlopen was called once
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch('clients.urllib.request.urlopen')
    def test_send_batch_non_retryable_403(self, mock_urlopen):
        """Test that 403 errors are not retried"""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=self.ingestion_endpoint,
            code=403,
            msg='Forbidden',
            hdrs={},
            fp=None
        )

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientNonRetryableError) as context:
            self.client.send_data(batch)

        self.assertEqual(context.exception.status_code, 403)
        # Should only try once, not retry
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch('clients.time.sleep')
    @patch('clients.urllib.request.urlopen')
    def test_send_batch_non_retryable_401(self, mock_urlopen, mock_sleep):
        """Test that 401 errors are not retried"""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=self.ingestion_endpoint,
            code=401,
            msg='Unauthorized',
            hdrs={},
            fp=None
        )

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientNonRetryableError) as context:
            self.client.send_data(batch)

        self.assertEqual(context.exception.status_code, 401)
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch('clients.urllib.request.urlopen')
    def test_send_batch_http_error_non_retryable(self, mock_urlopen):
        """Test that HTTPError with non-retryable status codes are not retried"""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=self.ingestion_endpoint,
            code=404,
            msg='Not Found',
            hdrs={},
            fp=None
        )

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientNonRetryableError) as context:
            self.client.send_data(batch)

        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch('clients.time.sleep')
    @patch('clients.urllib.request.urlopen')
    def test_send_batch_retries_on_500(self, mock_urlopen, mock_sleep):
        """Test that 500 errors are retried"""
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.reason = 'Internal Server Error'
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientMaxAttemptsError) as context:
            self.client.send_data(batch)

        # Should try max_attempts times (default 5)
        self.assertEqual(mock_urlopen.call_count, 5)
        # Should sleep 4 times (between attempts)
        self.assertEqual(mock_sleep.call_count, 4)
        self.assertEqual(context.exception.attempts, 5)
        self.assertEqual(context.exception.last_status, 500)

    @patch('clients.time.sleep')
    @patch('clients.urllib.request.urlopen')
    def test_send_batch_retries_then_succeeds(self, mock_urlopen, mock_sleep):
        """Test that retries eventually succeed"""
        # First 2 calls fail with 503, third succeeds
        mock_response_fail = MagicMock()
        mock_response_fail.status = 503
        mock_response_fail.reason = 'Service Unavailable'
        mock_response_fail.__enter__ = Mock(return_value=mock_response_fail)
        mock_response_fail.__exit__ = Mock(return_value=False)

        mock_response_success = MagicMock()
        mock_response_success.status = 200
        mock_response_success.reason = 'OK'
        mock_response_success.__enter__ = Mock(return_value=mock_response_success)
        mock_response_success.__exit__ = Mock(return_value=False)

        mock_urlopen.side_effect = [mock_response_fail, mock_response_fail, mock_response_success]

        batch = Batch(1000)
        batch.add("test log line")

        # Should succeed without raising exception
        self.client.send_data(batch)

        # Should have tried 3 times
        self.assertEqual(mock_urlopen.call_count, 3)
        # Should have slept 2 times (between attempts)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('clients.time.sleep')
    @patch('clients.urllib.request.urlopen')
    def test_send_batch_url_error_retries(self, mock_urlopen, mock_sleep):
        """Test that URLError causes retries"""
        mock_urlopen.side_effect = urllib.error.URLError('Connection refused')

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientMaxAttemptsError):
            self.client.send_data(batch)

        # Should try max_attempts times
        self.assertEqual(mock_urlopen.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 4)

    @patch('clients.time.sleep')
    @patch('clients.urllib.request.urlopen')
    def test_send_batch_timeout_retries(self, mock_urlopen, mock_sleep):
        """Test that timeout errors cause retries"""
        import socket
        mock_urlopen.side_effect = socket.timeout('Connection timed out')

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientMaxAttemptsError):
            self.client.send_data(batch)

        # Should try max_attempts times
        self.assertEqual(mock_urlopen.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 4)

    @patch('clients.urllib.request.urlopen')
    def test_send_batch_with_custom_max_attempts(self, mock_urlopen):
        """Test that custom max_attempts is respected"""
        custom_client = BrontoClient(
            api_key=self.api_key,
            ingestion_endpoint=self.ingestion_endpoint,
            dataset=self.dataset,
            collection=self.collection,
            client_type=self.client_type,
            tags=self.tags,
            max_attempts=2
        )

        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.reason = 'Internal Server Error'
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        batch = Batch(1000)
        batch.add("test log line")

        with self.assertRaises(BrontoClientMaxAttemptsError) as context:
            custom_client.send_data(batch)

        # Should only try 2 times
        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual(context.exception.attempts, 2)

    @patch('clients.urllib.request.urlopen')
    def test_send_data_compresses_batch(self, mock_urlopen):
        """Test that send_data properly compresses the batch data"""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.reason = 'OK'
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        batch = Batch(1000)
        test_data = "test log line"
        batch.add(test_data)

        self.client.send_data(batch)

        # Verify the request was made
        call_args = mock_urlopen.call_args
        request = call_args[0][0]

        # Verify data is compressed
        compressed_data = request.data
        decompressed = gzip.decompress(compressed_data).decode()
        self.assertIn(test_data, decompressed)

    def test_non_retryable_status_codes(self):
        """Test that NON_RETRYABLE_STATUS_CODES contains expected codes"""
        expected_codes = {400, 401, 403, 404, 405, 410, 422}
        self.assertEqual(BrontoClient.NON_RETRYABLE_STATUS_CODES, expected_codes)


if __name__ == '__main__':
    unittest.main()