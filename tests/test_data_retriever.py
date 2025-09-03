import base64
import gzip
import json
import tempfile

from config import Config
from data_retriever import CloudwatchDataRetriever, LBAccessLogsRetriever, S3AccessLogsRetriever


def test_cloudwatch():
    # from https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html#LambdaFunctionExample
    group_name = 'my_log_group'
    log_stream = 'my_log_stream'
    entries = {'logGroup': group_name, 'logStream': log_stream, 'logEvents': [{'message': 'entry1'}, {'message': 'entry2'}]}
    event = {'awslogs': {'data': base64.b64encode(gzip.compress(json.dumps(entries).encode())).decode()}}
    with tempfile.NamedTemporaryFile() as f:
        config = Config(event, f.name)
        retriever = CloudwatchDataRetriever(config)
        retriever.get_data()
        assert (open(config.filepath, 'rb').read().decode().split('\n') ==
                [entry['message'] for entry in entries['logEvents']])
        assert retriever.get_data_id() == group_name
        assert retriever.log_group_name == group_name
        assert retriever.log_stream == log_stream

def test_lb_access_logs_retriever():
    lb_id = 'load-balancer-id'
    # from https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-file-format
    bucket_name = 'my_bucket'
    s3_key = f'bucket/prefix/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_app.{lb_id}_end-time_ip-address_random-string.log.gz'
    event = {'Records': [{'s3': {
        'bucket': {'name': bucket_name},
        'object': {'key': s3_key}
    }}]}
    with tempfile.NamedTemporaryFile() as f:
        config = Config(event, f.name)
        retriever = LBAccessLogsRetriever(config, bucket_name, s3_key)
        assert retriever.get_data_id() == lb_id


def test_s3_access_logs_retriever():
    src_bucket = 'my-bucket'
    s3_key = f'prefix/{src_bucket}/2024-10-25-13-08-56-some_string-string.log'
    # from https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html for same account bucket
    event = {'Records': [{'s3': {
        'bucket': {'name': src_bucket},
        'object': {'key': s3_key}
    }}]}
    with tempfile.NamedTemporaryFile() as f:
        config = Config(event, f.name)
        retriever = S3AccessLogsRetriever(config, src_bucket, s3_key)
        assert retriever.get_data_id() == src_bucket

