## brontobytes-aws-ingestion-python

This component is designed to run as an AWS lambda function.

It consumes messages about AWS S3 objects containing log data, retrieves these objects and send their content to 
`brontobytes.io`.

### Configuration

This lambda function can be configured with the following attributes:

- `bronto_endpoint`: the BrontoBytes ingestion endpoint
- `bronto_api_key`: a BrontoBytes account API key
- `max_batch_size`: the maximum size of an uncompressed payload sent to BrontoBytes. This lambda function compresses 
the data with gzip. As a rule of thumb, a compression ratio of about 90% can be expected.
- `destination_config`: a base64 encoded map representing the configuration to where each log should be sent to. 

A sample configuration is:
```json
{
    "my-s3-bucket-name": {
      "logname": "my-bucket-access-logs",
      "logset": "s3AccessLogs",
      "log_type": "s3_access_log"
    },
    "my-load-balancer-name": {
      "logname": "my-load-balancer-access-logs",
      "logset": "lbAccessLogs",
      "log_type": "alb_access_log"
    },
    "my-lambda-function-log-group-name": {
      "logname": "log-group-logs",
      "logset": "lambdaLogs",
      "log_type": "cloudwatch_log"
    }
  }
```
The key of the maps are S3 bucket names for S3 access logs, 
load balancers names for load balancers access logs and log group names for cloudwatch logs. In general, 
- For Cloudwatch logs, the keys in the destination configuration map are matched against a log group name.
- For logs delivered to AWS S3, the keys in the destination configuration map are matched against the AWS 
resource name (e.g. S3 bucket or load balancer names on which access logs is enabled).
