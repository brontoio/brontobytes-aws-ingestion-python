## brontobytes-aws-ingestion-python

This component is designed to run as an AWS lambda function.

It consumes messages about AWS S3 objects containing log data, retrieves these objects and send their content to 
`bronto.io`.

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
      "dataset": "my-bucket-access-logs",
      "collection": "s3AccessLogs",
      "log_type": "s3_access_log"
    },
    "my-load-balancer-name": {
      "dataset": "my-load-balancer-access-logs",
      "collection": "lbAccessLogs",
      "log_type": "alb_access_log"
    },
    "my-lambda-function-log-group-name": {
      "dataset": "log-group-logs",
      "collection": "lambdaLogs",
      "log_type": "cloudwatch_log"
    }
  }
```
The key of the maps are S3 bucket names for S3 access logs, 
load balancers names for load balancers access logs and log group names for cloudwatch logs. In general, 
- For Cloudwatch logs, the keys in the destination configuration map are matched against log group names.
- For logs delivered to AWS S3, the keys in the destination configuration map are matched against AWS 
resource names (e.g. S3 bucket name, load balancer name, or Cloudfront distribution ID on which access logs are enabled).

Notes:

- For logs delivered to AWS S3, only data matching an entry in the configuration is forwarded.
- For logs delivered to AWS S3, `log_type` is a mandatory field. `dataset` and `collection` are optional. Data will be 
forwarded to default Collection and Dataset if `dataset` and `collection` are not set.
- For Cloudwatch logs, entries in the configuration map are optional. If not set, the bronto destination 
(i.e. `dataset` and `collection`) is so that:  
  - the collection is defined with the `cloudwatch_default_collection` environment variable or the default Bronto 
  collection if not set.
  - the dataset is the Cloudwatch log group name (e.g. `/aws/lambda/<lambda_function_name>`).
- Legacy attributes `logname` and `logset` are being deprecated in favour of `dataset` and `collection`. However, they
are still accepted in configuration maps for now.

### Supported Log Type

This function can parse keys and values of the following log types:
- S3 access logs (the lambda function must be deployed in the same account and region as the bucket whose access 
logs are being forwarded)
- Cloudtrail
- ALB
- VPC Flow Log (version 2 to 5)
- CloudFront Standard access logs
