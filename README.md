## brontobytes-aws-ingestion-python

This component is designed to run as an AWS lambda function.

It consumes messages about AWS S3 objects containing log data, retrieves these objects and send their content to 
`bronto.io`.

### Configuration

This lambda function can be configured with the following attributes:

- `bronto_endpoint`: the BrontoBytes ingestion endpoint
- `bronto_api_key`: a BrontoBytes account API key
- `tags`: a string representing tags to be applied to all destination datasets. The string if of the form `key1=value1,key2=value2,...`, 
where keys and values should only contain alphanumerical character or `-` or `_`.
- `max_batch_size`: the maximum size of an uncompressed payload sent to BrontoBytes. This lambda function compresses 
the data with gzip. As a rule of thumb, a compression ratio of about 90% can be expected.
- `destination_config`: a base64 encoded map representing the configuration to where each log should be sent to.
- `paths_regex`: `paths_regex` is a base64-encoded list of objects, each containing a regular expression pattern with a 
named capture group called `dest_config_id`. This is used for log data delivered to S3 when the S3 object key does not 
follow standard AWS naming conventions, such as when data is moved between buckets and renamed. In such cases, the 
default rules for identifying the log type or the originating resource (like a load balancer name) may no longer apply. 
To handle such cases, the forwarder uses the regex patterns defined in the `paths_regex` field, after applying the 
default rules (except for S3 Access Logs). It extracts the `dest_config_id` value from the S3 key and looks it up in 
the `destination_config` map. If a match is found, the corresponding configuration is used. For example, with a decoded 
paths_regex of `[{'pattern': '[^/]*/(?P<dest_config_id>[^/]+)'}]` and an S3 key like 
`some_prefix/my_config_id/some_suffix`, the extracted `dest_config_id` would be `my_config_id`, and the matching 
configuration in `destination_config` below would be applied.

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
    },
    "my-clustom-data-in-log-group-name": {
      "dataset": "log-group-logs",
      "collection": "MyCustomCollection",
      "log_type": "cloudwatch_log",
      "client_type": "FluentD"
    },
    "my_config_id": {
      "dataset": "my_dataset",
      "collection": "MyCustomCollection",
      "log_type": "default"
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
- For logs delivered to Cloudwatch, `log_type` should be set to `cloudwatch_log`.
- For logs delivered to AWS S3, `log_type` is a mandatory field. `dataset` and `collection` are optional. Data will be
forwarded to default Collection and Dataset if `dataset` and `collection` are not set. The possible log_type values are:
  - `bedrock_s3` for Bedrock logs delivered to S3. Note that some Bedrock model invocation logs may be delivered to Cloudwatch too.
  - `vpc_flow_log`
  - `cloudtrail`
  - `s3_access_log`
  - `alb_access_log`
  - `nlb_access_log`
  - `clb_access_log`
  - `cf_realtime_access_log`
  - `cf_standard_access_log`
  - `default` for the case where no parsing is needed (e.g. structured logs in JSON format)
- `client_type` is optional and should only be set when forwarding log data that is already in a format that should not 
be altered by this integration, e.g. the data is already in the `FluentD` format. Currently, the only values supported 
are `FluentD` and `LogStash`. This information is passed on to the Bronto backend, for parsing purpose, to indicate 
the client that the data originated from (e.g. data was sent from a FluentD agent, via Cloudwatch).
- For Cloudwatch logs, entries in the configuration map are optional. If not set, the bronto destination 
(i.e. `dataset` and `collection`) is so that:  
  - the collection is defined with the `cloudwatch_default_collection` environment variable or the default Bronto 
  collection if not set.
  - the dataset is the Cloudwatch log group name (e.g. `/aws/lambda/<lambda_function_name>`).
- Legacy attributes `logname` and `logset` are being deprecated in favour of `dataset` and `collection`. However, they
are still accepted in configuration maps for now.

### Parsing - Supported Log Type

This function can parse keys and values of the following log types:
- S3 access logs (the lambda function must be deployed in the same account and region as the bucket whose access 
logs are being forwarded)
- Cloudtrail
- ALB
- VPC Flow Log (version 2 to 5)
- CloudFront Standard access logs

Data in JSON format or for which `client_type` is set, will be parsed by Bronto's backend. 
