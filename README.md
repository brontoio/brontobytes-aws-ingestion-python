## brontobytes-aws-ingestion-python

This component is designed to run as an AWS lambda function.

It consumes messages from an SQS queue about AWS S3 objects containing 
log data. This component retrieves the objects and send the data it contains to 
`brontobytes.io`.

### Configuration

...
