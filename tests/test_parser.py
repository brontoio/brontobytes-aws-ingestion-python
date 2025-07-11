import json
from parser import ParserFactory

from config import (CLOUDFRONT_REALTIME_ACCESS_LOG_TYPE, ALB_ACCESS_LOG_TYPE, NLB_ACCESS_LOG_TYPE,
                    CLOUDFRONT_STANDARD_ACCESS_LOG_TYPE, CLASSIC_LB_ACCESS_LOG_TYPE, CLOUDTRAIL_LOG_TYPE,
                    S3_ACCESS_LOG_TYPE)

# https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-examples
ALB_ACCESS_LOG_SAMPLE = 'https 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.086 0.048 0.037 200 200 0 57 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337281-1d84f3d73c47ec4e58577259" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 1 2018-07-02T22:22:48.364000Z "authenticate,forward" "-" "-" "10.0.0.1:80" "200" "-" "-" TID_1234567890'

# https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-access-logs.html
NLB_ACCESS_LOG_SAMPLE = 'tls 2.0 2020-04-01T08:51:42 net/my-network-loadbalancer/c6e77e28c25b2234 g3d4b5e8bb8464cd 72.21.218.154:51341 172.100.100.185:443 5 2 98 246 - arn:aws:acm:us-east-2:671290407336:certificate/2a108f19-aded-46b0-8493-c63eb1ef4a99 - ECDHE-RSA-AES128-SHA tlsv12 - my-network-loadbalancer-c6e77e28c25b2234.elb.us-east-2.amazonaws.com h2 h2 "h2","http/1.1" 2020-04-01T08:51:20'

# https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
CLB_ACCESS_LOG_SAMPLE = '2015-05-13T23:39:43.945958Z my-loadbalancer 192.168.131.39:2817 10.0.0.1:80 0.000086 0.001048 0.001337 200 200 0 57 "GET https://www.example.com:443/ HTTP/1.1" "curl/7.38.0" DHE-RSA-AES128-SHA TLSv1.2'

# https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/real-time-logs.html
CF_REALTIME_ACCESS_LOG_SAMPLE = '''{
    "timestamp": 1602646738.145,
    "c-ip": "1.2.3.4",
    "time-to-first-byte": 0.002,
    "sc-status": 200,
    "sc-bytes": 16653,
    "cs-method": "GET",
    "cs-protocol": "https",
    "cs-host": "somehost123.cloudfront.net",
    "cs-uri-stem": "/image.jpg",
    "cs-bytes": 59,
    "x-edge-location": "IAD66-C1",
    "x-edge-request-id": "boNb1al7B50G5T7jXDOGi2zlYAF2VWrba2fnZWfucsomething12345_UA==",
    "x-host-header": "somehost123.cloudfront.net",
    "time-taken": 0.002,
    "cs-protocol-version": "HTTP/2.0",
    "c-ip-version": "IPv4",
    "cs-user-agent": "curl/7.53.1",
    "cs-referer": "-",
    "cs-cookie": "-",
    "cs-uri-query": "-",
    "x-edge-response-result-type": "Hit",
    "x-forwarded-for": "-",
    "ssl-protocol": "TLSv1.2",
    "ssl-cipher": "ECDHE-RSA-AES128-GCM-SHA256",
    "x-edge-result-type": "Hit",
    "fle-encrypted-fields": "-",
    "fle-status": "-",
    "sc-content-type": "image/jpeg",
    "sc-content-len": 16335,
    "sc-range-start": "-",
    "sc-range-end": "-",
    "c-port": 36242,
    "x-edge-detailed-result-type": "Hit",
    "c-country": "US",
    "cs-accept-encoding": "-",
    "cs-accept": "*/*",
    "cache-behavior-path-pattern": "*",
    "cs-headers": [
        {
            "Name": "host",
            "Value": "somehost123.cloudfront.net"
        },
        {
            "Name": "user-agent",
            "Value": "curl/7.53.1"
        },
        {
            "Name": "accept",
            "Value": "*/*"
        },
        {
            "Name": "CloudFront-Is-Mobile-Viewer",
            "Value": "false"
        },
        {
            "Name": "CloudFront-Is-Tablet-Viewer",
            "Value": "false"
        },
        {
            "Name": "CloudFront-Is-SmartTV-Viewer",
            "Value": "false"
        },
        {
            "Name": "CloudFront-Is-Desktop-Viewer",
            "Value": "true"
        },
        {
            "Name": "CloudFront-Viewer-Country",
            "Value": "US"
        }
    ],
    "cs-header-names": [
        "host",
        "user-agent",
        "accept",
        "CloudFront-Is-Mobile-Viewer",
        "CloudFront-Is-Tablet-Viewer",
        "CloudFront-Is-SmartTV-Viewer",
        "CloudFront-Is-Desktop-Viewer",
        "CloudFront-Viewer-Country"
    ],
    "cs-headers-count": 8,
    "timestamp_utc": "2020-10-14T03:38:58"
}'''.replace('\n', '')

# https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html
CF_STANDARD_ACCESS_LOG_SAMPLE = '2019-12-04	21:02:31	LAX1	392	192.0.2.100	GET	d111111abcdef8.cloudfront.net	/index.html	200	-	Mozilla/5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/78.0.3904.108%20Safari/537.36	-	-	Hit	SOX4xwn4XV6Q4rgb7XiVGOHms_BGlTAC4KyHmureZmBNrjGdRLiNIQ==	d111111abcdef8.cloudfront.net	https	23	0.001	-	TLSv1.2	ECDHE-RSA-AES128-GCM-SHA256	Hit	HTTP/2.0	-	-	11040	0.001	Hit	text/html	78	-	-'

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
S3_ACCESS_LOG_SAMPLE = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be DOC-EXAMPLE-BUCKET1 [06/Feb/2019:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - "GET /DOC-EXAMPLE-BUCKET1?versioning HTTP/1.1" 200 - 113 - 7 - "-" "S3Console/0.4" - s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader DOC-EXAMPLE-BUCKET1.s3.us-west-1.amazonaws.com TLSV1.2 arn:aws:s3:us-west-1:123456789012:accesspoint/example-AP Yes'

# https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html
CLOUDTRAIL_LOG_SAMPLE = '''{"Records": [{
    "eventVersion": "1.08",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "EXAMPLE6E4XEGITWATV6R",
        "arn": "arn:aws:iam::123456789012:user/Mateo",
        "accountId": "123456789012",
        "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
        "userName": "Mateo",
        "sessionContext": {
            "sessionIssuer": {},
            "webIdFederationData": {},
            "attributes": {
                "creationDate": "2023-07-19T21:11:57Z",
                "mfaAuthenticated": "false"
            }
        }
    },
    "eventTime": "2023-07-19T21:17:28Z",
    "eventSource": "ec2.amazonaws.com",
    "eventName": "StartInstances",
    "awsRegion": "us-east-1",
    "sourceIPAddress": "192.0.2.0",
    "userAgent": "aws-cli/2.13.5 Python/3.11.4 Linux/4.14.255-314-253.539.amzn2.x86_64 exec-env/CloudShell exe/x86_64.amzn.2 prompt/off command/ec2.start-instances",
    "requestParameters": {
        "instancesSet": {
            "items": [
                {
                    "instanceId": "i-EXAMPLE56126103cb"
                },
                {
                    "instanceId": "i-EXAMPLEaff4840c22"
                }
            ]
        }
    },
    "responseElements": {
        "requestId": "e4336db0-149f-4a6b-844d-EXAMPLEb9d16",
        "instancesSet": {
            "items": [
                {
                    "instanceId": "i-EXAMPLEaff4840c22",
                    "currentState": {
                        "code": 0,
                        "name": "pending"
                    },
                    "previousState": {
                        "code": 80,
                        "name": "stopped"
                    }
                },
                {
                    "instanceId": "i-EXAMPLE56126103cb",
                    "currentState": {
                        "code": 0,
                        "name": "pending"
                    },
                    "previousState": {
                        "code": 80,
                        "name": "stopped"
                    }
                }
            ]
        }
    },
    "requestID": "e4336db0-149f-4a6b-844d-EXAMPLEb9d16",
    "eventID": "e755e09c-42f9-4c5c-9064-EXAMPLE228c7",
    "readOnly": false,
    "eventType": "AwsApiCall",
    "managementEvent": true,
    "recipientAccountId": "123456789012",
    "eventCategory": "Management",
     "tlsDetails": {
        "tlsVersion": "TLSv1.2",
        "cipherSuite": "ECDHE-RSA-AES128-GCM-SHA256",
        "clientProvidedHostHeader": "ec2.us-east-1.amazonaws.com"
    },
    "sessionCredentialFromConsole": "true"
}]}'''.replace('\n', '')


def test_alb_access_log_parser():
    parser = ParserFactory.get_parser(ALB_ACCESS_LOG_TYPE, None)
    parsed = parser.parse(ALB_ACCESS_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['useragent'] == 'curl/7.46.0'
    assert data['conn_trace_id'] == 'TID_1234567890'


def test_nlb_access_log_parser():
    parser = ParserFactory.get_parser(NLB_ACCESS_LOG_TYPE, None)
    parsed = parser.parse(NLB_ACCESS_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['chosen_cert_arn'] == 'arn:aws:acm:us-east-2:671290407336:certificate/2a108f19-aded-46b0-8493-c63eb1ef4a99'


def test_clb_access_log_parser():
    parser = ParserFactory.get_parser(CLASSIC_LB_ACCESS_LOG_TYPE, None)
    parsed = parser.parse(CLB_ACCESS_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['ssl_protocol'] == 'TLSv1.2'


def test_cf_standard_access_log_parser():
    parser = ParserFactory.get_parser(CLOUDFRONT_STANDARD_ACCESS_LOG_TYPE, None)
    parsed = parser.parse(CF_STANDARD_ACCESS_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['sc_content_type'] == 'text/html'


def test_cf_realtime_access_log_parser():
    parser = ParserFactory.get_parser(CLOUDFRONT_REALTIME_ACCESS_LOG_TYPE, None)
    parsed = parser.parse(CF_REALTIME_ACCESS_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['sc-content-type'] == 'image/jpeg'


def test_s3_access_log_parser():
    parser = ParserFactory.get_parser(S3_ACCESS_LOG_TYPE, None)
    parsed = parser.parse(S3_ACCESS_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['UserAgent'] == 'S3Console/0.4'


def test_cloudtrail_log_parser():
    parser = ParserFactory.get_parser(CLOUDTRAIL_LOG_TYPE, None)
    parsed = parser.parse(CLOUDTRAIL_LOG_SAMPLE)
    data = json.loads(parsed)
    assert data['Records'][0]['eventName'] == 'StartInstances'
