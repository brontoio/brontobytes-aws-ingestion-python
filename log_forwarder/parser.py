import re
import json
from config import (CLOUDFRONT_REALTIME_ACCESS_LOG_TYPE, ALB_ACCESS_LOG_TYPE, NLB_ACCESS_LOG_TYPE,
                    CLOUDFRONT_STANDARD_ACCESS_LOG_TYPE, CLASSIC_LB_ACCESS_LOG_TYPE, CLOUDTRAIL_LOG_TYPE,
                    CLOUDWATCH_LOG_TYPE, VPC_FLOW_LOG_TYPE, S3_ACCESS_LOG_TYPE, BEDROCK_S3_LOG_TYPE)
# Regex used in this file mostly come from:
# https://github.com/aws-samples/siem-on-amazon-opensearch-service/blob/v2.10.2/source/lambda/es_loader/aws.ini
#


class Parser:

    def __init__(self, regex, input_file):
        self.regex = regex
        self.pattern = re.compile(self.regex) if self.regex is not None else None
        self.input_file = input_file

    def parse(self, line: str):
        stripped_line = line.strip()
        if self.regex is None:
            return stripped_line
        parsed = re.match(self.pattern, stripped_line)
        if parsed is not None:
            return json.dumps({k: v for k, v in parsed.groupdict().items() if v is not None})
        return stripped_line

    def get_parsed_lines(self):
        for line in self.input_file.get_lines():
            yield self.parse(line)


class S3AccessLogParser(Parser):

    REGEX = r'(?P<BucketOwner>[^ ]*) (?P<Bucket>[^ ]*) \[(?P<RequestDateTime>.*?)\] (?P<RemoteIP>[^ ]*) (?P<Requester>[^ ]*) (?P<RequestID>[^ ]*) (?P<Operation>[^ ]*) (?P<Key>[^ ]*) (\"(?P<RequestURI_operation>[^ ]*) (?P<RequestURI_key>[^ ]*) (?P<RequestURI_httpProtoversion>- |[^ ]*)\"|\"-\"|-) (?P<HTTPstatus>-|[0-9]*) (?P<ErrorCode>[^ ]*) (?P<BytesSent>[^ ]*) (?P<ObjectSize>[^ ]*) (?P<TotalTime>[^ ]*) (?P<TurnAroundTime>[^ ]*) (\"(?P<Referrer>.*?)\"|-) (\"(?P<UserAgent>.*?)\"|-) (?P<VersionId>[^ ]*)(?: (?P<HostId>[^ ]*) (?P<SigV>[^ ]*) (?P<CipherSuite>[^ ]*) (?P<AuthType>[^ ]*) (?P<EndPoint>[^ ]*) (?P<TLSVersion>[^ ]*))?( (?P<S3AccessPointARN>arn:[^ ]*|-))?'

    def __init__(self, input_file):
        super().__init__(S3AccessLogParser.REGEX, input_file)


class ALBAccessLogParser(Parser):

    REGEX   = r'(?P<request_type>[^\s]*) (?P<timestamp>[^\s]*) (?P<elb>[^\s]*) (?P<client_ip>[^\s]*):(?P<client_port>[0-9]*) (?P<target_ip>[^\s]*)[:-](?P<target_port>[0-9]*) (?P<request_processing_time>[-.0-9]*) (?P<target_processing_time>[-.0-9]*) (?P<response_processing_time>[-.0-9]*) (?P<elb_status_code>|[-0-9]*) (?P<target_status_code>-|[-0-9]*) (?P<received_bytes>[-0-9]*) (?P<sent_bytes>[-0-9]*) \"(?P<request>(-|(?P<http_method>\w+)) (-|(?P<http_protocol>\w*)://\[?(?P<http_host>[^/]+?)\]?:(?P<http_port>\d+)(-|(?P<http_path>/[^?]*?))(\?(?P<http_query>.*?))?) (-?|\w+/(?P<http_version>[0-9\.]*)))\" \"(|(?P<useragent>[^\"]+))\" (?P<ssl_cipher>[^\s]+) (?P<ssl_protocol>[^\s]*) (?P<target_group_arn>[^\s]*) \"(?P<trace_id>[^\"]*)\" \"(?P<domain_name>[^\"]*)\" \"(?P<chosen_cert_arn>[^\"]*)\" (?P<matched_rule_priority>[-.0-9]*) (?P<request_creation_time>[^\s]*) \"(?P<actions_executed>[^\"]*)\" \"(?P<redirect_url>[^\"]*)\" \"(?P<lambda_error_reason>[^\s]*)\" \"(?P<target_port_list>[^\s]+)\" \"(?P<target_status_code_list>[^\s]+)\"( \"(?P<classification>[^\s]+)\" \"(?P<classification_reason>[^\s]+)\")? ?(?P<conn_trace_id>[^\s]*)?'

    def __init__(self, input_file):
        super().__init__(ALBAccessLogParser.REGEX, input_file)


class NLBAccessLogParser(Parser):

    REGEX = r'(?P<listener_type>[^ ]+) (?P<log_entry_version>[^ ]+) (?P<timestamp>[^ ]+) (?P<elb>[^ ]+) (?P<listener>[^ ]+) (?P<client_ip>[0-9a-f.:]+):(?P<client_port>[0-9]+) (?P<destination_ip>[^ ]+):(?P<destination_port>[0-9]+) (?P<connection_time>[0-9]+) (-|(?P<tls_handshake_time>[0-9]+)) (-|(?P<received_bytes>[-0-9]+)) (?P<sent_bytes>[-0-9]+) (-|(?P<incoming_tls_alert>[^ ]+)) (-|(?P<chosen_cert_arn>[^ ]+)) (-|(?P<chosen_cert_serial>[^ ]+)) (-|(?P<tls_cipher>[^ ]+)) (-|(?P<tls_protocol_version>[^ ]+)) (-|(?P<tls_named_group>[^ ]+)) (-|(?P<domain_name>[^ ]+)) (-|(?P<alpn_fe_protocol>[^ ]+)) (-|(?P<alpn_be_protocol>[^ ]+)) (-|(?P<alpn_client_preference_list>[^ ]+))( (?P<tls_connection_creation_time>20[0-9T:-]+))?'

    def __init__(self, input_file):
        super().__init__(NLBAccessLogParser.REGEX, input_file)


class CLBAccessLogParser(Parser):

    REGEX = r'(?P<timestamp>[^ ]+) (?P<elb>[^ ]+) (?P<client_ip>[0-9a-f.:]+):(?P<client_port>[0-9]+) (-|(?P<backend_ip>[0-9a-f.:]+):(?P<backend_port>[-0-9]+)) (?P<request_processing_time>[0-9\.-]+) (?P<backend_processing_time>[0-9\.-]+) (?P<response_processing_time>[0-9\.-]+) (?P<elb_status_code>[0-9\.-]+) (?P<backend_status_code>[0-9\.-]+) (?P<received_bytes>[0-9\.-]+) (?P<sent_bytes>[0-9\.-]+) \"(?P<request>(-|(?P<http_method>[\w-]+)) (-|(?P<http_protocol>\w*)://\[?(?P<http_host>[^\[\]]+?)\]?:(?P<http_port>\d+)(-|(?P<http_path>/[^?]*?))(\?(?P<http_query>[^ ]*))?) (- |-|\w+/(?P<http_version>[0-9\.]*)))\" (-|\"(|(?P<useragent>.+))\") (?P<ssl_cipher>[^ ]+) (?P<ssl_protocol>[^ ]+)'

    def __init__(self, input_file):
        super().__init__(CLBAccessLogParser.REGEX, input_file)


class CloudFrontRealtimeAccessLogParser(Parser):

    REGEX = r'(?P<timestamp>[0-9\.]+)\t(?P<c_ip>[^\t]+)\t(?P<time_to_first_byte>[^\t]+)\t(?P<sc_status>[^\t]+)\t(?P<sc_bytes>[^\t]+)\t(?P<cs_method>[^\t]+)\t(?P<cs_protocol>[^\t]+)\t(?P<cs_host>[^\t]+)\t(?P<cs_uri_stem>[^\t]+)\t(?P<cs_bytes>[^\t]+)\t(?P<x_edge_location>[^\t]+)\t(?P<x_edge_request_id>[^\t]+)\t(?P<x_host_header>[^\t]+)\t(?P<time_taken>[^\t]+)\t(?P<cs_protocol_version>[^\t]+)\t(?P<c_ip_version>[^\t]+)\t(?P<cs_user_agent>[^\t]+)\t(?P<cs_referer>[^\t]+)\t(?P<cs_cookie>[^\t]+)\t(?P<cs_uri_query>[^\t]+)\t(?P<x_edge_response_result_type>[^\t]+)\t(?P<x_forwarded_for>[^\t]+)\t(?P<ssl_protocol>[^\t]+)\t(?P<ssl_cipher>[^\t]+)\t(?P<x_edge_result_type>[^\t]+)\t(?P<fle_encrypted_fields>[^\t]+)\t(?P<fle_status>[^\t]+)\t(?P<sc_content_type>[^\t]+)\t(?P<sc_content_len>[^\t]+)\t(?P<sc_range_start>[^\t]+)\t(?P<sc_range_end>[^\t]+)\t(?P<c_port>[^\t]+)\t(?P<x_edge_detailed_result_type>[^\t]+)\t(?P<c_country>[^\t]+)\t(?P<cs_accept_encoding>[^\t]+)\t(?P<cs_accept>[^\t]+)\t(?P<cache_behavior_path_pattern>[^\t]+)\t(?P<cs_headers>[^\t]+)\t(?P<cs_header_names>[^\t]+)\t(?P<cs_headers_count>[^\t]+)'

    def __init__(self, input_file):
        super().__init__(CloudFrontRealtimeAccessLogParser.REGEX, input_file)


class CloudFrontStandardAccessLogParser(Parser):

    REGEX = r'(?P<date>[0-9-]+)\t(?P<time>[0-9:]+)\t(?P<x_edge_location>[0-9A-Z-]+)\t(?P<sc_bytes>[0-9]+)\t(?P<c_ip>[0-9a-f.:]+)\t(?P<cs_method>[A-Z]+)\t(?P<cs_host>[0-9A-Za-z.]+)\t(?P<cs_uri_stem>[^\t]+)\t(?P<sc_status>[0-9-]+)\t(?P<cs_referer>[^\t]+)\t(?P<cs_user_agent>[^\t]+)\t(?P<cs_uri_query>[^\t]+)\t(?P<cs_cookie>[^\t]+)\t(?P<x_edge_result_type>[^\t]+)\t(?P<x_edge_request_id>[^\t]+)\t(?P<x_host_header>[^\t]+)\t(?P<cs_protocol>[^\t]+)\t(?P<cs_bytes>[^\t]+)\t(?P<time_taken>[^\t]+)\t(?P<x_forwarded_for>[^\t]+)\t(?P<ssl_protocol>[^\t]+)\t(?P<ssl_cipher>[^\t]+)\t(?P<x_edge_response_result_type>[^\t]+)\t(?P<cs_protocol_version>[^\t]+)\t(?P<fle_status>[^\t]+)\t(?P<fle_encrypted_fields>[^\t]+)(\t(?P<c_port>[^\t]+)\t(?P<time_to_first_byte>[^\t]+)\t(?P<x_edge_detailed_result_type>[^\t]+)\t(?P<sc_content_type>[^\t]+)\t(?P<sc_content_len>[^\t]+)\t(?P<sc_range_start>[^\t]+)\t(?P<sc_range_end>[^\t]+))?'

    def __init__(self, input_file):
        super().__init__(CloudFrontStandardAccessLogParser.REGEX, input_file)

    # https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#BasicDistributionFileFormat
    def get_parsed_lines(self):
        for line in self.input_file.get_lines():
            # Cloudfront Standard log files contain headers starting with '#'. This also skips empty lines,
            # should there be any.
            if len(line) > 0 and line[0] != '#':
                yield self.parse(line)


class VPCFlowLogParser(Parser):

    REGEX = r'(?P<version>[^ ]+) (?P<account_id>[^ ]+) (?P<interface_id>[^ ]+) (?P<srcaddr>[^ ]+) (?P<dstaddr>[^ ]+) (?P<srcport>[^ ]+) (?P<dstport>[^ ]+) (?P<protocol>[^ ]+) (?P<packets>[^ ]+) (?P<bytes>[^ ]+) (?P<start>[^ ]+) (?P<end>[^ ]+) (?P<action>[^ ]+) (?P<log_status>[^ ]+)( (?P<vpc_id>[^ ]+) (?P<subnet_id>[^ ]+) (?P<instance_id>[^ ]+) (?P<tcp_flags>[^ ]+) (?P<type>[^ ]+) (?P<pkt_srcaddr>[^ ]+) (?P<pkt_dstaddr>[^ ]+) (?P<region>[^ ]+) (?P<az_id>[^ ]+) (?P<sublocation_type>[^ ]+) (?P<sublocation_id>[^ ]+) (?P<pkt_src_aws_service>[^ ]+) (?P<pkt_dst_aws_service>[^ ]+) (?P<flow_direction>[^ ]+) (?P<traffic_path>[^ ]+))?'

    def __init__(self, input_file):
        super().__init__(VPCFlowLogParser.REGEX, input_file)


class DefaultParser(Parser):

    def __init__(self, input_file):
        super().__init__(None, input_file)

    def parse(self, line: str):
        return line


class CloudTrailParser(Parser):

    def __init__(self, input_file):
        super().__init__(None, input_file)

    def parse(self, line: str):
        return line

    def get_parsed_lines(self):
        for line in self.input_file.get_lines():
            data = json.loads(line)
            if 'Records' in data:
                for record in data['Records']:
                    yield json.dumps(record)
            else:
                yield line


class ParserFactory:

    @staticmethod
    def get_parser(log_type, input_file):
        if log_type == S3_ACCESS_LOG_TYPE:
            return S3AccessLogParser(input_file)
        if log_type == ALB_ACCESS_LOG_TYPE:
            return ALBAccessLogParser(input_file)
        if log_type == NLB_ACCESS_LOG_TYPE:
            return NLBAccessLogParser(input_file)
        if log_type == CLASSIC_LB_ACCESS_LOG_TYPE:
            return CLBAccessLogParser(input_file)
        if log_type == CLOUDFRONT_REALTIME_ACCESS_LOG_TYPE:
            return DefaultParser(input_file)
        if log_type == CLOUDFRONT_STANDARD_ACCESS_LOG_TYPE:
            return CloudFrontStandardAccessLogParser(input_file)
        if log_type == CLOUDTRAIL_LOG_TYPE:
            return CloudTrailParser(input_file)
        if log_type == VPC_FLOW_LOG_TYPE:
            return VPCFlowLogParser(input_file)
        if log_type == BEDROCK_S3_LOG_TYPE:
            return DefaultParser(input_file)
        if log_type == CLOUDWATCH_LOG_TYPE:
            return DefaultParser(input_file)
        return DefaultParser(input_file)
