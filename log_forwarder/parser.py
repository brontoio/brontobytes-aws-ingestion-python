import re
import json
import gzip
import os

# Regex used in this file mostly come from:
# https://github.com/aws-samples/siem-on-amazon-opensearch-service/blob/v2.10.2/source/lambda/es_loader/aws.ini
#


class Parser:

    def __init__(self, regex):
        self.regex = regex
        self.pattern = re.compile(self.regex) if self.regex is not None else None

    def parse(self, line: str):
        if self.regex is None:
            return line
        parsed = re.match(self.pattern, line)
        if parsed is not None:
            return json.dumps(parsed.groupdict())
        return line

    def parse_file(self, logfile):
        parse_filepath = os.path.join(os.path.dirname(logfile.get_filepath()), "parsed_" +
                                      os.path.basename(logfile.get_filepath())) + ".gz"
        with gzip.open(parse_filepath, 'wb') as f:
            for line in logfile.get_lines():
                parsed = self.parse(line) + "\n"
                f.write(parsed.encode())
        return parse_filepath


class S3AccessLogParser(Parser):

    REGEX = r'(?P<BucketOwner>[^ ]*) (?P<Bucket>[^ ]*) \[(?P<RequestDateTime>.*?)\] (?P<RemoteIP>[^ ]*) (?P<Requester>[^ ]*) (?P<RequestID>[^ ]*) (?P<Operation>[^ ]*) (?P<Key>[^ ]*) (\"(?P<RequestURI_operation>[^ ]*) (?P<RequestURI_key>[^ ]*) (?P<RequestURI_httpProtoversion>- |[^ ]*)\"|\"-\"|-) (?P<HTTPstatus>-|[0-9]*) (?P<ErrorCode>[^ ]*) (?P<BytesSent>[^ ]*) (?P<ObjectSize>[^ ]*) (?P<TotalTime>[^ ]*) (?P<TurnAroundTime>[^ ]*) (\"(?P<Referrer>.*?)\"|-) (\"(?P<UserAgent>.*?)\"|-) (?P<VersionId>[^ ]*)(?: (?P<HostId>[^ ]*) (?P<SigV>[^ ]*) (?P<CipherSuite>[^ ]*) (?P<AuthType>[^ ]*) (?P<EndPoint>[^ ]*) (?P<TLSVersion>[^ ]*))?( (?P<S3AccessPointARN>arn:[^ ]*|-))?'

    def __init__(self):
        super().__init__(S3AccessLogParser.REGEX)


class ALBAccessLogParser(Parser):

    REGEX = r'(?P<request_type>[^ ]*) (?P<timestamp>[^ ]*) (?P<elb>[^ ]*) (?P<client_ip>[^ ]*):(?P<client_port>[0-9]*) (?P<target_ip>[^ ]*)[:-](?P<target_port>[0-9]*) (?P<request_processing_time>[-.0-9]*) (?P<target_processing_time>[-.0-9]*) (?P<response_processing_time>[-.0-9]*) (?P<elb_status_code>|[-0-9]*) (?P<target_status_code>-|[-0-9]*) (?P<received_bytes>[-0-9]*) (?P<sent_bytes>[-0-9]*) \"(?P<request>(-|(?P<http_method>\w+)) (-|(?P<http_protocol>\w*)://\[?(?P<http_host>[^/]+?)\]?:(?P<http_port>\d+)(-|(?P<http_path>/[^?]*?))(\?(?P<http_query>.*?))?) (-?|\w+/(?P<http_version>[0-9\.]*)))\" \"(|(?P<useragent>[^\"]+))\" (?P<ssl_cipher>[()A-Z0-9-]+) (?P<ssl_protocol>[A-Za-z0-9.-]*) (?P<target_group_arn>[^ ]*) \"(?P<trace_id>[^\"]*)\" \"(?P<domain_name>[^\"]*)\" \"(?P<chosen_cert_arn>[^\"]*)\" (?P<matched_rule_priority>[-.0-9]*) (?P<request_creation_time>[^ ]*) \"(?P<actions_executed>[^\"]*)\" \"(?P<redirect_url>[^\"]*)\" \"(?P<lambda_error_reason>[^ ]*)\" \"(?P<target_port_list>[^\s]+)\" \"(?P<target_status_code_list>[^\s]+)\"( \"(?P<classification>[^\s]+)\" \"(?P<classification_reason>[^\s]+)\")?'

    def __init__(self):
        super().__init__(ALBAccessLogParser.REGEX)


class NLBAccessLogParser(Parser):

    REGEX = r'(?P<listener_type>[^ ]+) (?P<log_entry_version>[^ ]+) (?P<timestamp>[^ ]+) (?P<elb>[^ ]+) (?P<listener>[^ ]+) (?P<client_ip>[0-9a-f.:]+):(?P<client_port>[0-9]+) (?P<destination_ip>[^ ]+):(?P<destination_port>[0-9]+) (?P<connection_time>[0-9]+) (-|(?P<tls_handshake_time>[0-9]+)) (-|(?P<received_bytes>[-0-9]+)) (?P<sent_bytes>[-0-9]+) (-|(?P<incoming_tls_alert>[^ ]+)) (-|(?P<chosen_cert_arn>[^ ]+)) (-|(?P<chosen_cert_serial>[^ ]+)) (-|(?P<tls_cipher>[^ ]+)) (-|(?P<tls_protocol_version>[^ ]+)) (-|(?P<tls_named_group>[^ ]+)) (-|(?P<domain_name>[^ ]+)) (-|(?P<alpn_fe_protocol>[^ ]+)) (-|(?P<alpn_be_protocol>[^ ]+)) (-|(?P<alpn_client_preference_list>[^ ]+))( (?P<tls_connection_creation_time>20[0-9T:-]+))?'

    def __init__(self):
        super().__init__(NLBAccessLogParser.REGEX)


class CLBAccessLogParser(Parser):

    REGEX = r'(?P<timestamp>[^ ]+) (?P<elb>[^ ]+) (?P<client_ip>[0-9a-f.:]+):(?P<client_port>[0-9]+) (-|(?P<backend_ip>[0-9a-f.:]+):(?P<backend_port>[-0-9]+)) (?P<request_processing_time>[0-9\.-]+) (?P<backend_processing_time>[0-9\.-]+) (?P<response_processing_time>[0-9\.-]+) (?P<elb_status_code>[0-9\.-]+) (?P<backend_status_code>[0-9\.-]+) (?P<received_bytes>[0-9\.-]+) (?P<sent_bytes>[0-9\.-]+) \"(?P<request>(-|(?P<http_method>[\w-]+)) (-|(?P<http_protocol>\w*)://\[?(?P<http_host>[^\[\]]+?)\]?:(?P<http_port>\d+)(-|(?P<http_path>/[^?]*?))(\?(?P<http_query>[^ ]*))?) (- |-|\w+/(?P<http_version>[0-9\.]*)))\" (-|\"(|(?P<useragent>.+))\") (?P<ssl_cipher>[^ ]+) (?P<ssl_protocol>[^ ]+)'

    def __init__(self):
        super().__init__(CLBAccessLogParser.REGEX)


class CloudFrontRealtimeAccessLogParser(Parser):

    REGEX = r'(?P<timestamp>[0-9\.]+)\t(?P<c_ip>[^\t]+)\t(?P<time_to_first_byte>[^\t]+)\t(?P<sc_status>[^\t]+)\t(?P<sc_bytes>[^\t]+)\t(?P<cs_method>[^\t]+)\t(?P<cs_protocol>[^\t]+)\t(?P<cs_host>[^\t]+)\t(?P<cs_uri_stem>[^\t]+)\t(?P<cs_bytes>[^\t]+)\t(?P<x_edge_location>[^\t]+)\t(?P<x_edge_request_id>[^\t]+)\t(?P<x_host_header>[^\t]+)\t(?P<time_taken>[^\t]+)\t(?P<cs_protocol_version>[^\t]+)\t(?P<c_ip_version>[^\t]+)\t(?P<cs_user_agent>[^\t]+)\t(?P<cs_referer>[^\t]+)\t(?P<cs_cookie>[^\t]+)\t(?P<cs_uri_query>[^\t]+)\t(?P<x_edge_response_result_type>[^\t]+)\t(?P<x_forwarded_for>[^\t]+)\t(?P<ssl_protocol>[^\t]+)\t(?P<ssl_cipher>[^\t]+)\t(?P<x_edge_result_type>[^\t]+)\t(?P<fle_encrypted_fields>[^\t]+)\t(?P<fle_status>[^\t]+)\t(?P<sc_content_type>[^\t]+)\t(?P<sc_content_len>[^\t]+)\t(?P<sc_range_start>[^\t]+)\t(?P<sc_range_end>[^\t]+)\t(?P<c_port>[^\t]+)\t(?P<x_edge_detailed_result_type>[^\t]+)\t(?P<c_country>[^\t]+)\t(?P<cs_accept_encoding>[^\t]+)\t(?P<cs_accept>[^\t]+)\t(?P<cache_behavior_path_pattern>[^\t]+)\t(?P<cs_headers>[^\t]+)\t(?P<cs_header_names>[^\t]+)\t(?P<cs_headers_count>[^\t]+)'

    def __init__(self):
        super().__init__(CloudFrontRealtimeAccessLogParser.REGEX)


class CloudFrontStandardAccessLogParser(Parser):

    REGEX = r'(?P<date_time>[0-9-:\t]+)\t(?P<x_edge_location>[0-9A-Z-]+)\t(?P<sc_bytes>[0-9]+)\t(?P<c_ip>[0-9a-f.:]+)\t(?P<cs_method>[A-Z]+)\t(?P<cs_host>[0-9A-Za-z.]+)\t(?P<cs_uri_stem>[^\t]+)\t(?P<sc_status>[0-9-]+)\t(?P<cs_referer>[^\t]+)\t(?P<cs_user_agent>[^\t]+)\t(?P<cs_uri_query>[^\t]+)\t(?P<cs_cookie>[^\t]+)\t(?P<x_edge_result_type>[^\t]+)\t(?P<x_edge_request_id>[^\t]+)\t(?P<x_host_header>[^\t]+)\t(?P<cs_protocol>[^\t]+)\t(?P<cs_bytes>[^\t]+)\t(?P<time_taken>[^\t]+)\t(?P<x_forwarded_for>[^\t]+)\t(?P<ssl_protocol>[^\t]+)\t(?P<ssl_cipher>[^\t]+)\t(?P<x_edge_response_result_type>[^\t]+)\t(?P<cs_protocol_version>[^\t]+)\t(?P<fle_status>[^\t]+)\t(?P<fle_encrypted_fields>[^\t]+)(\t(?P<c_port>[^\t]+)\t(?P<time_to_first_byte>[^\t]+)\t(?P<x_edge_detailed_result_type>[^\t]+)\t(?P<sc_content_type>[^\t]+)\t(?P<sc_content_len>[^\t]+)\t(?P<sc_range_start>[^\t]+)\t(?P<sc_range_end>[^\t]+))?'

    def __init__(self):
        super().__init__(CloudFrontStandardAccessLogParser.REGEX)


class DefaultParser(Parser):

    def __init__(self):
        super().__init__(None)

    def parse(self, line: str):
        return line


class ParserFactory:

    @staticmethod
    def get_parser(log_type):
        if log_type == 's3_access_log':
            return S3AccessLogParser()
        if log_type == 'alb_access_log':
            return ALBAccessLogParser()
        if log_type == 'nlb_access_log':
            return NLBAccessLogParser()
        if log_type == 'clb_access_log':
            return CLBAccessLogParser()
        if log_type == 'cf_realtime_access_log':
            return DefaultParser()
        if log_type == 'cf_standard_access_log':
            return CloudFrontStandardAccessLogParser()
        if log_type == 'cloudtrail_log':
            return DefaultParser()
        if log_type == 'cloudwatch_log':
            return DefaultParser()
        return DefaultParser()
