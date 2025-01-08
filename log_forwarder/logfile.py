import gzip

from config import (ALB_ACCESS_LOG_TYPE, CLOUDTRAIL_LOG_TYPE, CLOUDWATCH_LOG_TYPE, VPC_FLOW_LOG_TYPE,
                    S3_ACCESS_LOG_TYPE)


class LogFile:

    def __init__(self, filepath):
        self.filepath = filepath

    def get_lines(self):
        raise NotImplementedError()

    def get_file(self):
        raise NotImplementedError()

    def get_filepath(self):
        return self.filepath


class GZipFile(LogFile):

    def __init__(self, filepath):
        super().__init__(filepath)
        self.file = gzip.open(self.filepath, 'rb')

    def get_file(self):
        return self.file

    def get_lines(self):
        with self.file as f:
            for line in f.readlines():
                yield line.decode().strip()


class PlaintextFile(LogFile):

    def __init__(self, filepath):
        super().__init__(filepath)
        self.file = open(self.filepath, 'r')

    def get_file(self):
        return self.file

    def get_lines(self):
        with self.file as f:
            for line in f.readlines():
                yield line.strip()


class LogFileFactory:

    @staticmethod
    def get_log_file(log_type, filepath):
        if log_type is None or log_type in [S3_ACCESS_LOG_TYPE, CLOUDWATCH_LOG_TYPE]:
            return PlaintextFile(filepath)
        if log_type in [ALB_ACCESS_LOG_TYPE, CLOUDTRAIL_LOG_TYPE, VPC_FLOW_LOG_TYPE]:
            return GZipFile(filepath)
        return GZipFile(filepath)
