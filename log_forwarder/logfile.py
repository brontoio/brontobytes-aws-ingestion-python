import gzip


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
                yield line.decode()


class PlaintextFile(LogFile):

    def __init__(self, filepath):
        super().__init__(filepath)
        self.file = open(self.filepath, 'r')

    def get_file(self):
        return self.file

    def get_lines(self):
        with self.file as f:
            for line in f.readlines():
                yield line


class LogFileFactory:

    @staticmethod
    def get_log_file(log_type, filepath):
        if log_type in ['s3_access_log', 'cloudwatch_log']:
            return PlaintextFile(filepath)
        if log_type in ['alb_access_log', 'cloudtrail_log']:
            return GZipFile(filepath)
        return GZipFile(filepath)
