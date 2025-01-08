from data_retriever import DataRetriever
from config import DestinationConfig, CLOUDWATCH_LOG_TYPE
from exceptions import LogTypeMissingException


class DestinationProvider:

    def __init__(self, dest_config: DestinationConfig, data_retriever: DataRetriever):
        self._config: DestinationConfig = dest_config
        self._data_retriever: DataRetriever = data_retriever

    def get_type(self, data_id):
        if (self._config.destination_config is not None and data_id in self._config.get_keys() and
                self._config.get_log_type(data_id) is not None):
            return self._config.get_log_type(data_id)
        if self._data_retriever.get_collection_type() == 'cloudwatch':
            return CLOUDWATCH_LOG_TYPE
        raise LogTypeMissingException('Log type required in configuration. data_type=%s, data_id=%s',
                                      self._data_retriever.get_collection_type(), data_id)

    def get_dataset(self, data_id):
        if (self._config.destination_config is not None and data_id in self._config.get_keys() and
                self._config.get_dataset(data_id) is not None):
            return self._config.get_dataset(data_id)
        if self._data_retriever.get_collection_type() == 'cloudwatch':
            return data_id
        return None

    def get_collection(self, data_id):
        if (self._config.destination_config is not None and data_id in self._config.get_keys() and
                self._config.get_collection(data_id) is not None):
            return self._config.get_collection(data_id)
        if self._data_retriever.get_collection_type() == 'cloudwatch':
            return self._config.get_cloudwatch_default_collection()
        return None
