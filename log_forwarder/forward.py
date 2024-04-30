import logging

from config import Config, DestinationConfig
from data_retriever import DataRetrieverFactory
from parser import ParserFactory
from clients import BrontoClient, Batch
from logfile import LogFileFactory

logger = logging.getLogger()
logger.setLevel("INFO")


def forward_logs(event, _):
    dest_config = DestinationConfig()
    config = Config(event)

    for data_retriever in DataRetrieverFactory.get_data_retrievers(config):
        if data_retriever is None:
            logger.info('Unknown data type from event. Aborting. event=%s', event)
            return
        logger.info('Data retriever selected. data_retriever=%s', type(data_retriever).__name__)
        # We need to retrieve the data in order to be able to determine data_id, log_name, etc in the case of
        # CloudWatch logs
        data_retriever.get_data()
        data_id = data_retriever.get_data_id()
        logger.info('Data ID retrieved. data_id=%s', data_id)

        log_name = dest_config.get_log_name(data_id)
        log_set = dest_config.get_log_set(data_id)
        log_type = dest_config.get_log_type(data_id)
        logger.info('Destination information retrieved. log_name=%s, log_set=%s, log_type=%s', log_name, log_set,
                    log_type)
        if log_type is None:
            logger.info('Log type could not be retrieved. Aborting. event=%s', event)
            return

        input_file = LogFileFactory.get_log_file(log_type, config.filepath)
        logger.info('Input file type detected. input_file=%s', type(input_file).__name__)
        parser = ParserFactory.get_parser(log_type, input_file)
        logger.info('Parser selected. parser=%s', type(parser).__name__)
        bronto_client = BrontoClient(dest_config.bronto_api_key, dest_config.bronto_endpoint, log_name, log_set)
        batch = Batch()
        for line in parser.get_parsed_lines():
            batch.add(line)
            if batch.get_batch_size() > dest_config.max_batch_size:
                bronto_client.send_data(batch)
                batch = Batch()
        if batch.get_batch_size() > 0:
            bronto_client.send_data(batch)
