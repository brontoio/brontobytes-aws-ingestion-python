import logging
import tempfile
import shutil

from config import Config, DestinationConfig
from data_retriever import DataRetrieverFactory
from destination_provider import DestinationProvider
from parser import ParserFactory
from clients import BrontoClient, Batch
from logfile import LogFileFactory

logger = logging.getLogger()
logger.setLevel("INFO")

MB = 1000 * 1000


def process(event):
    logger.debug('Processing event. event=%s', event)
    dest_config: DestinationConfig = DestinationConfig()
    # ephemeral storage path is based on https://docs.aws.amazon.com/lambda/latest/dg/configuration-ephemeral-storage.html
    total, used, free = shutil.disk_usage("/tmp")
    logger.info('Ephemeral disk usage. used_mb=%.3f, usage_ratio=%.6f', used / MB, used / total if total > 0 else -1)
    with tempfile.NamedTemporaryFile(delete=True, delete_on_close=True) as f:
        config: Config = Config(event, f.name)

        retrievers = DataRetrieverFactory.get_data_retrievers(config, dest_config)
        if len(retrievers) == 0:
            logger.warning('Event does not map to any retriever.')

        for data_retriever in retrievers:
            if data_retriever is None:
                logger.info('Unknown data type from event. Skipping.')
                continue
            logger.info('Data retriever selected. data_retriever=%s', type(data_retriever).__name__)
            # We need to retrieve the data in order to be able to determine data_id, dataset, etc in the case of
            # CloudWatch logs
            data_retriever.get_data()
            data_id = data_retriever.get_data_id()
            logger.info('Data ID retrieved. data_id=%s', data_id)

            destination_provider = DestinationProvider(dest_config, data_retriever)
            dataset = destination_provider.get_dataset(data_id)
            collection = destination_provider.get_collection(data_id)
            log_type = dest_config.get_log_type(data_id)
            client_type = dest_config.get_client_type(data_id)
            logger.info('Destination information retrieved. dataset=%s, collection=%s, log_type=%s', dataset,
                        collection, log_type)
            if log_type is None:
                logger.info('Log type not specified in configuration. Assuming type is Cloudwatch.')

            input_file = LogFileFactory.get_log_file(log_type, config.filepath)
            logger.info('Input file type detected. input_file=%s', type(input_file).__name__)
            parser = ParserFactory.get_parser(log_type, input_file)
            logger.info('Parser selected. parser=%s', type(parser).__name__)
            attributes = config.get_resource_attributes()
            attributes.update(data_retriever.get_log_attributes_from_payload())
            bronto_client = BrontoClient(dest_config.bronto_api_key, dest_config.bronto_endpoint, dataset, collection,
                client_type, config.tags)
            no_formatting = client_type is not None
            batch = Batch(no_formatting)
            for line in parser.get_parsed_lines():
                batch.add(line)
                if batch.get_batch_size() > dest_config.max_batch_size:
                    bronto_client.send_data(batch, attributes)
                    batch = Batch()
            if batch.get_batch_size() > 0:
                bronto_client.send_data(batch, config.get_resource_attributes())


def forward_logs(_event, _):
    logger.debug('event=%s', _event)
    source = _event.get('source')
    _event_details = _event.get('detail')
    # event coming from S3 via EventBridge
    if source is not None and source == 'aws.s3' and _event_details is not None:
        event = {'Records': [{'s3': _event_details}]}
        process(event)
        return
    # event coming from Cloudwatch or S3 notification
    process(_event)
