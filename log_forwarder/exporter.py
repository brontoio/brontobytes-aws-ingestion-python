from typing import Dict, List
from aggregator import Aggregator
from clients import BrontoClient, Batch
from parser import Parser


class BrontoExporter:

    def __init__(self, client: BrontoClient, parser: Parser, batch: Batch, aggregator: Aggregator,
                 attributes: Dict[str, str]):
        self.client = client
        self.parser = parser
        self.batch = batch
        self.aggregator = aggregator
        self.attributes = attributes

    def export(self):
        for line in self.parser.get_parsed_lines():
            self.aggregator.add_line(line)
            if not self.aggregator.has_complete_aggregated_line():
                continue
            _line = self.aggregator.get_complete_aggregated_line()
            self.batch.add(_line)
            if self.batch.get_batch_size() > self.batch.max_size:
                self.client.send_data(self.batch, self.attributes)
                self.batch.reset()
        self.aggregator.complete()
        if self.aggregator.has_complete_aggregated_line():
            _line = self.aggregator.get_complete_aggregated_line()
            self.batch.add(_line)
        if self.batch.get_batch_size() > 0:
            self.client.send_data(self.batch, self.attributes)
