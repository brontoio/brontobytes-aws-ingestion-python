import tempfile
import pytest
from typing import List

from exporter import BrontoExporter
from clients import BrontoClient, Batch
from logfile import LogFileFactory
from parser import ParserFactory
from aggregator import JavaStackTraceAggregator, NoopAggregator


class TestBrontoExporter:

    @pytest.fixture()
    def client(self, monkeypatch):
        return BrontoClient('api_key', 'endpoint', 'my_dataset','my_collection',
                            'my_client_type', {})

    @pytest.fixture()
    def log_file(self):
        filepath = tempfile.NamedTemporaryFile(delete=True, delete_on_close=True).name
        # create the file
        open(filepath, 'w')
        # cloudwatch log files are plaintext
        return LogFileFactory.get_log_file('cloudwatch_log', filepath)

    @pytest.fixture()
    def parser(self, log_file):
        return ParserFactory.get_parser('cloudwatch_log', log_file)

    @pytest.fixture()
    def java_stacktrace_aggregator(self):
        return JavaStackTraceAggregator()

    @pytest.fixture()
    def noop_aggregator(self):
        return NoopAggregator()

    @pytest.fixture()
    def batch(self):
        return Batch(2)

    @pytest.fixture()
    def attributes(self, parser, batch, java_stacktrace_aggregator, client):
        return {'key': 'value', 'service': 'test'}

    @staticmethod
    def add_lines_to_file(lines: List[str], filename):
        with open(filename, 'w') as f:
            for line in lines:
                f.write(line + '\n')

    def test_export_no_lines(self, parser, batch, java_stacktrace_aggregator, client, attributes, monkeypatch):
        exporter = BrontoExporter(client, parser, batch, java_stacktrace_aggregator, attributes)
        sent_batches = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda b: sent_batches.append(b))
        exporter.export()

        assert sent_batches == []

    def test_export_as_many_lines_as_max_batch_size(self, parser, batch, java_stacktrace_aggregator, client, attributes, monkeypatch):
        input_lines = [f'log line {i}' for i in range(0, batch.max_size)]
        exporter = BrontoExporter(client, parser, batch, java_stacktrace_aggregator, attributes)
        TestBrontoExporter.add_lines_to_file(input_lines, parser.input_file.filepath)
        sent_lines = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda _, b, __: sent_lines.extend(b.get_data()))
        exporter.export()
        assert sent_lines == [input_line.rstrip('\n') for input_line in input_lines]

    def test_export_less_lines_than_max_batch_size(self, parser, batch, java_stacktrace_aggregator, client, attributes, monkeypatch):
        input_lines = [f'log line {i}' for i in range(0, batch.max_size - 1)]
        exporter = BrontoExporter(client, parser, batch, java_stacktrace_aggregator, attributes)
        TestBrontoExporter.add_lines_to_file(input_lines, parser.input_file.filepath)
        sent_lines = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda _, b, __: sent_lines.extend(b.get_data()))
        exporter.export()
        assert sent_lines == [input_line.rstrip('\n') for input_line in input_lines]

    def test_export_more_lines_then_max_batch_size(self, parser, batch, java_stacktrace_aggregator, client, attributes, monkeypatch):
        input_lines = [f'log line {i}' for i in range(0, batch.max_size + 1)]
        exporter = BrontoExporter(client, parser, batch, java_stacktrace_aggregator, attributes)
        TestBrontoExporter.add_lines_to_file(input_lines, parser.input_file.filepath)
        sent_lines = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda _, b, __: sent_lines.extend(b.get_data()))
        exporter.export()
        assert sent_lines == [input_line.rstrip('\n') for input_line in input_lines]

    def test_export_with_stack_trace(self, parser, batch, java_stacktrace_aggregator, client, attributes, monkeypatch):
        input_lines = ['line.with.SomeException', 'at some.more.specific.line:123']
        exporter = BrontoExporter(client, parser, batch, java_stacktrace_aggregator, attributes)
        TestBrontoExporter.add_lines_to_file(input_lines, parser.input_file.filepath)
        sent_lines = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda _, b, __: sent_lines.extend(b.get_data()))
        exporter.export()
        assert sent_lines == [f'{input_lines[0].rstrip('\n')}\\n{input_lines[1]}']

    def test_export_with_stack_trace_with_return(self, parser, batch, java_stacktrace_aggregator, client, attributes, monkeypatch):
        input_lines = ['line.with.SomeException\n', 'at some.more.specific.line:123']
        exporter = BrontoExporter(client, parser, batch, java_stacktrace_aggregator, attributes)
        TestBrontoExporter.add_lines_to_file(input_lines, parser.input_file.filepath)
        sent_lines = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda _, b, __: sent_lines.extend(b.get_data()))
        exporter.export()
        assert sent_lines == [f'{input_lines[0].rstrip('\n')}\\n{input_lines[1]}']

    def test_export_with_noop_aggregator(self, parser, batch, noop_aggregator, client, attributes, monkeypatch):
        input_lines = [f'log line {i}' for i in range(0, batch.max_size)]
        exporter = BrontoExporter(client, parser, batch, noop_aggregator, attributes)
        TestBrontoExporter.add_lines_to_file(input_lines, parser.input_file.filepath)
        sent_lines = []
        monkeypatch.setattr(BrontoClient, 'send_data', lambda _, b, __: sent_lines.extend(b.get_data()))
        exporter.export()
        assert sent_lines == [input_line.rstrip('\n') for input_line in input_lines]
