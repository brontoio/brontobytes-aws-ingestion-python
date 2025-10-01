from typing import Union
from queue import Queue

AGGREGATED_TERMINATION_MARKER = None


class Aggregator:

    _NO_AGGREGATED_LINE = None

    def add_line(self, line: Union[str, None]):
        raise NotImplementedError()

    def get_complete_aggregated_line(self):
        raise NotImplementedError()

    def complete(self):
        self.add_line(AGGREGATED_TERMINATION_MARKER)

    def has_complete_aggregated_line(self):
        raise NotImplementedError()


class NoopAggregator(Aggregator):

    def __init__(self):
        self.lines = Queue(maxsize=2)

    def add_line(self, line: Union[str, None]):
        if self.has_complete_aggregated_line():
            raise Exception('Complete aggregated lines must be retrieved first')
        if line is not None and line == '':
            return Aggregator._NO_AGGREGATED_LINE
        self.lines.put(line)
        return

    def get_complete_aggregated_line(self):
        if self.has_complete_aggregated_line():
            return self.lines.get()
        return Aggregator._NO_AGGREGATED_LINE

    def has_complete_aggregated_line(self):
        return self.lines.full()


class JavaStackTraceAggregator(Aggregator):
    """ Inspired from https://www.elastic.co/docs/reference/beats/filebeat/multiline-examples#_java_stack_traces """

    def __init__(self):
        self.lines = Queue(maxsize=2)

    def add_line(self, line):
        if self.lines.full():
            raise Exception('Aggregator queue is full')
        if line is None:
            current_line = line
        else:
            stripped_line = line.lstrip()
            if stripped_line == '':
                return
            if (stripped_line.startswith('at ') or stripped_line.startswith('Caused by: ') or
                    stripped_line.startswith('...')):
                current_line = self.lines.get().rstrip('\n')
                current_line += f'\\n{line}'
            else:
                current_line = line
        self.lines.put(current_line)
        return

    def get_complete_aggregated_line(self):
        if self.has_complete_aggregated_line():
            return self.lines.get()
        return Aggregator._NO_AGGREGATED_LINE

    def has_complete_aggregated_line(self):
        return self.lines.full()


class AggregatorFactory:

    @staticmethod
    def get_aggregator(aggregator_name) -> Aggregator:
        if aggregator_name == 'java_stack_trace':
            return JavaStackTraceAggregator()
        return NoopAggregator()
