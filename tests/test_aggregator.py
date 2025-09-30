import pytest
from queue import Queue

from log_forwarder.aggregator import JavaStackTraceAggregator, NoopAggregator, AggregatorFactory


def test_aggregator_factory_stack_trace():
    assert isinstance(AggregatorFactory.get_aggregator('java_stack_trace'), JavaStackTraceAggregator)


def test_aggregator_factory_default():
    assert isinstance(AggregatorFactory.get_aggregator('anything_but_java_stack_trace'), NoopAggregator)


#
# NoopAggregator
#

def test_noop_init():
    aggregator = NoopAggregator()
    assert isinstance(aggregator.lines, Queue)
    assert aggregator.lines.maxsize == 2
    assert aggregator.lines.empty()


def test_noop_add_single_line():
    aggregator = NoopAggregator()
    line = "Exception in thread main java.lang.NullPointerException"
    aggregator.add_line(line)
    assert aggregator.get_complete_aggregated_line() is None


def test_noop_add_normal_line():
    aggregator = NoopAggregator()
    line = "Exception in thread main java.lang.NullPointerException"
    aggregator.add_line(line)
    aggregator.add_line(None)
    assert aggregator.get_complete_aggregated_line() == line


def test_noop_add_stack_trace():
    aggregator = NoopAggregator()
    input_lines = ["Exception in thread main java.lang.NullPointerException", "at com.example.Main.method(Main.java:42)"]

    aggregator.add_line(input_lines[0])
    aggregator.add_line(input_lines[1])
    result = [aggregator.get_complete_aggregated_line()]
    aggregator.add_line(None)
    result.append(aggregator.get_complete_aggregated_line())
    assert result == input_lines

#
# Java Stack Trace Aggregator
#

def test_init():
    aggregator = JavaStackTraceAggregator()
    assert isinstance(aggregator.lines, Queue)
    assert aggregator.lines.maxsize == 2
    assert aggregator.lines.empty()


def test_add_line_normal_line():
    aggregator = JavaStackTraceAggregator()
    line = "Exception in thread main java.lang.NullPointerException"
    aggregator.add_line(line)
    assert aggregator.lines.qsize() == 1
    assert aggregator.get_complete_aggregated_line() is None


def test_add_line_stack_trace_line():
    aggregator = JavaStackTraceAggregator()
    main_line = "Exception in thread main java.lang.NullPointerException"
    stack_line = "at com.example.Main.method(Main.java:42)"

    aggregator.add_line(main_line)
    aggregator.add_line(stack_line)

    assert aggregator.lines.qsize() == 1
    result = aggregator.lines.get()
    expected = f"{main_line}\\n{stack_line}"
    assert result == expected


def test_add_line_stack_trace_line_with_caused_by():
    aggregator = JavaStackTraceAggregator()
    main_line = "Exception in thread main java.lang.NullPointerException"
    stack_line = "Caused by: java.net.SocketException: some socket error"

    aggregator.add_line(main_line)
    aggregator.add_line(stack_line)

    assert aggregator.lines.qsize() == 1
    result = aggregator.lines.get()
    expected = f"{main_line}\\n{stack_line}"
    assert result == expected

def test_add_line_stack_trace_line_with_3_dots():
    aggregator = JavaStackTraceAggregator()
    main_line = "Exception in thread main java.lang.NullPointerException"
    stack_line = "Caused by: java.net.SocketException: some socket error"

    aggregator.add_line(main_line)
    aggregator.add_line(stack_line)

    assert aggregator.lines.qsize() == 1
    result = aggregator.lines.get()
    expected = f"{main_line}\\n{stack_line}"
    assert result == expected

def test_add_line_multiple_stack_trace_lines():
    aggregator = JavaStackTraceAggregator()
    main_line = "Exception in thread main java.lang.NullPointerException"
    stack_line1 = "at com.example.Main.method1(Main.java:42)"
    stack_line2 = "at com.example.Main.method2(Main.java:35)"

    aggregator.add_line(main_line)
    aggregator.add_line(stack_line1)
    aggregator.add_line(stack_line2)

    assert aggregator.lines.qsize() == 1
    result = aggregator.lines.get()
    expected = f"{main_line}\\n{stack_line1}\\n{stack_line2}"
    assert result == expected


def test_add_line_none():
    aggregator = JavaStackTraceAggregator()
    aggregator.add_line(None)
    assert aggregator.lines.qsize() == 1
    assert aggregator.lines.get() is None


def test_add_line_queue_full_exception():
    aggregator = JavaStackTraceAggregator()
    aggregator.add_line("line1")
    aggregator.add_line("line2")

    with pytest.raises(Exception, match="Aggregator queue is full"):
        aggregator.add_line("line3")


def test_get_line_empty_queue():
    aggregator = JavaStackTraceAggregator()
    assert aggregator.get_complete_aggregated_line() is None


def test_get_line_partial_queue():
    aggregator = JavaStackTraceAggregator()
    aggregator.add_line("single line")
    assert aggregator.get_complete_aggregated_line() is None


def test_get_line_full_queue():
    aggregator = JavaStackTraceAggregator()
    aggregator.add_line("line1")
    aggregator.add_line("line2")

    result = aggregator.get_complete_aggregated_line()
    assert result == "line1"
    assert aggregator.lines.qsize() == 1


def test_stack_trace_aggregation_workflow():
    aggregator = JavaStackTraceAggregator()

    exception_line = "java.lang.RuntimeException: Something went wrong"
    stack_line1 = "at com.example.Service.doWork(Service.java:123)"
    stack_line2 = "at com.example.Main.main(Main.java:45)"
    next_line = "INFO: Processing completed"

    aggregator.add_line(exception_line)
    aggregator.add_line(stack_line1)
    assert aggregator.get_complete_aggregated_line() is None

    aggregator.add_line(stack_line2)
    assert aggregator.get_complete_aggregated_line() is None

    aggregator.add_line(next_line)

    first_result = aggregator.get_complete_aggregated_line()
    expected_stack_trace = f"{exception_line}\\n{stack_line1}\\n{stack_line2}"
    assert first_result == expected_stack_trace

    # add poison pill
    aggregator.add_line(None)
    second_result = aggregator.get_complete_aggregated_line()
    assert second_result == next_line


def test_non_stack_trace_after_stack_trace():
    aggregator = JavaStackTraceAggregator()

    line1 = "Normal log line"
    stack_line = "at com.example.Test.method(Test.java:10)"
    line2 = "Another normal line"

    aggregator.add_line(line1)
    aggregator.add_line(stack_line)
    aggregator.add_line(line2)

    first_result = aggregator.get_complete_aggregated_line()
    expected = f"{line1}\\n{stack_line}"
    assert first_result == expected

    no_result_as_no_poison_pill = aggregator.get_complete_aggregated_line()
    assert no_result_as_no_poison_pill is None
    aggregator.add_line(None)
    second_result = aggregator.get_complete_aggregated_line()
    assert second_result == line2


def test_edge_case_stack_trace_prefix():
    aggregator = JavaStackTraceAggregator()

    main_line = "Exception occurred"
    not_stack_line = "AT the beginning of line"
    actual_stack_line = "at com.example.Class.method(Class.java:1)"

    aggregator.add_line(main_line)
    aggregator.add_line(not_stack_line)

    first_result = aggregator.get_complete_aggregated_line()
    assert first_result == main_line

    aggregator.add_line(actual_stack_line)
    # add poison pill / termination marker
    aggregator.add_line(None)
    second_result = aggregator.get_complete_aggregated_line()
    expected = f"{not_stack_line}\\n{actual_stack_line}"
    assert second_result == expected