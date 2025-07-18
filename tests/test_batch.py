import json

from clients import Batch


def test_add_to_batch():
    batch = Batch()
    entry = "an entry"
    batch.add(entry)
    assert batch.get_batch_size() == len(entry)
    assert batch.get_data() == [entry]


def test_get_formatted_batch():
    batch = Batch()
    entry = "an entry"
    batch.add(entry)
    attributes = {'key': 'value'}
    expected = {'log': entry}
    expected.update(attributes)
    assert json.loads(batch.get_formatted_data(attributes)) == expected

def test_get_formatted_batch_with_no_formatting():
    batch = Batch(no_formatting=True)
    entry = "an entry"
    batch.add(entry)
    attributes = {'key': 'value'}
    assert batch.get_formatted_data(attributes) == entry

