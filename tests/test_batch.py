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
