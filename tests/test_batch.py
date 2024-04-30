from clients import Batch


def test_add_to_batch():
    batch = Batch()
    entry = "an entry"
    batch.add(entry)
    assert batch.get_batch_size() == len(entry)
    assert batch.get_data() == [entry]
