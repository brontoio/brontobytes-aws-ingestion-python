import os
import gzip

from logfile import PlaintextFile, GZipFile


def test_plaintext(tmp_path):
    filepath = os.path.join(tmp_path, 'some_file')
    entry1 = 'some entry 1'
    entry2 = 'some entry 2'
    entries = [entry1, entry2]
    with open(filepath, 'a') as f:
        for entry in entries:
            f.write(entry + '\n')
    my_file = PlaintextFile(filepath)
    assert list(my_file.get_lines()) == entries


def test_gzip(tmp_path):
    filepath = os.path.join(tmp_path, 'some_file')
    entry1 = 'some entry 1'
    entry2 = 'some entry 2'
    entries = [entry1, entry2]
    with gzip.open(filepath, 'a') as f:
        for entry in entries:
            f.write((entry + '\n').encode())
    my_file = GZipFile(filepath)
    assert list(my_file.get_lines()) == entries
