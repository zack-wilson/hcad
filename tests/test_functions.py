import unittest
from pathlib import Path
from typing import Generator

from hcad import functions


class TestSchemaFunctions(unittest.TestCase):
    def setUp(self):
        self.table_names = set(i[0] for i in functions.dictionary)

    def test_get_fields(self):
        result = [functions.get_fields(t) for t in self.table_names]
        self.assertTrue(result)
        self.assertIsInstance(result, list)

    def test_get_field_size_limit(self):
        result = [functions.get_field_size_limit(i) for i in self.table_names]
        self.assertIsInstance(next(iter(result)), int)

    def test_get_max_columns(self):
        result = [functions.get_max_columns(i) for i in self.table_names]
        self.assertIsInstance(next(iter(result)), int)


class TestProcessZip(unittest.TestCase):
    def setUp(self):
        self.sample_zip_files = Path("tests/samples").rglob("*.zip")

    def test_decompress_zip(self):
        result = functions.decompress_zip(*self.sample_zip_files)
        self.assertIsInstance(result, Generator)
        self.assertIsInstance(next(result), Path)


class TestProcessGzip(unittest.TestCase):
    def setUp(self):
        self.files = Path("tests/samples").rglob("*.gz")

    def test_decompress_gzip(self):
        result = functions.decompress_gzip(*self.files)
        self.assertTrue(result)


class TestProcessTxt(unittest.TestCase):
    def setUp(self):
        self.sample_text_files = Path("tests/samples").rglob("*.txt")

    def test_process_txt(self):

        result = functions.process_txt(*self.sample_text_files)
        self.assertIsInstance(result, Generator)


if __name__ == "__main__":
    unittest.main()
