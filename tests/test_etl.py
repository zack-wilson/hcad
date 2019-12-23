import io
import subprocess
import unittest
from collections import OrderedDict
from pathlib import Path
from typing import Generator, TextIO

from hcad import etl


class TestFunctions(unittest.TestCase):
    def setUp(self):
        self.samples = Path("samples")
        self.table_names = set(i[0] for i in etl.dictionary)

    def test_dictionary(self):
        result = etl.dictionary
        self.assertIsInstance(result, list)

    def test_get_fields(self):
        result = [etl.get_fields(t) for t in self.table_names]
        self.assertTrue(result)
        self.assertIsInstance(result, list)

    def test_get_field_size_limit(self):
        result = [etl.get_field_size_limit(i) for i in self.table_names]
        self.assertIsInstance(next(iter(result)), int)

    def test_get_max_columns(self):
        result = [etl.get_max_columns(i) for i in self.table_names]
        self.assertIsInstance(next(iter(result)), int)


class TestLand(unittest.TestCase):
    def test_land(self):
        result = subprocess.call("hcad-land.sh")
        self.assertIs(result, 0)


class TestProcessZip(unittest.TestCase):
    def setUp(self):
        self.sample_zip_file = next(Path("tests/samples/zip").rglob("*.zip"))

    def test_process_zip(self):
        result = etl.process_zip(self.sample_zip_file)
        self.assertIsInstance(result, Generator)
        self.assertIsInstance(next(result), Path)


class TestProcessTxt(unittest.TestCase):
    def setUp(self):
        self.sample_txt_file = next(
            Path("tests/samples/txt").rglob("arb_protest_real.txt")
        )

    def test_process_txt(self):
        result = etl.process_txt(self.sample_txt_file)
        self.assertIsInstance(next(result), Path)


if __name__ == "__main__":
    unittest.main()
