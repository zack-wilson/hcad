import gzip
import io
import shutil
import subprocess
import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path
from typing import Generator, TextIO

from hcad import etl


class TestFunctions(unittest.TestCase):
    def setUp(self):
        self.table_names = set(i[0] for i in etl.dictionary)

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


class TestProcessZip(unittest.TestCase):
    def setUp(self):
        self.sample_zip_file = next(Path("tests/samples").rglob("*.zip"))

    def test_decompress_zip(self):
        result = etl.decompress_zip(self.sample_zip_file)
        self.assertIsInstance(result, Generator)
        self.assertIsInstance(next(result), Path)


class TestProcessGzip(unittest.TestCase):
    def setUp(self):
        self.files = Path("tests/samples").rglob("*.gz")

    def test_decompress_gzip(self):
        result = etl.decompress_gzip(*self.files)
        self.assertTrue(result)
        self.assertIsInstance(next(result), Path)


class TestProcessTxt(unittest.TestCase):
    def setUp(self):
        self.samples = Path("tests/samples")
        self.tmp = Path(tempfile.mkdtemp(dir=self.samples))
        self.sample_text_files = []
        for file in self.samples.joinpath("txt").rglob("*.gz"):
            with gzip.open(file) as f_in:
                with self.tmp.joinpath(file.stem).with_suffix(".txt") as f_out:
                    self.sample_text_files.append(f_out)
                    f_out.write_bytes(f_in.read())

    def test_process_txt(self):
        gen_result = etl.process_txt(*self.sample_text_files)
        result = next(gen_result)
        self.assertIsInstance(gen_result, Generator)
        self.assertIsInstance(result, Path)
        self.assertTrue(result.exists())

    def tearDown(self):
        shutil.rmtree(self.tmp)


if __name__ == "__main__":
    unittest.main()
