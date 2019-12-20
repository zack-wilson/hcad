import unittest

from hcad.etl import main, services, settings
from hcad.etl.jobs import land, stage


class TestServices(unittest.TestCase):
    def test_http(self):
        result = services.http.head("https://www.example.com")
        self.assertTrue(result)


class TestJobsLand(unittest.TestCase):
    def test_run(self):
        result = land.run()
        self.assertTrue(result)
