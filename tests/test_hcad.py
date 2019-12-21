import unittest
import os

import hcad


class TestLand(unittest.TestCase):
    def setUp(self):
        self.landing = os.path.abspath("data/hcad/landing")

    def test_paths(self):
        self.assertTrue(os.path.exists(self.landing))


class TestStage(unittest.TestCase):
    def setUp(self):
        self.staging = os.path.abspath("data/hcad/staging")
    def test_paths(self):
        self.assertTrue(os.path.exists(self.staging))



if __name__ == "__main__":
    unittest.main()
