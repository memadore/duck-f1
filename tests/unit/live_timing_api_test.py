import unittest
from functools import partial
from unittest.mock import MagicMock

from duck_f1.pipelines.assets.live_timing.api import LiveTimingApi


class TestLiveTimingApiFileBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api_client = LiveTimingApi(MagicMock())

    def test_file_processor_builder_json(self):
        file = {
            "file_name": "test.json",
            "expected_functions": self.api_client._json_processor,
        }
        processor = self.api_client._file_processor_builder(file["file_name"])
        self.assertEqual(processor, file["expected_functions"])

    def test_file_processor_builder_json_stream(self):
        file = {
            "file_name": "test.jsonStream",
            "expected_functions": self.api_client._json_stream_processor,
        }
        processor = self.api_client._file_processor_builder(file["file_name"])
        self.assertEqual(processor, file["expected_functions"])

    def test_file_processor_builder_compressed_json_stream(self):
        file = {
            "file_name": "test.z.jsonStream",
            "expected_functions": partial(
                self.api_client._json_stream_processor,
                data_post_process=self.api_client._zlib_decompress,
            ),
        }
        processor = self.api_client._file_processor_builder(file["file_name"])
        self.assertEqual(processor.args, file["expected_functions"].args)
        self.assertEqual(processor.keywords, file["expected_functions"].keywords)
