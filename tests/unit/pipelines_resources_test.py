import os
import shutil
import unittest

import pyarrow as pa
from dagster import (
    AssetKey,
    PythonObjectDagsterType,
    build_input_context,
    build_output_context,
)
from upath import UPath

from duck_f1.pipelines.resources.file_system_io_manager import ArrowParquetIOManager


class TestArrowParquetIOManagerUtilities(unittest.TestCase):
    def test_size_label(self) -> None:
        self.assertEqual(ArrowParquetIOManager._compute_size_label(1234, 2), "1.23 KB")
        self.assertEqual(
            ArrowParquetIOManager._compute_size_label(56789000, 1),
            "56.8 MB",
        )


class TestArrowParquetIOManagerOutput(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.io_manager = ArrowParquetIOManager(base_path=UPath("./tmp"))
        data = [{"col_a": 1, "col_b": 2}, {"col_a": 3, "col_b": 4}]
        cls.data_table = pa.Table.from_pylist(data)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists("./tmp/test/data.parquet"):
            shutil.rmtree("./tmp")

    def test_arrow_parquet_io_manager_output(self) -> None:
        context = build_output_context(
            name="test",
            step_key="test_01",
            asset_key=AssetKey(path=["test", "data"]),
            dagster_type=PythonObjectDagsterType(pa.Table),
        )

        self.io_manager.handle_output(context, self.data_table)
        self.assertTrue(os.path.exists("./tmp/test/data.parquet"))
        self.assertTrue("table_size" in context._user_generated_metadata)
        self.assertTrue("table_size_label" in context._user_generated_metadata)
        self.assertTrue("col_count" in context._user_generated_metadata)
        self.assertTrue("row_count" in context._user_generated_metadata)


class TestArrowParquetIOManagerInput(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.io_manager = ArrowParquetIOManager(base_path=UPath("./tmp"))

        data = [{"col_a": 1, "col_b": 2}, {"col_a": 3, "col_b": 4}]
        cls.data_table = pa.Table.from_pylist(data)

        context = build_output_context(
            name="test_a",
            step_key="test_01",
            asset_key=AssetKey(path=["test", "data"]),
            dagster_type=PythonObjectDagsterType(pa.Table),
        )

        cls.io_manager.handle_output(context, cls.data_table)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists("./tmp/test/data.parquet"):
            shutil.rmtree("./tmp")

    def test_arrow_parquet_io_manager_input(self) -> None:
        context = build_input_context(
            name="test_b",
            asset_key=AssetKey(path=["test", "data"]),
            dagster_type=PythonObjectDagsterType(pa.Table),
        )

        data = self.io_manager.load_input(context)
        self.assertEqual(data, self.data_table)
