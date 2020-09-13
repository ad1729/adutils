import unittest
import pandas as pd
from pyspark.sql import SparkSession

from adutils.missing_values import has_missing


class PandasDFTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.pd_df_with_missing = pd.DataFrame({
            "x1": [2, 3, 4, None],
            "x2": [None, "a", "bcd", "str4"]
        })

        self.pd_df_without_missing = pd.DataFrame({
            "x1": [2, 3, 4, 5],
            "x2": ["xyz", "a", "bcd", "str4"]
        })

    def test_pandas_df_with_missing(self) -> None:
        self.assertEqual(has_missing(self.pd_df_with_missing), True)

    def test_pandas_df_without_missing(self) -> None:
        self.assertEqual(has_missing(self.pd_df_without_missing), False)

    def test_pandas_df_missing_rows(self) -> None:
        missing_df = pd.DataFrame({
            "x1": [2, None],
            "x2": [None, "str4"]
        })
        output_df = has_missing(self.pd_df_with_missing, return_missing=True).reset_index(drop=True)
        self.assertEqual(missing_df.equals(output_df), True)


class SparkDFTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

        self.spark_df_with_missing = self.spark.createDataFrame(
            pd.DataFrame({
                "x1": [2, 3, 4, None],
                "x2": [None, "a", "bcd", "str4"]
            })
        )

        self.spark_df_without_missing = self.spark.createDataFrame(
            pd.DataFrame({
                "x1": [2, 3, 4, 5],
                "x2": ["xyz", "a", "bcd", "str4"]
            })
        )

    def test_spark_df_with_missing(self) -> None:
        self.assertEqual(has_missing(self.spark_df_with_missing), True)

    def test_spark_df_without_missing(self) -> None:
        self.assertEqual(has_missing(self.spark_df_without_missing), False)

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
