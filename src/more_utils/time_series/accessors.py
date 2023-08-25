"""DataFrame Type accessor class"""

import json
import pprint
from typing import List
import pandas as pd
import pyspark.sql as spark
from more_utils.logging import configure_logger


LOGGER = configure_logger(logger_name="Timeseries")


class BaseAccessor(object):
    """Base accessor class"""

    def __init__(self) -> None:
        pass

    def create_data(self, data):
        """wraps time series data in a list

        Args:
            data Union[List, tuple]: time series data

        Returns:
            List: List of data
        """
        if isinstance(data, tuple):
            data = [data]
        return data


class JsonAccessor(BaseAccessor):
    """Return accessor to output time series data as a JSON String"""

    def to_json(self, columns: List[str], data: List[tuple]) -> str:
        """Create timeseries in JSON String

        Args:
            columns (str): List of column labels
            data (List[tuple]): List of time series tuples

        Returns:
            str: time series in JSON string
        """
        return json.dumps(
            {"columns": columns, "data": self.create_data(data)},
            indent=4,
            sort_keys=True,
            default=str,
        )


class PandasAccessor(BaseAccessor):
    """Return accessor to output time series data as a Pandas dataframe"""

    def to_pandas(self, columns: List[str], data: List[tuple]) -> pd.DataFrame:
        """Create timeseries in Pandas dataframe

        Args:
            columns (str): List of column labels
            data (List[tuple]): List of time series tuples

        Returns:
            pd.DataFrame: time series in Pandas dataframe
        """
        return pd.DataFrame(data=self.create_data(data), columns=columns)


class PySparkAccessor(BaseAccessor):
    """Return accessor to output time series data as a PySpark dataframe"""

    def to_spark(self, columns: List[str], data: List[tuple]) -> spark.DataFrame:
        """Create timeseries in Spark dataframe

        Args:
            columns (str): List of column labels
            data (List[tuple]): List of time series tuples

        Returns:
            spark.DataFrame: time series in Spark dataframe
        """
        session = spark.SparkSession.builder.getOrCreate()
        return session.createDataFrame(data=self.create_data(data), schema=columns)
