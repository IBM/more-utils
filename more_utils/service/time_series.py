"""TimeSeries service class"""

import itertools
from typing import Dict, Union, List
from uuid import uuid1
from numpy import int64
import pandas as pd
import copy
from more_utils.util.logging import configure_logger
from more_utils.persistence.base import AbstractDBLayer
import more_utils.persistence.cassandradb as cassandradb
from .accessors import JsonAccessor, PandasAccessor, PySparkAccessor
from .queries import safe_substitute, safe_substitute_v2


logger = configure_logger()
TIME_SERIES_ID_LABEL = "TID"
DEFAULT_VALUE_LABEL = "VALUE"
TIMESTAMP_LABEL = "TIMESTAMP"


class TimeSeries(JsonAccessor, PandasAccessor, PySparkAccessor):
    """[summary]
    A Time-Series placeholder class that holds time series data. The class
    instance does not store any active DB session. The class is a sink for
    time series data fetched from DB. Once instantiates, this class provides
    APIs to retrieve data wholly or a batch at a time.

    Args:
        result_generators (List): A list of result set generator per
                                    timeseries.
        merge_on (Union[str, None]): common field to merge multiple timeseries.
    """

    def __init__(
        self,
        result_generators: List,
        columns: Union[None, List[str]] = [],
        merge_on: Union[str, None] = None,
    ) -> None:
        super(TimeSeries, self).__init__()
        self._result_generators = result_generators
        self._merge_on = merge_on
        self._columns = columns
        self._result_set = []

    def __len__(self) -> int:
        """no. of rows in current time series"""
        return len(self._result_set)

    # def __str__(self) -> str:
    #     """Pass time series object to COS"""
    #     return super().__str__()

    @property
    def columns(self) -> List[str]:
        """Return column labels

        Returns:
            List[str]: List of column labels
        """
        return self._columns

    @property
    def result_set(self) -> List[tuple]:
        """Return current resultset

        Returns:
            List[tuple]: current resultset
        """
        return self._result_set

    def count(self) -> int:
        """Return length of current resultset

        Returns:
            int: length of current resultset
        """
        return len(self._result_set)

    def fetch_next(self, fetch_type: str = "pandas", batch_size: int = 1):
        """Return time-series data batch_size at a time.

        Args:
            fetch_type (str, optional): Return time series data in
                                        [pandas, json, spark] dataframe.
                                        Defaults to "pandas".
            batch_size (int, optional): size of the time series batch.
                                        Defaults to 1.

        Returns:
            [TimeSeries]: A dataframe containing time series data.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        return self._create_ts_generator(fetch_type, batch_size)

    def fetch_all(self, fetch_type="pandas"):
        """Return entire time-series data.

        Args:
            fetch_type (str, optional): Return time series data in
                                        [pandas, json, spark] dataframe.
                                        Defaults to "pandas".

        Returns:
            [TimeSeries]: A dataframe containing time series data.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        if self._result_set:
            method = getattr(self, "to_" + fetch_type)
            return method(columns=self._columns, data=self._result_set)
        return next(self._create_ts_generator(fetch_type))

    def _create_ts_generator(self, fetch_type: str, batch_size=None):
        """Create time series generator from `self._result_generators`

        Args:
            fetch_type (str): Return time series data in
                                        [pandas, json, spark] dataframe.
                                        Defaults to "pandas".
            batch_size (int, optional): size of the time series batch.
                                        Defaults to None.

        Yields:
            Generator: time series generator
        """
        while True:
            ts_data_args = []
            for ts_columns, ts_data_gen in self._result_generators:
                ts_data = list(
                    itertools.islice(ts_data_gen, batch_size)
                    if batch_size
                    else ts_data_gen
                )
                if ts_data:
                    ts_data_args.append((ts_columns, ts_data))

            if not ts_data_args:
                break

            method = getattr(self, "to_" + fetch_type)
            if len(ts_data_args) == 1:
                self._columns = ts_data_args[0][0]
                self._result_set = ts_data_args[0][1]
            else:
                self._columns, self._result_set = self._merge_time_series(
                    ts_data_args, self._merge_on
                )

            yield method(columns=self._columns, data=self._result_set)

    def _merge_time_series(
        self, data_args: List[tuple], merge_on: Union[str, None] = None
    ):
        """Merge multiple timeseries using the merge_on field.

        if merge_on is present, merge on the given field.
        if merge_on is None, stack time series vertically on axis = 0.

        Args:
            data_args (List[tuple]): List of tuples having columns and data
            merge_on (str): common field to merge multiple timeseries.

        Returns:
            List[str], List[tuple]: List of column labels, merged time series.
        """
        master_df = pd.DataFrame()
        for ts_columns, ts_data in data_args:
            current_df = self.to_pandas(columns=ts_columns, data=ts_data)
            if master_df.empty:
                master_df = current_df
            elif merge_on:
                current_df = current_df.drop(
                    labels=TIME_SERIES_ID_LABEL, axis=1
                )
                if TIME_SERIES_ID_LABEL in master_df.columns:
                    master_df = master_df.drop(
                        labels=TIME_SERIES_ID_LABEL, axis=1
                    )
                master_df = pd.merge(
                    master_df, current_df, on=merge_on, how="outer"
                )
            else:
                master_df = pd.concat([master_df, current_df])

        return list(master_df.columns), list(
            master_df.itertuples(index=False, name=None)
        )


class BaseService:
    """Base class for Time Series Service."""

    def __init__(self, source_db_conn: AbstractDBLayer=None, sink_db_conn: AbstractDBLayer=None) -> None:
        self.source_db_conn = source_db_conn
        self.sink_db_conn = sink_db_conn

    def execute(
        self,
        query_params: Dict[str, Union[str, int]],
        value_column_label: Union[str, None] = None,
    ):
        """Execute given query params on the source DB.

        Args:
            query_params (Dict[str, Union[str, int]]): query params to
                                                       create a query.
            value_column_label (Union[str, None], optional): Label to replace
                                                            value column.
                                                            Defaults to None.

        Returns:
            Tuple[List[str], Generator]: Tuple of columns and result set
                                         generator
        """
        with self.source_db_conn.create_session() as session:
            query = safe_substitute(query_params)
            logger.debug(query)
            session.execute(query)
            if not session.columns:
                raise ValueError("NULL RESPONSE FROM SERVER.")
            columns = [
                value_column_label
                if value_column_label and value[0] == DEFAULT_VALUE_LABEL
                else value[0]
                for value in session.columns
            ]
            return (columns, session.result_set)

    def execute_v2(
        self,
        query_params: Dict[str, Union[str, int]]
    ):
        """Execute given query params on the source DB.

        Args:
            query_params (Dict[str, Union[str, int]]): query params to
                                                       create a query.
            value_column_label (Union[str, None], optional): Label to replace
                                                            value column.
                                                            Defaults to None.

        Returns:
            Tuple[List[str], Generator]: Tuple of columns and result set
                                         generator
        """
        with self.source_db_conn.create_session() as session:
            query = safe_substitute_v2(query_params)
            logger.debug(query)
            session.execute(query)
            if not session.columns:
                raise ValueError("NULL RESPONSE FROM SERVER.")
            columns = [value[0]
                for value in session.columns
            ]
            return (columns, session.result_set)

class TimeseriesService(BaseService):
    """[summary]
    Time Series Service has all the APIs to retrieve time series data from the
    remote database. The service uses a database connection to create one DB
    session per API service call. The database session is closed once the API
    call exits. The service recreates sessions for every subsequent API call.

    Args:
        db_conn (AbstractDBLayer): database connection object to create DB
                                   sessions.
    """

    def get_time_series(
        self,
        model_table: str,
        from_date: Union[str, None] = None,
        to_date: Union[str, None] = None,
        limit: Union[int, None] = None,
    ) -> TimeSeries:
        """Fetch time-series data points for time series ids in `ts_ids`.

        Args:
            model_table (str): time series model_table.
            from_date (Union[str, None], optional): Start timestamp.
                                                    Defaults to None.
            to_date (Union[str, None], optional): End timestamp.
                                                  Defaults to None.
            limit (Union[int, None], optional): No of data points to fetch.
                                                Defaults to None.

        Returns:
            TimeSeries: A time-series placeholder class containing time series.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        assert isinstance(model_table, str), "Time Series model_table must be a str."

        result_generators = []
        query_params = {
            "MODEL_TABLE": model_table,
            "START_TIME_COLUMN": "datetime",
            "END_TIME_COLUMN": "datetime",
            "START_TIME": from_date,
            "END_TIME": to_date,
            "LIMIT": limit,
        }
        generator = self.execute_v2(query_params)
        result_generators.append(generator)

        return TimeSeries(result_generators=result_generators, columns=generator[0])
    
    def get_time_series_data_from_ts_ids(
        self,
        ts_ids: List[int],
        from_date: Union[str, None] = None,
        to_date: Union[str, None] = None,
        merge_on: str = TIMESTAMP_LABEL,
        value_column_labels: Union[List[str], None] = None,
        limit: Union[int, None] = None,
    ) -> TimeSeries:
        """Fetch time-series data points for time series ids in `ts_ids`.

        Args:
            ts_ids (List[int]): time series id(s).
            from_date (Union[str, None], optional): Start timestamp.
                                                    Defaults to None.
            to_date (Union[str, None], optional): End timestamp.
                                                  Defaults to None.
            merge_on (str, optional): common field to merge multiple
                                      timeseries. Defaults to TIMESTAMP_LABEL.
            value_column_labels (Union[List[str], None], optional): List of
            string to replace value column labels. Defaults to None.
            limit (Union[int, None], optional): No of data points to fetch.
                                                Defaults to None.

        Returns:
            TimeSeries: A time-series placeholder class containing time series.

        Raises:
            ValueError: if any param is not a valid argument.
        """

        assert isinstance(ts_ids, list) and all(
            isinstance(ts_id, int) for ts_id in ts_ids
        ), "Time Series Id (ts_ids) must be a list of int."

        if value_column_labels is not None:
            assert len(ts_ids) == len(
                value_column_labels
            ), "ts_ids and value_column_labels must be equal in length."
            assert isinstance(value_column_labels, list) and isinstance(
                value_column_labels[0], str
            ), (
                "value_column_labels must be a list of string. "
                "Pass the argument as None to use default column label."
            )

        result_generators = []
        for index, ts_id in enumerate(ts_ids):
            query_params = {
                "SCHEMA": "DataPoint",
                "TS_ID": ts_id,
                "START_TIME_COLUMN": "TIMESTAMP",
                "END_TIME_COLUMN": "TIMESTAMP",
                "START_TIME": from_date,
                "END_TIME": to_date,
                "LIMIT": limit,
            }

            if value_column_labels:
                value_column_label = value_column_labels[index]
            else:
                value_column_label = DEFAULT_VALUE_LABEL + "_" + str(ts_id)

            generator = self.execute(query_params, value_column_label)
            result_generators.append(generator)

        return TimeSeries(result_generators, merge_on)

    def get_time_series_data_models_from_ts_ids(
        self,
        ts_ids: List[int],
        from_date: Union[str, None] = None,
        to_date: Union[str, None] = None,
        limit: Union[int, None] = None,
    ) -> TimeSeries:
        """Fetch time-series data models for the given time series `ts_ids`.

        Args:
            ts_ids (List[int]): time series id(s).
            from_date (Union[str, None], optional): Start timestamp.
                                                    Defaults to None.
            to_date (Union[str, None], optional): End timestamp.
                                                  Defaults to None.
            limit (Union[int, None], optional): No of data points to fetch.
                                                Defaults to None.

        Returns:
            TimeSeries: A time-series placeholder class containing time series.

        Raises:
            ValueError: if any param is not a valid argument.
        """

        assert isinstance(ts_ids, list) and all(
            isinstance(ts_id, int) for ts_id in ts_ids
        ), "Time Series Id (ts_ids) must be a list of int."

        result_generators = []
        for ts_id in ts_ids:
            query_params = {
                "SCHEMA": "Segment",
                "TS_ID": ts_id,
                "START_TIME_COLUMN": "START_TIME",
                "END_TIME_COLUMN": "END_TIME",
                "START_TIME": from_date,
                "END_TIME": to_date,
                "LIMIT": limit,
            }
            generator = self.execute(query_params)
            result_generators.append(generator)

        return TimeSeries(result_generators)
    
    def store_time_series(self, df:pd.DataFrame, namespace:str=None)->uuid1:
        """Store time series data into Cassandra cluster.

        Args:
            df (pd.DataFrame): Input DataFrame
            namespace (str, optional): Namespace to insert the table to. Defaults to None.

        Returns:
            uuid1: The uuid1 time-series id.
        """        
        ts_entity = cassandradb.create_timeseries_entity(df)
        with self.sink_db_conn.create_session() as session:
            rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='"+ts_entity.__keyspace__+"';")
            tables = [row["table_name"] for row in rows.all()]
            if not ts_entity.__table_name__ in tables:
                session.create_schema(ts_entity)
            time_series_id = session.insert(df, ts_entity)

        return time_series_id