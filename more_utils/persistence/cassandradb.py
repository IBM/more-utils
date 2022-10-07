from uuid import uuid1
from typing import List
import pandas as pd
from pandas.api.types import is_string_dtype, is_float_dtype, is_int64_dtype, is_datetime64_dtype

from cassandra.cluster import Cluster, Session
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.connection import register_connection
from cassandra.policies import WhiteListRoundRobinPolicy

from more_utils.persistence.base import AbstractDBLayer, AbstractDBSession

import os
os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "CQLENG_ALLOW_SCHEMA_MANAGEMENT"

DEFAULT_CONNECTION_NAME = "default"
DEFAULT_KEYSPACE = "moreutils"


default_entity_params = {
    "__keyspace__" : DEFAULT_KEYSPACE,
    "__connection__" : DEFAULT_CONNECTION_NAME,
    "__table_name__" : "TIME_SERIES",
    "__table_name_case_sensitive__" : False,
    "time_series_id" : columns.TimeUUID(primary_key=True, default=uuid1)
}


def create_timeseries_entity(df:pd.DataFrame):
    """Creae Entity class from the DataFrame column types.

    Args:
        df (pd.DataFrame): Source DataFrame`

    Returns:
        Model: DataBase Entity class.
    """
    column_params = {}
    for col in df.columns:
        if is_datetime64_dtype(df[col]):
            column_params["ts_"+col] = columns.DateTime(primary_key=True, clustering_order="ASC")
        elif is_string_dtype(df[col]):
            column_params[col] = columns.Text()
        elif is_int64_dtype(df[col]):
            column_params[col] = columns.Integer()
        elif is_float_dtype(df[col]):
            column_params[col] = columns.Double()
        else:
            raise ValueError("Invalid Column dtype: %s", df[col].dtype)

    column_params.update(default_entity_params)
    return ClassFactory("TimeSeriesEntity", column_params, Model)


def ClassFactory(name, argnames, BaseClass):
    entity_class = type(name, (BaseClass,), argnames)
    return entity_class


class CassandraDBSession(AbstractDBSession):
    """
    Class that holds a cursor with the CassandraDB connection.

    Args:
        name (str): name of the DB connection.
        cursor (Session): cursor to use with the CassandraDB connection.
    """

    def __init__(self, name:str, cursor:Session) -> None:
        super(CassandraDBSession, self).__init__()
        self.name = name
        self._cursor = cursor
        register_connection(name=name, session=cursor)
        # set_default_connection(name=name)

    def __enter__(self):
        return self

    def execute(self, query):
        """Execute given query on the active cursor.

        Arguments:
            query [str]: Query string to be executed on the active cursor.

        Returns:
            [int]: Returns result_set if the execution is successful otherwise None.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        try:
            result_set = self._cursor.execute(query)
            return result_set
        except Exception as error:
            print("Exception: ", str(error))
            return None
    
    def insert(self, df:pd.DataFrame, ts_entity:Model, batch_size=750)->uuid1:
        """Insert time-series data into the database

        Args:
            df (pd.DataFrame): Input DataFrame
            ts_entity (Model): TimeSeries database entity
            batch_size (int, optional): batch size of the CassandraDB BatchQuery. Defaults to 750.
        
        Returns:
            uuid1: The uuid1 time-series id.
        """      
        query = BatchQuery()
        for index, row in enumerate(df.to_dict('records')):
            params = {}
            for key,value in row.items():
                key = "ts_timestamp" if key == "timestamp" else key
                params.update({
                    key:value
                }) 
            entity = ts_entity.batch(query).create(**params)
            if index!=0 and index%batch_size==0:
                query.execute()

        return entity.time_series_id
    
    def create_schema(self, entity:Model):
        """Create schema using the given entity.

        Args:
            entity (Model): TimeSeries database entity
        """        
        sync_table(entity)

    def __exit__(self, *args):
        return self.close()

    def close(self):
        """Mark the cursor as closed."""
        self._cursor.shutdown()


class CassandraDB(AbstractDBLayer):
    """
    A class that holds a connection to the CassandraDB. It exposes
    APIs to create connection and session with the CassandraDB.

    Arguments:
        db_conn -- Connection object that opens the session to the CassandraDB.
    """

    def __init__(self, name, keyspace, db_conn) -> None:
        super(CassandraDB, self).__init__()
        self._name = name
        self._keyspace = keyspace
        self._db_conn = db_conn

    @classmethod
    def connect(
        cls,
        contact_points:List[str]=["localhost"],
        port:int=9042,
        name:str=DEFAULT_CONNECTION_NAME, 
        keyspace:str=DEFAULT_KEYSPACE,
        protocol_version:int=5
    ):
        """Establish a connection to CassandraDB

        Args:
            contact_points (List[str], optional): Hostnames of the cluster nodes.
                                                  Defaults to ["localhost"].
            port (int, optional): Port number of the connection. Defaults to 9042.
            name (str, optional): Name of the DB connection. Defaults to DEFAULT_CONNECTION_NAME.
            keyspace (str, optional): Namespace for creating tables in the DB. 
                                      Defaults to DEFAULT_KEYSPACE.
            protocol_version (int, optional): Protocol version of the Cassandra cluster. Defaults to 5.

        Returns:
            CassandraDB: Object of the type CassandraDB.
                         It holds a connection with the CassandraDB.
        
        Raises:
            ValueError: if any param is not a valid argument.
        """

        cluster = Cluster(contact_points=contact_points, port=port, load_balancing_policy=WhiteListRoundRobinPolicy(contact_points), protocol_version=protocol_version)
        return CassandraDB(name, keyspace, cluster)

    def create_session(self) -> CassandraDBSession:
        """Open a cursor with the CassandraDB connection.

        Returns:
            CassandraDBSession: An object of the type CassandraDBSession.
                              It holds a cursor with the CassandraDB.
        """
        if self._keyspace:
            session = self._db_conn.connect(self._keyspace)
        else:
            session = self._db_conn.connect()
        
        return CassandraDBSession(self._name, session)

    def close(self):
        """Mark the connection as closed."""
        self._db_conn.shutdown()

