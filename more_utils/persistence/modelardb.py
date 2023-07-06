import pymodelardb as pymodelardb
from .base import AbstractDBLayer, AbstractDBSession
from typing import Union


class ModelarDBSession(AbstractDBSession):
    """
    Class that holds a cursor with the ModelarDB connection.

    Args:
        cursor: cursor registered with the ModelarDB connection.
    """

    def __init__(self, cursor) -> None:
        super(ModelarDBSession, self).__init__()
        self._cursor = cursor

    def __enter__(self):
        return self

    @property
    def rowcount(self) -> int:
        """The number of rows returned by the last query."""
        return self._cursor.rowcount

    @property
    def columns(self):
        """A description of the columns in the last query."""
        return self._cursor.description

    @property
    def result_set(self):
        """A description of the columns in the last query."""
        return self._cursor._result_set

    def execute(self, query):
        """Execute given query on the active cursor.

        Arguments:
            query [str]: Query string to be executed on the active cursor.

        Returns:
            [int]: Returns 1 if the execution is successful otherwise 0.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        try:
            self._cursor.execute(query)
            return 1
        except Exception as error:
            print("Exception: ", str(error))
            return 0

    def insert(self, data):
        raise NotImplementedError()
        
    def __exit__(self, *args):
        return self.close()

    def close(self):
        """Mark the cursor as closed."""
        self._cursor.close()


class ModelarDB(AbstractDBLayer):
    """
    A class that holds a connection to the ModelarDB. It exposes
    an API to create a session with the ModelarDB connection.

    Arguments:
        db_conn -- Connection object that opens the session to the ModelarDB.
    """

    def __init__(self, db_conn) -> None:
        super(ModelarDB, self).__init__()
        self._db_conn = db_conn

    @classmethod
    def connect(
        cls,
        conn_string: Union[str, None] = None,
        hostname: str = "localhost",
        interface: str = "arrow",
        port: int = "9999"
    ):
        """Establish a connection to ModelarDB

        Args:
            conn_string (Union[str, None], optional): Connection String.
                                                      Defaults to None.
            hostname (str, optional): Hostname of the connection.
                                      Defaults to "localhost".
            interface (str, optional): Interface type [arrow|socket|http].
                                       Defaults to "arrow".

        Returns:
            ModelarDB: Object of the type ModelarDB.
                         It holds a connection with the ModelarDB.

        Raises:
            ValueError: if any param is not a valid argument.
        """
        if conn_string:
            conn = pymodelardb.connect(dsn=conn_string)
        else:
            conn = pymodelardb.connect(host=hostname, interface=interface, port=port)
        return ModelarDB(conn)

    def create_session(self) -> ModelarDBSession:
        """Open a cursor with the ModelarDB connection.

        Returns:
            ModelarDBSession: An object of the type ModelarDBSession.
                              It holds a cursor with the ModelarDB.
        """
        return ModelarDBSession(self._db_conn.cursor())

    def close(self):
        """Mark the connection as closed."""
        self._db_conn.close()
