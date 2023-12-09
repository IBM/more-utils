import pymodelardb as pymodelardb
from .base import AbstractDBLayer, AbstractDBSession
from typing import Literal
from more_utils.persistence.arrow import ArrowCursor
from more_utils.logging import configure_logger
LOGGER = configure_logger(logger_name="ModelarDB")
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

    def execute_action(self, *args, **kwargs):
        self._cursor.execute_action(*args, **kwargs)

    def insert(self, *args, **kwargs):
        self._cursor.insert(*args, **kwargs)

    def list(self):
        return self._cursor.list()

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

    def __init__(self, manager_conn, edge_conn, cloud_conn) -> None:
        super(ModelarDB, self).__init__()
        self._manager_conn = manager_conn
        self._edge_conn = edge_conn
        self._cloud_conn = cloud_conn

    @classmethod
    def connect(
        cls,
        hostname: str = "localhost",
        manager_port: int = "9998",
        edge_port: int = "9999",
        cloud_port: int = "9997",
        interface: Literal["arrow", "http", "socket"] = "arrow",
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
        manager_conn = pymodelardb.connect(
            host=hostname, interface=interface, port=manager_port
        )
        edge_conn = pymodelardb.connect(
            host=hostname, interface=interface, port=edge_port
        )
        cloud_conn = pymodelardb.connect(
            host=hostname, interface=interface, port=cloud_port
        )

        return ModelarDB(manager_conn, edge_conn, cloud_conn)

    def create_session(
        self, conn_type: Literal["manager", "edge", "cloud"]
    ) -> ModelarDBSession:
        """Open a cursor with the ModelarDB connection.

        Returns:
            ModelarDBSession: An object of the type ModelarDBSession.
                              It holds a cursor with the ModelarDB.
        """

        if "manager" == conn_type:
            return ModelarDBSession(self._manager_conn.cursor())
        elif "edge" == conn_type:
            return ModelarDBSession(self._edge_conn.cursor())
        elif "cloud" == conn_type:
            return ModelarDBSession(self._cloud_conn.cursor())
        else:
            raise Exception(
                "Invalid ModelarDB connection type. Valid valies ['manager', 'edge', 'cloud']"
            )

    def create_arrow_session(self, conn_type) -> ModelarDBSession:
        """Open a cursor with the ModelarDB connection.

        Returns:
            ModelarDBSession: An object of the type ModelarDBSession.
                              It holds a cursor with the ModelarDB.
        """
        if "manager" == conn_type:
            conn = self._manager_conn
        elif "edge" == conn_type:
            conn = self._edge_conn
        elif "cloud" == conn_type:
            conn = self._cloud_conn
        else:
            raise Exception(
                "Invalid ModelarDB connection type. Valid valies ['manager', 'edge', 'cloud']"
            )

        return ModelarDBSession(
            ArrowCursor(
                conn,
                conn._Connection__host,
                conn._Connection__port,
            )
        )

    def flush(self, mode: Literal["FlushMemory", "FlushEdge"] = "FlushEdge"):
        # Flush data to disk or object store.
        with self.create_arrow_session(conn_type="edge") as session:
            if mode == "FlushMemory":
                session.execute_action("FlushMemory", b"")
            elif mode == "FlushEdge":
                session.execute_action("FlushEdge", b"")

        LOGGER.info(f"{mode}: Compressed data buffers flushed.")

    def list_tables(self):
        with self.create_arrow_session(conn_type="manager") as session:
            tables = session.list()
            return [table.decode("UTF-8") for table in list(tables)[0]]

    def close(self):
        """Mark the connection as closed."""
        self._manager_conn.close()
        self._edge_conn.close()
        self._cloud_conn.close()
