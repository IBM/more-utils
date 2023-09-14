from typing import Any


import pyarrow
from pyarrow import flight
from pyarrow.lib import ArrowException
from pyarrow._flight import FlightUnavailableError

from pymodelardb.connection import Connection
from pymodelardb.types import ProgrammingError
from pymodelardb.types import TypeOf


class Cursor(object):
    """Represents a single connection to ModelarDB.

    Arguments:

     :param connection: the Connection object that created the Cursor.
    """

    def __init__(self, connection: Connection):
        self._connection = connection

    def close(self):
        """Mark the cursor as closed."""
        self._is_closed("cannot close the cursor as it is already closed")
        self._connection = None

    def _is_closed(self, message: str):
        """Check if the cursor or connection have been closed."""
        if not self._connection:
            raise ProgrammingError(message)


class ArrowCursor(Cursor):
    def __init__(self, connection: Connection, host: str, port: int):
        Cursor.__init__(self, connection)
        self.__uri = "grpc://" + host + ":" + str(port)
        self.__client = flight.FlightClient(self.__uri)

    def list(self):
        """Execute operation after adding the parameters."""
        self._is_closed("cannot execute action as the cursor is closed")
        try:
            tables = map(
                lambda flight: flight.descriptor.path, self.__client.list_flights()
            )
            return tables
        except FlightUnavailableError:
            raise ProgrammingError("unable to connect to: " + self.__uri) from None
        except ArrowException as ae:
            error = ae.args[0]
            start_of_error = error.find("{") + 1
            end_of_error = error.rfind("}")
            error = error[start_of_error:end_of_error]
            message = "unable to execute query due to: " + error
            raise ProgrammingError(message) from None

    def insert(self, table_name, arrow_table):
        """Execute operation after adding the parameters."""
        self._is_closed("cannot execute action as the cursor is closed")
        upload_descriptor = flight.FlightDescriptor.for_path(table_name)
        try:
            writer, _ = self.__client.do_put(upload_descriptor, arrow_table.schema)
            writer.write(arrow_table)
            writer.close()
        except FlightUnavailableError:
            raise ProgrammingError("unable to connect to: " + self.__uri) from None
        except ArrowException as ae:
            error = ae.args[0]
            start_of_error = error.find("{") + 1
            end_of_error = error.rfind("}")
            error = error[start_of_error:end_of_error]
            message = "unable to execute query due to: " + error
            raise ProgrammingError(message) from None

    def execute_action(self, action: str, params: Any = None):
        """Execute operation after adding the parameters."""
        self._is_closed("cannot execute action as the cursor is closed")
        action = flight.Action(action, params)
        try:
            response = self.__client.do_action(action)
            out = list(response)
            if len(out) > 1:
                print(out)
        except FlightUnavailableError:
            raise ProgrammingError("unable to connect to: " + self.__uri) from None
        except ArrowException as ae:
            error = ae.args[0]
            start_of_error = error.find("{") + 1
            end_of_error = error.rfind("}")
            error = error[start_of_error:end_of_error]
            message = "unable to execute query due to: " + error
            raise ProgrammingError(message) from None
