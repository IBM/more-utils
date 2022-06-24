"""
Conf test file for persistence module
"""

from datetime import datetime
import socket
import pytest


DEFAULT_PORT_NUMBER = 9999


@pytest.fixture(scope="module", autouse=True)
def create_socket():
    """Mock socket connection with ModelarDB"""
    db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    db_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    db_socket.bind(("localhost", DEFAULT_PORT_NUMBER))
    db_socket.listen()


@pytest.fixture(scope="function")
def data_points():
    ts_data = [
        (1, parse_ts("1990-05-01 12:00:00.0"), 0.37),
        (2, parse_ts("1990-05-01 12:00:00.0"), 0.55),
        (3, parse_ts("1990-05-01 12:00:00.0"), 0.73),
    ]
    return (data for data in ts_data)


def parse_ts(timestamp: str):
    """Parse timestamps from str to datetime.datetime objects."""
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
