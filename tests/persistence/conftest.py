"""
Conf test file for persistence module
"""

from datetime import datetime
import socket
import pytest
from more_utils.persistence import ModelarDB

@pytest.fixture(autouse=True)
def create_manager_socket():
    """Mock socket connection with ModelarDB"""
    db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    db_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    db_socket.bind(("localhost", 9998))
    db_socket.listen()
    yield "Manager socket shutdown..."
    db_socket.close()

@pytest.fixture( autouse=True)
def create_edge_socket():
    """Mock socket connection with ModelarDB"""
    db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    db_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    db_socket.bind(("localhost", 9999))
    db_socket.listen()
    yield "Edge socket shutdown..."
    db_socket.close()

@pytest.fixture(autouse=True)
def create_cloud_socket():
    """Mock socket connection with ModelarDB"""
    db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    db_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    db_socket.bind(("localhost", 9997))
    db_socket.listen()
    yield "cloud socket shutdown..."
    db_socket.close()

@pytest.fixture(scope="module", autouse=True)
def modelardb_conn():
    conn_obj = ModelarDB.connect(hostname="localhost", manager_port=9998, edge_port=9999, cloud_port=9997, interface="arrow")
    return conn_obj

@pytest.fixture(scope="function")
def data_points():
    ts_data = [
        (parse_ts("1990-05-01 12:00:00.0"), 195.240982, 0.37),
        (parse_ts("1990-05-01 12:00:00.0"), 196.890507, 0.55),
        (parse_ts("1990-05-01 12:00:00.0"), 198.895595, 0.73),
    ]
    return (data for data in ts_data)

@pytest.fixture(scope="function")
def columns():
    return ["datetime", "active_power", "wind_speed"]

def parse_ts(timestamp: str):
    """Parse timestamps from str to datetime.datetime objects."""
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
