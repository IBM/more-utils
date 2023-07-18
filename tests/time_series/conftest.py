"""
Conf test file for service module
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
def data_points_model_table():
    ts_data = [
        (4.79, parse_ts("2019-01-01 00:00:02.0"), 0.37),
        (4.23, parse_ts("2019-01-01 00:00:04.0"), 0.55),
        (3.86, parse_ts("2019-01-01 00:00:06.0"), 0.73),
    ]
    return (["wind_speed", "datetime", "active_power"], (data for data in ts_data))

@pytest.fixture(scope="function")
def data_points_tid_1():
    ts_data = [
        (1, parse_ts("2019-01-01 00:00:02.0"), 0.37),
        (1, parse_ts("2019-01-01 00:00:04.0"), 0.55),
        (1, parse_ts("2019-01-01 00:00:06.0"), 0.73),
    ]
    return (["TID", "TIMESTAMP", "active power"], (data for data in ts_data))


@pytest.fixture(scope="function")
def data_points_tid_2():
    ts_data = [
        (2, parse_ts("2019-01-01 00:00:02.0"), 0.44),
        (2, parse_ts("2019-01-01 00:00:04.0"), 0.55),
        (2, parse_ts("2019-01-01 00:00:06.0"), 0.77),
    ]
    return (["TID", "TIMESTAMP", "rotor speed"], (data for data in ts_data))


@pytest.fixture(scope="function")
def data_points_tid_3():
    ts_data = [
        (3, parse_ts("2019-01-01 00:00:02.0"), 0.24),
        (3, parse_ts("2019-01-01 00:00:04.0"), 0.87),
        (3, parse_ts("2019-01-01 00:00:06.0"), 0.11),
    ]
    return (["TID", "TIMESTAMP", "wind speed"], (data for data in ts_data))


@pytest.fixture(scope="function")
def data_model_columns():
    return [
        "TID",
        "START_TIME",
        "END_TIME",
        "MTID",
        "MODEL",
        "OFFSETS",
    ]


@pytest.fixture(scope="function")
def data_models_tid_2(data_model_columns):
    ts_data = [
        (
            2,
            "2019-01-01 00:00:02",
            "2019-01-01 11:45:48",
            2,
            "xcar\xea\xde\x8cy\xafY\xfe\x12\xf6\x1f _R",
            "x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00",
        ),
        (
            2,
            "2019-01-01 11:45:50",
            "2019-01-01 11:47:14",
            3,
            "xcar\xea\xde\x8cy\xafY\xfe\x12\xf6\x1f _R",
            "x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00",
        ),
    ]
    return (data_model_columns, (data for data in ts_data))


@pytest.fixture(scope="function")
def data_models_tid_1(data_model_columns):
    ts_data = [
        (
            1,
            "2019-01-01 00:00:02",
            "2019-01-01 00:01:40",
            4,
            "xcar\xea\xde\x8cy\xafY\xfe\x12\xf6\x1f _R",
            "x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00",
        ),
        (
            1,
            "2019-01-01 00:01:42",
            "2019-01-01 00:03:20",
            4,
            "xcar\xea\xde\x8cy\xafY\xfe\x12\xf6\x1f _R",
            "x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00",
        ),
    ]
    return (data_model_columns, (data for data in ts_data))


def parse_ts(timestamp: str):
    """Parse timestamps from str to datetime.datetime objects."""
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
