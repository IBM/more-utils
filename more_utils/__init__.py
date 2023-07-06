"""MoreUtils root module

Python APIs to support MORE applications on operations related to time-series
data.

Significant features:
1) Help clients access decompressed time-series data points from
ModelarDB.
2) Time-series manipulations.
3) APIs to save data to any Cloud Object Storage (COS) / Cassandra DB.
"""

__version__ = "0.4.0"
__package_name__ = "MoreUtils"
_logging_level = "INFO"


def set_logging_level(logging_level):
    """Set root logging level

    Args:
        logging_level (str): root logging level
    """
    global _logging_level
    _logging_level = logging_level


def get_logging_level():
    """get root logging level

    Returns:
        str: root logging level
    """
    return _logging_level
