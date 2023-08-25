import logging
import logzero
import more_utils
from typing import Union


def configure_logger(
    logger_name: str,
    package_name: Union[None, str] = "moreutils",
    logging_level=more_utils.get_logging_level(),
    json=False,
):
    if logger_name is None or logger_name == "":
        raise Exception(
            "Logger name cannot be None or empty. Accessing root logger is restricted. Please use routine configure_root_logger() to access the root logger."
        )

    heading = logger_name
    if package_name and package_name != "":
        heading = package_name + " " + "(" + heading + ")"

    info_log_format = (
        "%(color)s[%(asctime)s:%(msecs)d] - %(levelname)s - " + heading + " - "
        "%(end_color)s%(message)s"
    )
    rest_log_format = (
        "%(color)s[%(asctime)s:%(msecs)d] - %(levelname)s - " + heading + " - "
        "%(module)s:%(funcName)s:%(lineno)d%(end_color)s %(message)s"
    )

    logging_level = logging.getLevelName(logging_level)
    if logging.INFO == logging_level:
        log_format = info_log_format
    else:
        log_format = rest_log_format

    formatter = logzero.LogFormatter(fmt=log_format, datefmt="%Y-%m-%d %H:%M:%S")
    return logzero.setup_logger(
        name=logger_name, level=logging_level, formatter=formatter, json=json
    )


def configure_root_logger(
    package_name: str = "moreutils",
    logging_level=more_utils.get_logging_level(),
    json=False,
):
    heading = ""
    if package_name and package_name != "":
        heading = package_name + " - "

    info_log_format = (
        "%(color)s[%(asctime)s:%(msecs)d] - %(levelname)s - "
        + heading
        + "%(end_color)s%(message)s"
    )
    rest_log_format = (
        "%(color)s[%(asctime)s:%(msecs)d] - %(levelname)s - "
        + heading
        + "%(module)s:%(funcName)s:%(lineno)d%(end_color)s %(message)s"
    )

    logging_level = logging.getLevelName(logging_level)
    if logging.INFO == logging_level:
        log_format = info_log_format
    else:
        log_format = rest_log_format

    formatter = logzero.LogFormatter(fmt=log_format, datefmt="%Y-%m-%d %H:%M:%S")
    return logzero.setup_logger(
        name=None, level=logging_level, formatter=formatter, json=json
    )
