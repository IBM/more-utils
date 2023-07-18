import logging
import logzero
import more_utils


def configure_logger(
    package_name:str="MoreUtils", logger_name: str = "", logging_level=more_utils.get_logging_level()
):
    if logger_name:
        logger_name = " (" + logger_name + ")"

    info_log_format = (
        "%(color)s[%(asctime)s:%(msecs)d] - %(levelname)s - "
        + package_name
        + " : "
        "%(end_color)s%(message)s"
    )
    rest_log_format = (
        "%(color)s[%(asctime)s:%(msecs)d] - %(levelname)s - "
        + package_name
        + logger_name
        + " "
        "%(module)s:%(funcName)s:%(lineno)d%(end_color)s %(message)s"
    )

    logging_level = logging.getLevelName(logging_level)
    if logging.INFO == logging_level:
        log_format = info_log_format
    else:
        log_format = rest_log_format

    formatter = logzero.LogFormatter(
        fmt=log_format, datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logzero.setup_logger(
        name=logger_name,
        level=logging_level,
        formatter=formatter,
    )
