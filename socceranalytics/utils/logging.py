import codecs
import os
import logging
import sys
from datetime import datetime
import pytz

from socceranalytics.data.paths import Config

from socceranalytics.utils.utils import build_dated_file_path


def log(message: str, error: bool = False, exception: bool = False) -> None:
    if exception:
        setup_logger(exception=True).exception(message)
        sys.exit(1)
    else:
        if error:
            setup_logger(error_level=True).error(message)
        else:
            setup_logger(error_level=False).info(message)


class Formatter(logging.Formatter):
    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp)
        tzinfo = pytz.timezone("Europe/Paris")
        return dt.replace(tzinfo=pytz.utc).astimezone(tzinfo)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def setup_logger(error_level: bool = False, exception: bool = False):
    """
    Configure and return a logger.

    Args:
        error_level (bool): The logging level (e.g., logging.DEBUG, logging.INFO, etc.). Defaults to False.
        exception (bool)

    Returns:
        logging.Logger: Configured logger.
    """

    logger = logging.getLogger("analytics")

    # Check if the logger has handlers already to avoid adding them multiple times
    if not logger.hasHandlers():

        if Config.ENV != "local":
            path_logging = build_dated_file_path(Config.PATH_LOGGING, "log")
            path_latest_logging = f"{Config.PATH_LOGGING}/latest.log"
            # Ensure the directory exists
            log_dir = os.path.dirname(path_logging)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
            file_handler = logging.FileHandler(path_logging, mode='w', encoding="utf-8")

            log_dir_latest = os.path.dirname(path_latest_logging)
            if log_dir_latest and not os.path.exists(log_dir_latest):
                os.makedirs(log_dir_latest)
            latest_file_handler = logging.FileHandler(path_latest_logging, mode='w', encoding="utf-8")

        else:
            file_handler = logging.NullHandler()
            latest_file_handler = logging.NullHandler()

        console_handler = logging.StreamHandler(codecs.getwriter("utf-8")(sys.stdout.buffer, "replace"))

        formatter = Formatter(fmt='%(asctime)s %(levelname)s %(message)s')

        file_handler.setFormatter(formatter)
        latest_file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        if not exception:
            if error_level:
                logger.setLevel(logging.ERROR)
            else:
                logger.setLevel(logging.INFO)

        logger.addHandler(file_handler)
        logger.addHandler(latest_file_handler)
        logger.addHandler(console_handler)

    return logger
