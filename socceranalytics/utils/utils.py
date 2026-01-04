import argparse
import re
from datetime import datetime
import pytz


def get_ti() -> datetime:
    """Returns the current timestamp. Used to keep the timestamp of the insertion of something in our Mongo database."""
    return datetime.now(tz=pytz.timezone('Europe/Amsterdam'))


def get_date_from_datetime(date_string: str, format_date: str) -> str:
    return datetime.strftime(date_string, format_date)


def build_dated_file_path(root_folder: str, ext: str):
    current_ti = datetime.now(tz=pytz.timezone('Europe/Amsterdam'))
    return f"{root_folder}/{get_date_from_datetime(current_ti,'%Y/%m/%d/%Y_%m_%d_%H_%M_%S')}.{ext}"


def parse_args():
    parser = argparse.ArgumentParser(description='Script de mise à jour des tables analytiques')

    parser.add_argument("--env", type=str, required=True, choices=["dev", "prod"])
    parser.add_argument("--min_season", type=season_type, required=False)
    parser.add_argument("--max_season", type=season_type, required=False)

    return parser.parse_args()


def season_type(value: str) -> str:
    pattern = r"^\d{4}_\d{4}$"
    if not re.match(pattern, value):
        raise argparse.ArgumentTypeError(
            "Format invalide. Format attendu : YYYY_YYYY (ex: 2025_2026)"
        )

    start, end = map(int, value.split("_"))
    if end != start + 1:
        raise argparse.ArgumentTypeError(
            "La deuxième année doit être égale à la première + 1 (ex: 2025_2026)"
        )

    return value
