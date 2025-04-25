from datetime import datetime
import pytz

current_ti = datetime.now(tz=pytz.timezone('Europe/Amsterdam'))


class Utils():
    """Utils functions.
    """
    def get_ti(self) -> datetime:
        """Returns the current timestamp. Used to keep the timestamp of the insertion of something in our Mongo database."""
        return datetime.now(tz=pytz.timezone('Europe/Amsterdam'))

    def get_date_from_datetime(self, date_string: str, format_date: str) -> str:
        return datetime.strftime(date_string, format_date)

    def build_dated_file_path(self, root_folder: str, ext: str):
        return f"{root_folder}/{self.get_date_from_datetime(current_ti, '%Y/%m/%d/%Y_%m_%d_%H_%M_%S')}.{ext}"
