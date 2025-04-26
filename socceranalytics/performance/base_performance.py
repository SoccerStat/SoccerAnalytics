from abc import ABC, abstractmethod

from socceranalytics.postgres.postgres_querying import PostgresQuerying
from socceranalytics.utils.data_loader import DataLoader


class BasePerformance(ABC):
    """Fill performance tables.
    """
    def __init__(self, postgres_to_dataframe: PostgresQuerying, kind: str):
        self.db = postgres_to_dataframe
        self.data_loader = DataLoader(postgres_to_dataframe)

        self.performance_sql_path = f"socceranalytics/sql/performance/{kind}"
        self.ranking_sql_path = f"socceranalytics/sql/rankings/{kind}"
        self.opposition_sql_path = f"socceranalytics/sql/oppositions/{kind}_x_teams"

    @abstractmethod
    def process_performance_table(self):
        pass
