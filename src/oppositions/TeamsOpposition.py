from postgres.PostgresToDataFrame import PostgresToDataframe

class TeamsOpposition:
    def __init__(self, postgres_to_dataframe: PostgresToDataframe):
        self.db = postgres_to_dataframe