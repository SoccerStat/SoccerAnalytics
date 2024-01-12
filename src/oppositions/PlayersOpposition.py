from postgres.PostgresToDataFrame import PostgresToDataframe

class PlayersOpposition:
    def __init__(self, postgres_to_dataframe: PostgresToDataframe):
        self.db = postgres_to_dataframe