
class CompHelper:
    def __init__(self):
        self.mapping = {
            "Premier League": "EPL",
            "Ligue 1": "Ligue_1",
            "La Liga": "La_liga",
            "Fußball-Bundesliga": "Bundesliga",
            "Serie A": "Serie_A"
        }

    def get_understat_comp_from_soccerstat(self, soccerstat_comp: str):
        return self.mapping.get(soccerstat_comp)