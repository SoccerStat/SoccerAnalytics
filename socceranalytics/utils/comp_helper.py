
class CompHelper:
    def __init__(self):
        self.mapping = {
            "premier_league": "EPL",
            "ligue_1": "Ligue_1",
            "la_liga": "La_liga",
            "fussball_bundesliga": "Bundesliga",
            "serie_a": "Serie_A"
        }
    def get_understat_comp_from_soccerstat(self, soccerstat_comp: str):
        return self.mapping[soccerstat_comp] or None