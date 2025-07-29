import aiohttp
from understat import Understat
import asyncio
import json

def reprocess_club_name(club):
    return club.replace(
        'Parma Calcio 1913',
        'Parma'
    )

def get_teams_xG(understat_comp, season):
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            fixtures = await understat.get_league_results(
                understat_comp,
                season
            )
            xG_data = []
            for match in fixtures:
                match_id = match["id"]
                home_team = reprocess_club_name(match["h"]["title"])
                away_team = reprocess_club_name(match["a"]["title"])
                home_xg = match["xG"]["h"]
                away_xg = match["xG"]["a"]
                xG_data.append({
                    "match": match_id,
                    "name_team": home_team,
                    "name_opponent": away_team,
                    "played_home": True,
                    "home_xg_for": home_xg,
                    "away_xg_for": 0,
                    "home_xg_against": away_xg,
                    "away_xg_against": 0
                })
                xG_data.append({
                    "match": match_id,
                    "name_team": away_team,
                    "name_opponent": home_team,
                    "played_home": False,
                    "home_xg_for": 0,
                    "away_xg_for": away_xg,
                    "home_xg_against": 0,
                    "away_xg_against": home_xg
                })
            return xG_data
        return None
    return asyncio.run(main())

def get_players_xG(match_id):
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            fixtures = await understat.get_match_players(match_id)

            xG_data = []
            for side, players in fixtures.items():
                for _, stats in players.items():
                    xG_data.append({
                        "Match": match_id,
                        "Player": stats["player"],
                        "played_home": side == "h",
                        "Shots": stats["shots"],
                        "xG": stats["xG"]
                    })

            return xG_data

        return None

    return asyncio.run(main())

print(get_teams_xG("EPL", "2024"))