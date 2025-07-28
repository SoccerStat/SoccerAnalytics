import aiohttp
from understat import Understat
import asyncio
import json

def get_xG(name_comp, autumn_season):
    async def main():
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            fixtures = await understat.get_league_results(
                name_comp,
                autumn_season
            )

            xG_data = []
            for match in fixtures:
                match_id = match["id"]
                home_team = match["h"]["title"]
                away_team = match["a"]["title"]
                home_xg = match["xG"]["h"]
                away_xg = match["xG"]["a"]
                xG_data.append({
                    "Match": match_id,
                    "Club": home_team,
                    "played_home": True,
                    "xG For": home_xg,
                    "xG Against": away_xg
                })
                xG_data.append({
                    "Match": match_id,
                    "Club": away_team,
                    "played_home": False,
                    "xG For": away_xg,
                    "xg Against": home_xg
                })

            return xG_data

        return None

    return asyncio.run(main())