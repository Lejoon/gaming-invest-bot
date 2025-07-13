import os, requests, pandas as pd

STEAM_KEY = os.getenv("STEAM_KEY")          # or hard-code it
url = (
    "https://api.steampowered.com/ISteamChartsService/"
    "GetMostPlayedGames/v1/"
)
params = {
    "key":   STEAM_KEY,
    "start": 0,         # offset
    "count": 5000,      # max allowed
    "format": "json"
}

resp = requests.get(url, params=params, timeout=20)
resp.raise_for_status()
games = resp.json()["response"]["ranks"]

df = pd.DataFrame(games)          # appid, rank, concurrent_in_game, ...
print(df)
