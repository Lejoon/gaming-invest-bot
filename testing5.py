import os, requests, pandas as pd

STEAM_KEY = os.getenv("STEAM_KEY")          # or hard-code it
url = (
    "http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)
params = {
    "key":   STEAM_KEY,
    "appid": 730
}

def get_response_size(url):
    response = requests.get(url, params=params, timeout=20)
    return len(response.content) * 10000 * 10 * 30 / 1024**3  # Convert bytes to gigabytes

print(f"Response size: {get_response_size(url)} GB")

