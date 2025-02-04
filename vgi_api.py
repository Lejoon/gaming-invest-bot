# vgi_api.py

import requests

BASE_URL = "https://vginsights.com/api/v1"

def fetch_api(url, error_msg):
    """Generic function to perform a GET request and return JSON data."""
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"{error_msg} (Status code: {response.status_code})")
    return response.json()

def search_game(game_name):
    """Call the search API to find matching game names."""
    search_url = f"{BASE_URL}/site-search?q={game_name}&limit=10"
    return fetch_api(search_url, f"Failed to retrieve search results from {search_url}")

def get_quick_stats(game_slug):
    """
    Retrieve quick stats for a given game slug from the vginsights quick-stats API.

    The API returns JSON data in the following format:
    {
        "steam": {
            "avg_playtime": 56.45,
            "med_playtime": 12.45,
            "players_latest_time": 39,
            "max_players_24h": 13879,
            "players_latest": 13879,
            "wishlist_count": -1,
            "followers": 811000,
            "reviews": 268000,
            "followers_increase": 629,
            "wishlists_increase": 567,
            "units_sold_vgi": 12600000,
            "units_sold_vgi_beta": null,
            "revenue_vgi": "228500000",
            "rating": 93.3,
            "daily_active_users": -1,
            "monthly_active_users": -1,
            "released": "2015-03-10T00:00:00.000Z"
        }
    }

    :param game_slug: The game slug (e.g. "cities-skylines") to use in the API URL.
    :return: The parsed JSON dictionary.
    :raises Exception: if the request fails.
    """
    quick_stats_url = f"{BASE_URL}/game/{game_slug}/quick-stats"
    return fetch_api(quick_stats_url, f"Failed to retrieve quick stats from {quick_stats_url}")

def get_game_details(game_slug):
    """Retrieve game details for a given slug."""
    details_url = f"{BASE_URL}/game/{game_slug}"
    return fetch_api(details_url, f"Failed to retrieve detailed stats from {details_url}")


def get_sales_data_steam(game_slug):
    """Retrieve monthly sales-summary data for the given game slug."""
    sales_url = (
        f"{BASE_URL}/game/{game_slug}/sales-summary?"
        "period=Quarter&isAlignedForCumulative=truealse&isAlignedToRelease=false&platforms=steam"
    )
    return fetch_api(sales_url, f"Failed to retrieve sales data from {sales_url}")

def get_sales_data_playstation(game_slug):
    """Retrieve monthly sales-summary data for the given game slug."""
    sales_url = (
        f"{BASE_URL}/game/{game_slug}/sales-summary?"
        "period=Quarter&isAlignedForCumulative=true&isAlignedToRelease=false&platforms=playstation"
    )
    return fetch_api(sales_url, f"Failed to retrieve sales data from {sales_url}")


def get_logos_data(game_slug):
    """Retrieve logos (e.g. capsule image) for the given game slug."""
    logos_url = f"{BASE_URL}/game/{game_slug}/logos"
    response = requests.get(logos_url)
    if response.status_code == 200:
        return response.json()
    return None