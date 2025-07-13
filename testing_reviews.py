# Script that analyzes Steam app reviews to count reviews by num_games_owned

import requests
import json
import os

def get_response_size(url):
    response = requests.get(url)
    gigabytes = len(response.content) / 1024**3  # Convert bytes to gigabytes
    # Calculate number of requests as * 25 for 2500 results and 6 times per day for 30 days in gigabytes
    return gigabytes * 25 * 6 * 30

def appreviews(appid, filter="all", language="all", day_range="365", cursor="*", 
               review_type="all", purchase_type="steam", num_per_page="20", 
               filter_offtopic_activity=1):
    """
    Fetches Steam app reviews using the Steam Store API.
    
    Arguments:
    appid (str): The Steam App ID to get reviews for
    filter (str): How to sort reviews:
        - "recent" - sorted by creation time
        - "updated" - sorted by last updated time  
        - "all" - (default) sorted by helpfulness with sliding windows
    language (str): Language code (see Steam API language codes) or "all" for all reviews
    day_range (str): Range from now to n days ago for helpful reviews (max 365, only for "all" filter)
    cursor (str): Pagination cursor - use "*" for first request, then returned cursor value
    review_type (str): Type of reviews to return:
        - "all" - all reviews (default)
        - "positive" - only positive reviews
        - "negative" - only negative reviews
    purchase_type (str): Purchase type filter:
        - "all" - all reviews
        - "non_steam_purchase" - reviews by users who didn't buy on Steam
        - "steam" - reviews by users who bought on Steam (default)
    num_per_page (str): Number of reviews per page (default 20, max 100)
    filter_offtopic_activity (int): 1 to filter out review bombs (default), 0 to include them
    
    Returns:
    dict: API response containing:
        - success: 1 if query was successful
        - query_summary: Summary info (returned in first request):
            - num_reviews: Number of reviews returned
            - review_score: The review score
            - review_score_desc: Description of review score
            - total_positive: Total positive reviews
            - total_negative: Total negative reviews
            - total_reviews: Total reviews matching query
        - cursor: Value for next pagination request
        - reviews: Array of review objects with:
            - recommendationid: Unique recommendation ID
            - author: Author information:
                - steamid: User's Steam ID
                - num_games_owned: Number of games owned by user
                - num_reviews: Number of reviews written by user
                - playtime_forever: Lifetime playtime in this app
                - playtime_last_two_weeks: Playtime in past two weeks
                - playtime_at_review: Playtime when review was written
                - deck_playtime_at_review: Steam Deck playtime at review time
                - last_played: When user last played
            - language: Language review was written in
            - review: Text of the review
            - timestamp_created: Review creation date (unix timestamp)
            - timestamp_updated: Review last updated date (unix timestamp)
            - voted_up: True if positive recommendation
            - votes_up: Number of helpful votes
            - votes_funny: Number of funny votes
            - weighted_vote_score: Helpfulness score
            - comment_count: Number of comments on review
            - steam_purchase: True if user bought on Steam
            - received_for_free: True if user got app for free
            - written_during_early_access: True if written during Early Access
            - developer_response: Developer response text (if any)
            - timestamp_dev_responded: Unix timestamp of developer response
            - primarily_steam_deck: True if played primarily on Steam Deck
    """
    url = f"https://store.steampowered.com/appreviews/{appid}"
    
    params = {
        'filter': filter,
        'language': language,
        'day_range': day_range,
        'cursor': cursor,
        'review_type': review_type,
        'purchase_type': purchase_type,
        'num_per_page': num_per_page,
        'filter_offtopic_activity': filter_offtopic_activity,
        'json': 1
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching reviews for app {appid}: {e}")
        return None

def GetOwnedGames(steamid, steam_key, include_appinfo=False, include_played_free_games=True, appids_filter=None):
    """
    Fetches the owned games for a Steam ID using the Steam Web API.
    
    Arguments:
    steamid (str): The SteamID of the account
    steam_key (str): Steam Web API key
    include_appinfo (bool): Include game name and logo information in the output. Default True.
    include_played_free_games (bool): Include free games if the player has played them. Default True.
    appids_filter (list): Optional list of app IDs to filter results
    
    Returns:
    dict: API response containing:
        - game_count: Total number of games the user owns
        - games: Array of games with the following for each game:
            - appid: Unique identifier for the game
            - name: The name of the game (if include_appinfo=True)
            - playtime_2weeks: Minutes played in the last 2 weeks
            - playtime_forever: Total minutes played on record
            - img_icon_url: Filename for game icon
            - img_logo_url: Filename for game logo
            - has_community_visible_stats: Boolean indicating if stats page available
    """
    url = "http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/"
    
    params = {
        'key': steam_key,
        'steamid': steamid,
        'format': 'json',
        'include_appinfo': 1 if include_appinfo else 0,
        'include_played_free_games': 1 if include_played_free_games else 0
    }
    
    # Note: appids_filter requires JSON format as per API documentation
    if appids_filter:
        # This would need to be sent as JSON in the request body
        # For URL parameters, we'll skip this advanced filtering
        pass
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching owned games for {steamid}: {e}")
        return None

STEAM_KEY = os.getenv("STEAM_API_KEY")          # or hard-code it

url = "https://store.steampowered.com/appreviews/578080?num_per_page=100&purchase_type=steam&cursor=*&json=1"

print(f"Response size: {get_response_size(url)} GB")

response = requests.get(url)
data = response.json()

if data.get('success') == 1:
    reviews = data.get('reviews', [])
    
    num_games_owned_zero = 0
    num_games_owned_greater_than_zero = 0
    steamids_with_games = []
    
    for review in reviews:
        author = review.get('author', {})
        num_games_owned = author.get('num_games_owned', 0)
        steamid = author.get('steamid', '')
        
        if num_games_owned == 0:
            num_games_owned_zero += 1
        elif num_games_owned > 0:
            num_games_owned_greater_than_zero += 1
            steamids_with_games.append(steamid)
    
    print(f"Reviews with num_games_owned = 0: {num_games_owned_zero}")
    print(f"Reviews with num_games_owned > 0: {num_games_owned_greater_than_zero}")
    print(f"Total reviews analyzed: {len(reviews)}")
    print(f"\nSteam IDs with num_games_owned > 0:")
    for steamid in steamids_with_games[:2]:
        print(steamid)
        print(GetOwnedGames(steamid, STEAM_KEY))
        
else:
    print("Failed to fetch reviews")

