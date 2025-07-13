# Script that tests https://store.steampowered.com/search/results/?query&start=0&count=100&dynamic_data=&sort_by=_ASC&filter=globaltopsellers&infinite=1 and sees how many kb the response is

import requests
def get_response_size(url):
    response = requests.get(url)
    gigabytes = len(response.content) / 1024**3  # Convert bytes to gigabytes
    # Calculate number of requests as * 25 for 2500 results and 6 times per day for 30 days in gigabytes
    return gigabytes * 25 * 6 * 30


url = "https://store.steampowered.com/search/results/?query&start=0&count=100&dynamic_data=&sort_by=_ASC&filter=globaltopsellers&infinite=1"
print(f"Response size: {get_response_size(url)} GB")
