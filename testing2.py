import os, json, gzip, requests
from typing import List, Dict, Optional

API_URL = (
    "https://api.steampowered.com/"
    "ISteamChartsService/GetGamesByConcurrentPlayers/v1/"
)

def fetch_ccu_table(key: str,
                    context: Optional[dict] = None,
                    save_raw: Optional[str] = None) -> List[Dict]:
    if not key:
        raise RuntimeError("STEAM_KEY env var is empty or missing")

    params = {"key": key, "format": "json"}
    if context:
        params["context"] = json.dumps(context, separators=(",", ":"))

    with requests.get(API_URL, params=params, stream=True, timeout=30) as r:
        r.raise_for_status()
        data = r.json()                       # requests auto-decodes gzip

    resp = data.get("response", data)
    ranks = resp.get("ranks")                # correct field name
    if ranks is None:
        raise RuntimeError(f"unexpected payload: {json.dumps(resp)[:300]}")

    if save_raw:
        with gzip.open(save_raw, "wt", encoding="utf-8") as fh:
            json.dump(ranks, fh, separators=(",", ":"))

    return ranks

if __name__ == "__main__":
    steam_key = os.getenv("STEAM_API_KEY")
    rows = fetch_ccu_table(
        key=steam_key,
        context={"language": "english", "country_code": "SE", "currency_code": "EUR"},
        # save_raw="ccu_snapshot.json.gz"
    )
    print(f"downloaded {len(rows):,} apps; top slot: {rows[0]}")
