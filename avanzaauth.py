import os
import hashlib
import requests
import pyotp
from dotenv import load_dotenv
import time
import threading

load_dotenv()

# ─── Configuration ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.avanza.se"
AUTH_PATH = "/_api/authentication/sessions/usercredentials"
TOTP_PATH = "/_api/authentication/sessions/totp"
OVERVIEW_P = "/_api/account-overview/overview/categorizedAccounts"

# from your .env
USER = os.getenv("AVANZA_USERNAME")
PWD = os.getenv("AVANZA_PASSWORD")
TOTP_SECRET = os.getenv("AVANZA_TOTP_SECRET")

if not (USER and PWD and TOTP_SECRET):
    raise SystemExit("Please set AVANZA_USERNAME, AVANZA_PASSWORD & AVANZA_TOTP_SECRET in your .env")

avanza_session = None
push_id_global = None
keep_alive_thread = None

def _authenticate():
    """Handles the authentication and 2FA process with Avanza."""
    session = requests.Session()
    session.trust_env = True  # honour HTTP(S)_PROXY env vars if you prefer

    resp = session.post(
        BASE_URL + AUTH_PATH,
        json={"username": USER, "password": PWD, "maxInactiveMinutes": 1440}
    )
    resp.raise_for_status()
    auth_data = resp.json()

    if auth_data.get("twoFactorLogin"):
        code = pyotp.TOTP(TOTP_SECRET, digest=hashlib.sha1).now()
        resp2 = session.post(BASE_URL + TOTP_PATH, json={"method": "TOTP", "totpCode": code})
        resp2.raise_for_status()
        auth_data = resp2.json()

    # grab security token & pushSubscriptionId
    token = resp.headers.get("X-SecurityToken") or resp2.headers.get("X-SecurityToken")
    push_id = auth_data["pushSubscriptionId"]
    session.headers.update({"X-SecurityToken": token})

    # sanity check
    ov = session.get(BASE_URL + OVERVIEW_P).json()
    print("✅ Logged in. Your categories are:", [c["name"] for c in ov["categories"]])

    return session, push_id

def _keep_alive(sess, interval=300):  # hit overview every 5 minutes
    global avanza_session # Declare global at the beginning of the function
    while True:
        try:
            response = sess.get(f"{BASE_URL}{OVERVIEW_P}")
            response.raise_for_status() # Check for HTTP errors
            # Optionally, check if the response content indicates a valid session
            # For example, if an error message is returned in the JSON
            if "error" in response.json(): # This is a hypothetical check
                print("⚠️ Keep-alive detected session issue, attempting re-authentication.")
                avanza_session = None # Mark session as invalid
                break # Exit keep-alive loop
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Keep-alive failed: {e}")
            avanza_session = None # Mark session as invalid on network or other request errors
            break # Exit keep-alive loop
        except Exception as e: # Catch other potential errors
            print(f"⚠️ Keep-alive encountered an unexpected error: {e}")
            avanza_session = None
            break
        time.sleep(interval)

def get_avanza_session():
    global avanza_session, push_id_global, keep_alive_thread
    if avanza_session is None:
        print("Attempting to authenticate with Avanza...")
        try:
            avanza_session, push_id_global = _authenticate()
            if keep_alive_thread is None or not keep_alive_thread.is_alive():
                keep_alive_thread = threading.Thread(target=_keep_alive, args=(avanza_session,), daemon=True)
                keep_alive_thread.start()
                print("Avanza keep-alive thread started.")
        except requests.exceptions.HTTPError as e:
            print(f"Authentication failed: {e.response.status_code} - {e.response.text}")
            avanza_session = None # Ensure session is None if auth fails
            push_id_global = None
            return None, None # Return None if authentication fails
        except Exception as e:
            print(f"An unexpected error occurred during authentication: {e}")
            avanza_session = None
            push_id_global = None
            return None, None
    return avanza_session, push_id_global

# Example of how to fetch price chart data using the session
def fetch_price_chart_authenticated(order_book_id, period):
    """Fetch price chart JSON for a given stock when logged in"""
    global avanza_session # Declare global at the beginning of the function
    session, _ = get_avanza_session()
    if not session:
        print("❌ Cannot fetch price chart: Not authenticated.")
        return None
    
    url = f"{BASE_URL}/_api/price-chart/stock/{order_book_id}?timePeriod={period}"
    try:
        resp = session.get(url)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e: # Catch HTTPError specifically
        print(f"HTTP error fetching price chart data: {e}")
        if e.response is not None and e.response.status_code == 401: # Unauthorized
            print("Session might be invalid/expired. Attempting re-authentication on next call.")
            avanza_session = None # Invalidate session
        return None
    except requests.exceptions.RequestException as e: # Catch other request-related errors
        print(f"Error fetching price chart data: {e}")
        # Potentially invalidate session here too if it's a persistent network issue
        # or if certain errors imply session invalidity. For now, only 401 invalidates.
        return None
    except Exception as e:
        print(f"An unexpected error occurred while fetching price chart data: {e}")
        return None