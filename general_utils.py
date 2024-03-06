from datetime import datetime, timedelta
import asyncio
import random
import aiohttp

def log_message(message, bot):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f'[LOG] {timestamp} - {message}')
    
def error_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f'[ERR] {timestamp} - {message}')

def get_seconds_until(time_hour, time_minute):
    now = datetime.now()
    target_time = datetime(now.year, now.month, now.day, time_hour, time_minute)
    
    # If target time is in the past, calculate for the next day
    if now > target_time:
        target_time += timedelta(days=1)
        
    return int((target_time - now).total_seconds())

def aiohttp_retry(retries=5, base_delay=15.0, max_delay=120.0):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            nonlocal retries, base_delay, max_delay
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, aiohttp.ClientConnectorError, aiohttp.ClientConnectionError) as e:
                    if attempt == retries - 1:  # If this was the last retry
                        raise  # Re-raise the last exception
                    else:
                        # Calculate delay: base_delay * 2 ^ attempt, but with a random factor to avoid thundering herd problem
                        delay = min(max_delay, base_delay * 2 ** attempt) * (0.5 + random.random())
                        print(f'Awaiting {delay:.2f} seconds before retrying...')
                        await asyncio.sleep(delay)
            return await func(*args, **kwargs)  # Try one last time
        return wrapper
    return decorator