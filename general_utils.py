from datetime import datetime, timedelta
import asyncio
import random
import aiohttp

def get_seconds_until(time_hour, time_minute):
    now = datetime.now()
    target_time = datetime(now.year, now.month, now.day, time_hour, time_minute)
    
    # If target time is in the past, calculate for the next day
    if now > target_time:
        target_time += timedelta(days=1)
        
    return int((target_time - now).total_seconds())

async def retry_with_backoff(func, retries=5, base_delay=10.0, max_delay=120.0):
    """
    Retry a function with exponential backoff.

    Parameters:
    - func: The function to retry. It should be a coroutine.
    - retries: The maximum number of retries before giving up. Default is 5.
    - base_delay: The base delay in seconds. Default is 1.0.
    - max_delay: The maximum delay in seconds. Default is 60.0.
    """
    for retry in range(retries):
        try:
            return await func()
        except aiohttp.ClientConnectorError as e:
            if retry == retries - 1:  # If this was the last retry
                raise  # Re-raise the last exception
            else:
                # Calculate delay: base_delay * 2 ^ retry, but randomized to spread out the load
                delay = min(max_delay, base_delay * 2 ** retry) * (0.5 + random.random())
                print('Trying to reconnect in', delay, 'seconds...')
                await asyncio.sleep(delay)