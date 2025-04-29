from datetime import datetime, timedelta
import asyncio
import random
import aiohttp
import discord

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f'[LOG] {timestamp} - {message}')
    
ERROR_ID = 1162053416290361516 # Define your error channel ID

async def error_message(message: str, bot: discord.Client | None = None):
    """
    Logs an error message to the console and, if a bot object is provided,
    attempts to send it to a specific Discord channel.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Added seconds for more precision
    log_entry = f"[ERR] {timestamp} - {message}"
    print(log_entry) # Always print to console

    # Only attempt to send to Discord if bot object is provided
    if bot:
        try:
            # Attempt to get the channel and send the message
            error_channel = bot.get_channel(ERROR_ID)
            if error_channel and isinstance(error_channel, discord.TextChannel):
                 # Send the same message that was printed (or customize if needed)
                 # Ensure message length is within Discord limits (2000 chars)
                await error_channel.send(log_entry[:2000])
            elif error_channel:
                # Log locally if channel is wrong type
                print(f"[ERR] {timestamp} - Channel {ERROR_ID} is not a valid text channel.")
            else:
                # Log locally if channel not found
                print(f"[ERR] {timestamp} - Could not find error channel with ID: {ERROR_ID}")

        except discord.errors.Forbidden:
            print(f"[ERR] {timestamp} - Bot lacks permissions to send messages in channel {ERROR_ID}.")
        except Exception as e:
            # Catch any other exceptions during the Discord send process
            print(f"[ERR] {timestamp} - Failed to report error to Discord channel {ERROR_ID}: {type(e).__name__}: {e}")

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
