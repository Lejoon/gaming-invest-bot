from datetime import datetime, timedelta
import pytz
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from discord import Embed
from general_utils import log_message, error_message
import time

CHANNEL_ID = 1161207966855348246
CUSTOM_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
INTERESTED_EPICS = ["IX.D.OMX.IFD.IP", "IX.D.DAX.IFD.IP", "IX.D.SPTRD.IFD.IP", "IX.D.FTSE.CFD.IP", "IX.D.DOW.IFD.IP", "IX.D.NASDAQ.IFD.IP"]
LABEL_EPICS = {"IX.D.OMX.IFD.IP": "OMX", "IX.D.DAX.IFD.IP": "DAX", "IX.D.SPTRD.IFD.IP": "SP500", "IX.D.FTSE.CFD.IP": "FTSE 100", "IX.D.DOW.IFD.IP": "Dow Jones", "IX.D.NASDAQ.IFD.IP": "Nasdaq"}

def get_seconds_until(time_hour, time_minute):
    now = datetime.now()
    target_time = datetime(now.year, now.month, now.day, time_hour, time_minute)
    # If target time is in the past, calculate for the next day
    if now > target_time:
        target_time += timedelta(days=1)
    return int((target_time - now).total_seconds())

def _scrape_ig_once(options) -> list[dict]:
    """Synchronous, blocking Selenium scrape. To be executed in a thread."""
    with webdriver.Chrome(options=options) as driver:
        driver.get('https://www.ig.com/se/index/marknader-index/')

        # Wait for the web component and give a tiny buffer for rendering
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "igws-live-prices")))
        time.sleep(1)

        web_component = driver.find_element(By.CSS_SELECTOR, 'igws-live-prices')
        shadow_root = driver.execute_script('return arguments[0].shadowRoot', web_component)

        WebDriverWait(driver, 5).until(
            lambda d: len(shadow_root.find_elements(By.CSS_SELECTOR, '.dynamic-table__row.clickable')) > 0
        )

        rows = shadow_root.find_elements(By.CSS_SELECTOR, '.dynamic-table__row.clickable')
        log_message(f"Found {len(rows)} rows in the table")

        scraped_data: list[dict] = []
        for i, row in enumerate(rows):
            try:
                index_element = row.find_element(By.CSS_SELECTOR, 'a[data-epic]')
                index = index_element.get_attribute('data-epic')

                if index in INTERESTED_EPICS:
                    change_value = None
                    selectors = [
                        'span[data-field="CPC"]',
                        'span[data-field="PC"]',
                        '.price-change',
                        '[data-field*="change"]',
                        '[data-field*="Change"]'
                    ]

                    for selector in selectors:
                        try:
                            change_element = row.find_element(By.CSS_SELECTOR, selector)
                            change_value = change_element.text.strip()
                            if change_value and change_value != "":
                                break
                        except Exception:
                            continue

                    # Skip placeholder or missing values like "-"
                    if change_value and change_value not in ("", "-"):
                        scraped_data.append({'Index': index, 'Change Value': change_value})
                        log_message(f"Successfully scraped {LABEL_EPICS.get(index, index)}: {change_value}")
                    else:
                        log_message(f"Could not find change value for {LABEL_EPICS.get(index, index)}")

            except Exception as e:
                log_message(f"Error processing row {i}: {str(e)}")
                continue

        return scraped_data


async def get_scraped_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={CUSTOM_USER_AGENT}")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            scraped_data = await asyncio.to_thread(_scrape_ig_once, options)
            log_message(f"Attempt {attempt + 1}: scraped {len(scraped_data)} candidates")

            valid_data = validate_scraped_data(scraped_data)
            if valid_data:
                log_message(f"Successfully scraped {len(valid_data)} valid indexes")
                return valid_data
            else:
                await error_message(f"Attempt {attempt + 1}: No valid numeric data found, retrying...")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        except Exception as e:
            await error_message(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)

    await error_message("All retry attempts failed")
    return []

def validate_scraped_data(scraped_data):
    """Validate that scraped data contains numeric change values"""
    valid_data = []
    for data in scraped_data:
        change_value = data['Change Value']
        # Remove common non-numeric characters and check if it's a valid number
        cleaned_value = change_value.replace('%', '').replace('+', '').replace('-', '').replace(',', '.').strip()
        try:
            float(cleaned_value)
            valid_data.append(data)
            log_message(f"Validated numeric value for {LABEL_EPICS.get(data['Index'], data['Index'])}: {change_value}")
        except ValueError:
            log_message(f"Invalid numeric value for {LABEL_EPICS.get(data['Index'], data['Index'])}: {change_value}")

    return valid_data

async def send_daily_message(bot, time_hour, time_minute):
    while True:
        await asyncio.sleep(get_seconds_until(time_hour, time_minute))
        
        current_day = datetime.now().weekday()
        if current_day in [5, 6]:
            log_message("It's the weekend. Skipping the daily message.")
            await asyncio.sleep(60 * 60)  # Sleep for 24 hours
            continue
        
        scraped_data = await get_scraped_data()
        # If time_hour > 12 it's evening, otherwise it's morning
        title_text = "\U0001F4C8 Indexterminer"
        description_text = "Snart börjar aktiehandeln, terminerna indikerar:" if time_hour < 12 else "Aktiehandeln i USA stänger, terminerna indikerar:"
        embed = Embed(
            title=title_text,
            description=description_text,
            color=0x3498db,
            timestamp=datetime.now(pytz.utc)
        )
        embed.set_footer(text="Källa: IG.com")
        
        for data in scraped_data:
            label = LABEL_EPICS.get(data['Index'], data['Index'])
            embed.add_field(name=label, value=f"{data['Change Value']}%", inline=True)
            
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            await channel.send(embed=embed)
            log_message(f'Sent daily index to Discord.')
async def send_current_index(ctx):
    try:
        scraped_data = await get_scraped_data()
        
        if not scraped_data:
            await ctx.send("❌ Could not retrieve index data at this time. Please try again later.")
            return
            
        embed = Embed(
            title="\U0001F4C8 Indexterminer",
            description="Aktuella index med fördröjning på OMX, handlas även utanför normala börstider men ej helger:",
            color=0x3498db,
            timestamp=datetime.now(pytz.utc)
        )
        embed.set_footer(text="Källa: IG.com")

        valid_data_count = 0
        for data in scraped_data:
            if data['Change Value'] and data['Change Value'].strip():
                label = LABEL_EPICS.get(data['Index'], data['Index'])
                embed.add_field(name=label, value=f"{data['Change Value']}%", inline=True)
                valid_data_count += 1
        
        if valid_data_count == 0:
            await ctx.send("❌ No valid index data available at this time.")
            return
            
        await ctx.send(embed=embed)
        log_message(f'Sent current index to Discord with {valid_data_count} valid entries.')
        
    except Exception as e:
        await error_message(f"Error in send_current_index: {str(e)}")
        await ctx.send("❌ An error occurred while fetching index data.")

async def daily_message_morning(bot):
    log_time = datetime.now()
    log_time += timedelta(seconds=get_seconds_until(8,55))
    log_message(f'Waiting until {log_time.strftime("%Y-%m-%d %H:%M:%S")} to send morning message.')
    await send_daily_message(bot, 8, 55)

async def daily_message_evening(bot):
    log_time = datetime.now()
    log_time += timedelta(seconds=get_seconds_until(21,59))
    log_message(f'Waiting until {log_time.strftime("%Y-%m-%d %H:%M:%S")} to send evening message.')
    await send_daily_message(bot, 21, 59)
    
async def current_index(ctx):
    await send_current_index(ctx)

async def debug_scraping():
    """Debug function to test scraping without Discord bot"""
    print("=" * 50)
    print("DEBUG: Testing index scraping")
    print("=" * 50)
    
    scraped_data = await get_scraped_data()
    
    if not scraped_data:
        print("❌ No data was scraped")
        return
    
    print(f"\n✅ Successfully scraped {len(scraped_data)} indexes:")
    print("-" * 50)
    print(f"{'Index':<12} | {'Label':<12} | {'Change Value'}")
    print("-" * 50)
    
    for data in scraped_data:
        index = data['Index']
        label = LABEL_EPICS.get(index, index)
        change_value = data['Change Value']
        print(f"{index:<12} | {label:<12} | {change_value}")
    
    print("-" * 50)
    print(f"Total valid entries: {len(scraped_data)}")

if __name__ == "__main__":
    asyncio.run(debug_scraping())

# Key changes made:
# 1. Extended wait times: 30s initial, 10s for shadow DOM, 2s buffer
# 2. Added multiple CSS selectors as fallbacks for change values
# 3. Added comprehensive error handling and logging
# 4. Added data validation before sending to Discord
# 5. Added graceful failure messages to users
# 6. Reduced wait times and added retry logic based on data validation