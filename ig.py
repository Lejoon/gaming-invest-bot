from datetime import datetime, timedelta
import pytz
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
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

async def get_scraped_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument(f"user-agent={CUSTOM_USER_AGENT}")

    with webdriver.Chrome(options=options) as driver:
        driver.get('https://www.ig.com/se/index/marknader-index/')
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "igws-live-prices")))
        web_component = driver.find_element(By.CSS_SELECTOR, 'igws-live-prices')
        shadow_root = driver.execute_script('return arguments[0].shadowRoot', web_component)
        rows = shadow_root.find_elements(By.CSS_SELECTOR, '.dynamic-table__row.clickable')
        
        scraped_data = []
        for row in rows:
            index_element = row.find_element(By.CSS_SELECTOR, 'a[data-epic]')
            index = index_element.get_attribute('data-epic')
            if index in INTERESTED_EPICS:
                change_value = row.find_element(By.CSS_SELECTOR, 'span[data-field="CPC"]').text
                scraped_data.append({'Index': index, 'Change Value': change_value})
    return scraped_data

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
    scraped_data = await get_scraped_data()
    embed = Embed(
        title="\U0001F4C8 Indexterminer",
        description="Aktuella index med fördröjning på OMX, handlas även utanför normala börstider men ej helger:",
        color=0x3498db,
        timestamp=datetime.now(pytz.utc)
    )
    embed.set_footer(text="Källa: IG.com")

    for data in scraped_data:
        label = LABEL_EPICS.get(data['Index'], data['Index'])
        embed.add_field(name=label, value=f"{data['Change Value']}%", inline=True)
    
    await ctx.send(embed=embed)
    log_message(f'Sent current index to Discord.')

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