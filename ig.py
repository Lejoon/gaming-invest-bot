from datetime import datetime, timedelta
import pytz
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from discord import Embed
import time

CHANNEL_ID = 1161207966855348246
CUSTOM_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
INTERESTED_EPICS = ["IX.D.OMX.IFD.IP", "IX.D.DAX.IFD.IP", "IX.D.SPTRD.IFD.IP"]

def get_seconds_until(time_hour, time_minute, next_day=False):
    now = datetime.now()
    day_offset = timedelta(days=1) if next_day else timedelta(days=0)
    target_time = datetime(now.year, now.month, now.day, time_hour, time_minute) + day_offset
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

async def send_daily_message(bot, time_hour, time_minute, next_day=False):
    while True:
        await asyncio.sleep(get_seconds_until(time_hour, time_minute, next_day))
        scraped_data = await get_scraped_data()
        # If time_hour > 12 it's evening, otherwise it's morning
        title_text = "Börsen öppnar snart!" if time_hour < 12 else "Börsen har stängt!"
        description_text = "Indexterminerna indikerar följande per 08:30 sen senaste stäng:" if time_hour < 12 else "Indexterminerna i USA stängde följande per 22:00 och OMX har kvällsauktion:"
        embed = Embed(
            title=title_text,
            description=description_text,
            color=0x3498db,
            timestamp=datetime.now(pytz.utc)
        )
        # Include OMX only in the morning
        for data in scraped_data:
            label = {"IX.D.OMX.IFD.IP": "OMX", "IX.D.DAX.IFD.IP": "DAX", "IX.D.SPTRD.IFD.IP": "SP500"}.get(data['Index'], data['Index'])
            embed.add_field(name=label, value=f"{data['Change Value']}%", inline=True)
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            await channel.send(embed=embed)
            
async def send_current_index(ctx):
    scraped_data = await get_scraped_data()
    embed = Embed(
        title="Börserna just nu!",
        description="Läget är som följer på börsen just nu:",
        color=0x3498db,
        timestamp=datetime.now(pytz.utc)
    )
    # Include OMX only in the morning
    for data in scraped_data:
        label = {"IX.D.OMX.IFD.IP": "OMX", "IX.D.DAX.IFD.IP": "DAX", "IX.D.SPTRD.IFD.IP": "SP500"}.get(data['Index'], data['Index'])
        embed.add_field(name=label, value=f"{data['Change Value']}%", inline=True)
    
    await ctx.send(embed=embed)

async def daily_message_morning(bot):
    await send_daily_message(bot, 8, 45, next_day=True)

async def daily_message_evening(bot):
    await send_daily_message(bot, 22, 0)
    
async def current_index(ctx):
    await send_current_index(ctx)