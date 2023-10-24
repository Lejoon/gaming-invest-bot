from datetime import datetime, timedelta
import pytz
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from discord import Embed
import time

async def seconds_until_0845():
    stockholm = pytz.timezone('Europe/Stockholm')
    now = datetime.now(stockholm)
    next_0845 = datetime(now.year, now.month, now.day, 8, 45, tzinfo=stockholm)
    if now >= next_0845:
        next_0845 += timedelta(days=1)
    delta = next_0845 - now
    return delta.total_seconds()

async def daily_message(bot):
    channel_id = 1161207966855348246  # Replace with your channel ID
    while True:
        await asyncio.sleep(5)

        # Set up Chrome options
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Run Chrome in headless mode
        options.add_argument("--no-sandbox")

        # Initialize the Chrome WebDriver using ChromeDriverManager
        driver = webdriver.Chrome(options=options)

        
        # Navigate to the website
        driver.get('https://www.ig.com/se/index/marknader-index/')

        driver.execute_script("window.scrollTo(0, 800);")
        # Get the HTML source of the page
        html_source = driver.page_source

        # Split the HTML source into lines and return the first few lines
        html_lines = html_source.split('\n')

        # Return the first 10 lines (you can adjust the number)
        print('\n'.join(html_lines[:10]))


        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "igws-live-prices"))
        )
        time.sleep(3)
        print('Looking for accept button')
        wait = WebDriverWait(driver, 20)
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#onetrust-accept-btn-handler"))).click()

        interested_epics = ["IX.D.OMX.IFD.IP", "IX.D.DAX.IFD.IP", "IX.D.SPTRD.IFD.IP"]
        # First, get the web component that hosts the shadow root
        web_component = driver.find_element(By.CSS_SELECTOR, 'igws-live-prices')

        # Then, switch to its shadow root
        shadow_root = driver.execute_script('return arguments[0].shadowRoot', web_component)

        # Now, you can find elements inside the shadow root
        rows = shadow_root.find_elements(By.CSS_SELECTOR, '.dynamic-table__row.clickable')

        print(rows)  # This should now print the elements inside the shadow root

        scraped_data = []

        for row in rows:
            index_element = row.find_element(By.CSS_SELECTOR, 'a[data-epic]')
            index = index_element.get_attribute('data-epic')
            if index in interested_epics:
                change_value = row.find_element(By.CSS_SELECTOR, 'span[data-field="CPC"]').text
                scraped_data.append({
                    'Index': index,
                    'Change Value': change_value
                })
                
        embed_title = "Börsen öppnar snart"
        embed_description = "Indexterminerna indikerar följande per 08:30:"
        embed_color = 0x3498db  # You can change this to your preferred color

        embed = Embed(title=embed_title, description=embed_description, color=embed_color)

        for data in scraped_data:
            label = {
                "IX.D.OMX.IFD.IP": "OMX",
                "IX.D.DAX.IFD.IP": "DAX",
                "IX.D.SPTRD.IFD.IP": "SP500"
            }.get(data['Index'], data['Index'])

            embed.add_field(name=label, value=f"{data['Change Value']}%", inline=True)

        # Close the webdriver
        driver.quit()
    
        channel = bot.get_channel(channel_id)
        if channel:
            await channel.send(embed=embed)




