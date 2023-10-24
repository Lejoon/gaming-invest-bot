from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio

async def test():
    # Initialize webdriver
    driver = webdriver.Chrome()

    # Navigate to the website
    driver.get('https://www.ig.com/se/index/marknader-index/')

    driver.execute_script("window.scrollTo(0, 800);")

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "igws-live-prices"))
    )
    wait = WebDriverWait(driver, 10)
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

    message = "Börsen öppnar snart, indexterminerna indikerar följande per 08:30:\n"
    for data in scraped_data:
        label = {
            "IX.D.OMX.IFD.IP": "OMX",
            "IX.D.DAX.IFD.IP": "DAX",
            "IX.D.SPTRD.IFD.IP": "SP500"
        }.get(data['Index'], data['Index'])

        message += f"**{label}:** {data['Change Value']}%\n"
    print(message)

    # Close the webdriver
    driver.quit()

loop = asyncio.get_event_loop()
loop.run_until_complete(test())
