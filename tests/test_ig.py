import aiohttp
from bs4 import BeautifulSoup
import asyncio

async def test():
    async with aiohttp.ClientSession() as session:
            async with session.get('https://www.ig.com/se/index/marknader-index/') as resp:
                    html_content = await resp.text()
            # write content to a file
            with open('test.html', 'w') as f:
                f.write(html_content)
            
            #print(html_content)
            soup = BeautifulSoup(html_content, 'html.parser')
            interested_epics = ["IX.D.OMX.IFD.IP", "IX.D.DAX.IFD.IP", "IX.D.SPTRD.IFD.IP"]
            rows = soup.find_all('div', {'class': 'dynamic-table__row'})
            print(rows)
            scraped_data = []
            
            for row in rows:
                index = row.find('a', {'data-epic': True}).get('data-epic')
                if index in interested_epics:
                    change_value = row.find('span', {'data-field': 'CPC'}).text
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
            
loop = asyncio.get_event_loop()
loop.run_until_complete(test())