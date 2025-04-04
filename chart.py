import pandas as pd
from lightweight_charts.widgets import StreamlitChart, JupyterChart
from selenium import webdriver
from selenium.webdriver.chrome.options import Options



import requests
from pydantic import BaseModel, ValidationError, Field
from typing import List, Optional
from enum import Enum
from time import sleep
import pandas as pd
from datetime import datetime
from pytz import timezone
import mplfinance as mpf
import io
import discord
from discord.ext import commands
import os
import asyncio


# Load environment variables
from dotenv import load_dotenv
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents = intents)

class OHLC(BaseModel):
    timestamp: int
    open: float
    close: float
    low: float
    high: float
    totalVolumeTraded: int

class Resolution(BaseModel):
    chartResolution: str
    availableResolutions: List[str]

class Metadata(BaseModel):
    resolution: Resolution

class PriceChartData(BaseModel):
    ohlc: List[OHLC]
    metadata: Metadata
    previousClosingPrice: float
    from_: str = Field(..., alias='from')
    to: str

class Price(BaseModel):
    last: Optional[str]
    currency: str
    todayChangePercent: str
    todayChangeValue: str
    todayChangeDirection: int
    threeMonthsAgoChangePercent: Optional[str]
    threeMonthsAgoChangeDirection: int
    spread: Optional[str]

class StockSector(BaseModel):
    id: int
    level: int
    name: str
    englishName: str
    highlightedName: Optional[str]

class SearchHit(BaseModel):
    type: str
    title: str
    highlightedTitle: str
    description: str
    highlightedDescription: str
    path: Optional[str]
    flagCode: Optional[str]
    orderBookId: str
    urlSlugName: str
    tradeable: bool
    sellable: bool
    buyable: bool
    price: Price
    stockSectors: List[StockSector]
    fundTags: List
    marketPlaceName: str
    subType: Optional[str]
    highlightedSubType: str

class AvanzaSearchResult(BaseModel):
    totalNumberOfHits: int
    hits: List[SearchHit]
    searchQuery: str
    pagination: dict
    searchFilter: dict
    facets: dict

def avanza_search_body(query: str) -> dict:
    return {
        "query": query
    }

async def search_avanza(query: str) -> Optional[AvanzaSearchResult]:
    url = "https://www.avanza.se/_api/search/filtered-search"
    headers = {'Content-Type': 'application/json'}
    post_body = avanza_search_body(query)
    response = requests.post(url, headers=headers, json=post_body)
    
    if response.status_code == 200:
        if response.json()["totalNumberOfHits"] > 0:
            # Only parse the first hit
            parsed_response = AvanzaSearchResult.model_validate(response.json())
            return parsed_response
        else:
            return None
    else:
        return None
    return None

class Resolution(str, Enum):
    DAY = "day"
    WEEK = "week"
    HOUR = "hour"
    
class Period(str, Enum):
    ONE_D = "one_day"
    ONE_W = "one_week"
    ONE_M = "one_month"
    THREE_M = "three_months"
    YTD = "this_year"
    ONE_Y = "one_year"
    THREE_Y = "three_years"
    FIVE_Y = "five_years"
    ALL = "infinity"    
    
async def get_isin(order_book_id: str):
    url = f"https://www.avanza.se/_api/market-guide/stock/{order_book_id}"
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    return response.json()["isin"]
    
async def get_latest_events(ISIN: str):
    url = f"https://www.avanza.se/_api/quartr-integration/company/{ISIN}"
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        events = data["company"]["events"]

        sorted_events = sorted(events, key=lambda x: datetime.strptime(x["eventDate"], "%Y-%m-%dT%H:%M:%S%z"), reverse=True)

        # Format the event date and title, and add links to available URLs
        formatted_events = [
            f"{event['eventTitle']}, " +
            f"{datetime.strptime(event['eventDate'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d')}: " +
            " ".join(filter(None, [
                f"[aud]({event['audioUrl']})" if event['audioUrl'] else "",
                f"[pdf]({event['pdfUrl']})" if event['pdfUrl'] else "",
                f"[report]({event['reportUrl']})" if event['reportUrl'] else ""
            ]))
            for event in sorted_events[:5]]
            
        return "\n".join(formatted_events)        

    else:
        return f"Request failed with status code: {response.status_code}"

    
async def get_offhours_data(order_book_id: str):
    url = f"https://www.avanza.se/_push/market-offhours-price/latest/{order_book_id}"
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers).json()
    """
    This returns JSON in the following style: 
    
    {"quote":null,"status":"TRADING"}
    
    if it's currently live trading. Otherwise it will give a quote after hours:
    
    {"quote":{"buy":174.8000,"sell":175.0500,"last":174.9000,"highest":175.7500,"lowest":155.0600,"change":18.9000,"changePercent":12.1200,"updated":"2024-04-25T20:05:33.179Z","timeOfLast":"2024-04-25T20:05:33.179Z"},"status":"POST_MARKET"}
    """
    status_code = response["status"]
    if status_code == "POST_MARKET" or status_code == "PRE_MARKET":
        return response["quote"]["changePercent"], response["quote"]["last"] + response["quote"]["change"]
    else:
        return None, None


async def get_quotes(order_book_id: str):
    """
    The following output is given:
    {"buy":437.69,"sell":437.83,"last":437.91,"highest":445.77,"lowest":414.50,"change":-55.59,"changePercent":-11.26,"spread":0.03,"timeOfLast":1714066345122,"totalValueTraded":13196214346.0012,"totalVolumeTraded":63217358,"updated":1714066345122,"volumeWeightedAveragePrice":430.25}
    """
    return None

async def get_chart_data(
    order_book_id: str,
    period: str,
    resolution: Optional[Resolution] = None,
):
    """
    Return chart data for an order book for the specified time period with given resolution
    """

    #TODO: use options
    options = {"timePeriod": period.lower()}

    if resolution is not None:
        #TODO: use resolution
        options["resolution"] = resolution.lower()

    for _ in range(3):
        try:
            url = f"https://www.avanza.se/_api/price-chart/stock/{order_book_id}?timePeriod={period}"
            headers = {'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers)
            
            ohlc_data = response.json()["ohlc"]

            # Format the data for the lightweight-charts
            chart_data = [
                {
                    'time': ohlc['timestamp'],
                    'open': ohlc['open'],
                    'high': ohlc['high'],
                    'low': ohlc['low'],
                    'close': ohlc['close'],
                    'volume': ohlc['totalVolumeTraded'],
                }
                for ohlc in ohlc_data
            ]
            
            # Make into DF
            df = pd.DataFrame(chart_data)
            
            df["time"] = [
            datetime.fromtimestamp(x / 1000).astimezone(timezone("Europe/Stockholm"))
            for x in df["time"]]
            
            df = df.set_index(pd.to_datetime(df['time']))
            df['MA10'] = df['close'].rolling(window=10).mean()
            df['MA50'] = df['close'].rolling(window=50).mean()
            df['MA200'] = df['close'].rolling(window=200).mean()

            return pd.DataFrame(df)


        except ValidationError as e:
            print(f"Validation error: {e}")
            sleep(3)
        except Exception as e:
            print(f"Error fetching chart data: {e}")
            sleep(3)

def plot_chart_data(df: pd.DataFrame, title: str):
    """
    Plot chart data
    """
    try:
        # Calculate the MA10, MA50, MA200 at the last time step.
        
        
        # Only keep one month worth of data
        df = df.tail(90)

        width_config={'candle_linewidth':1, 'candle_width':0.525, 'volume_width': 0.525}

        mc = mpf.make_marketcolors(base_mpf_style='yahoo')
        s  = mpf.make_mpf_style(base_mpf_style='yahoo',
                                marketcolors=mc)

        image_stream = io.BytesIO()
        
        mas = df[['MA200', 'MA50', 'MA10']]
        # Make three different MA colors
        colors = ['#f60c0c', '#fb6500', '#f6c309']
        mas_plot = [mpf.make_addplot(mas[f'MA{i}'], type='line', color=colors[j], width=1, alpha=0.5) for j, i in enumerate([200, 50, 10])]
        
        binance_dark = {
        "base_mpl_style": "dark_background",
        "marketcolors": {
            "candle": {"up": "#26a69a", "down": "#ef5350"},  
            "edge": {"up": "#26a69a", "down": "#ef5350"},  
            "wick": {"up": "#26a69a", "down": "#ef5350"},  
            "ohlc": {"up": "green", "down": "red"},
            "volume": {"up": "#26a69a", "down": "#ef5350"},  
            "vcedge": {"up": "#26a69a", "down": "#ef5350"},  
            "vcdopcod": False,
            "alpha": 1,
            },
            "mavcolors": ("#ad7739", "#a63ab2", "#62b8ba"),
            "facecolor": "#131722",
            "backcolor": "#131722",
            "gridcolor": "#2c2e31",
            "gridstyle": "--",
            "y_on_right": True,
            "rc": {
                "axes.grid": True,
                "axes.grid.axis": "y",
                "axes.edgecolor": "#474d56",
                "axes.titlecolor": "red",
                "figure.facecolor": "#161a1e",
                "figure.titlesize": "x-large",
                "figure.titleweight": "semibold",
            },
            "base_mpf_style": "binance-dark",
        }
        
        mpf.plot(df, 
                 volume=True, 
                 type='candle', 
                 savefig=image_stream, 
                 style=binance_dark,
                 update_width_config=width_config,
                 title=title,
                 tight_layout=True,
                 xrotation=20,
                 addplot=mas_plot)
        
        image_stream.seek(0)
        return image_stream

    except Exception as e:
        print(f"Chart error: {e}")
        
def plot_light_chart_data(df: pd.DataFrame, title: str, legend: str, lastprice: float):
    """
    Plot lightweight chart data
    """
    try:
        # Calculate the MA10, MA50, MA200 at the last time step.
        
        chart = StreamlitChart(width=900, height=600)

        chart.watermark(title)
        chart.legend(visible=True, ohlc=True, text = legend, font_size=14, font_family = 'Monaco')
        #chart.topbar.textbox('symbol', title)
        if lastprice:
            chart.horizontal_line(price=lastprice, style='dotted')
        
        chart.set(df)
        chart.load()
        html = chart._html
        html += "</script></body></html>"

        image_stream = render_html_string_to_image(html)
   
        return image_stream

    except Exception as e:
        print(f"Chart error: {e}")

async def report_command(ctx, *, company_name):
    result = await search_avanza(company_name)
    
    if result: 
        hit = result.hits[0]
        order_book_id = hit.orderBookId
        isin = await get_isin(order_book_id)
        await ctx.send(f"{hit.title}:")
        await ctx.send(f"{await get_latest_events(isin)}")
    else:
        await ctx.send(f"No hits found.")
    
async def chart_command(ctx, *, company_name, test=False):
    result = await search_avanza(company_name)
    order_book_id = ''
    name = ''
    if result:
        hit = result.hits[0]
        order_book_id = hit.orderBookId
        name = hit.title
        # Correctly await the asynchronous function here
        chart_data = await get_chart_data(order_book_id, "one_year", Resolution.DAY)
        if chart_data is not None:
            legend = ""
            off_hours_change_percent, off_hours_price = await get_offhours_data(order_book_id)
            
            if off_hours_change_percent is not None:
                legend +=f'Last: {hit.price.last}, off hours: {off_hours_change_percent}%'
            else:
                legend +=f'Last: {hit.price.last}'
                
            legend += f", MA10: {chart_data['MA10'].iloc[-1]:.1f}, MA50: {chart_data['MA50'].iloc[-1]:.1f}, MA200: {chart_data['MA200'].iloc[-1]:.1f}"
            image = plot_light_chart_data(chart_data, title=name, legend=legend, lastprice = off_hours_price)
            if image:
                await ctx.send(file=discord.File(image, filename='chart_plot.png'))
            else:
                await ctx.send("Failed to generate chart image.")
        else:
            await ctx.send("Failed to retrieve chart data.")
    else:
        await ctx.send(f'No hits found.')

def testing():  
    print('testing')
    query = "Embracer"

    # Create an event loop
    loop = asyncio.get_event_loop()
    
    # Correctly call search_avanza asynchronously if needed
    # For example:
    search_result_future = search_avanza(query)
    search_result = loop.run_until_complete(search_result_future)
    
    # Return first hit
    if search_result:
        hit = search_result.hits[0]
        print(hit.title, hit.type, hit.orderBookId, hit.price.last)
        order_book_id = hit.orderBookId
    else:
        print("No hits found.")
    
    # Run the get_chart_data coroutine within the event loop
    chart_data_future = get_chart_data(order_book_id, "one_year", Resolution.DAY)
    chart_data = loop.run_until_complete(chart_data_future)
    
    # Now you can use chart_data as intended
    if chart_data is not None:
        print(chart_data)
        image_stream = plot_chart_data(chart_data, title=hit.title)
        if image_stream:
            # Assuming you have a way to display or save image_stream
            pass


def test_report(query: str):
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(search_avanza(query))
    if result:
        hit = result.hits[0]
        order_book_id = hit.orderBookId
        isin = loop.run_until_complete(get_isin(order_book_id))
        print(f"{hit.title}:")
        print(f"{loop.run_until_complete(get_latest_events(isin))}")
    else:
        print(f"No hits found.")
        
        
def render_html_string_to_image(html_string):
    # Set up the Chrome driver options for headless mode
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Initialize the Chrome driver
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Encode HTML string in base64
        import base64
        encoded = base64.b64encode(html_string.encode('utf-8')).decode('utf-8')
        url = f'data:text/html;base64,{encoded}'

        # Load the HTML content
        driver.get(url)
        # Set the window size for screenshot
        driver.set_window_size(900, 600)
    

        # Take a screenshot and save it
        from io import BytesIO
        image_stream = BytesIO()
        driver.get_screenshot_as_png()
        image_stream.write(driver.get_screenshot_as_png())
        image_stream.seek(0)
        return image_stream
    finally:
        driver.quit()

if __name__ == "__main__":
    #testing()
    # Await get_latest_events
    test_report("evolution")
    #bot.run(BOT_TOKEN)