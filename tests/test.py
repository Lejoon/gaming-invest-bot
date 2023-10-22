from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

print('hi')

def fetch_steam_top_sellers():
    # Initialize Selenium WebDriver
    driver = webdriver.Chrome()

    url = "https://store.steampowered.com/search/?filter=globaltopsellers"
    driver.get(url)

    # Scroll down the page to load more items
    for i in range(20):  # Increase the range to scroll more
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Wait for the new items to load

    # Get the page source and parse with BeautifulSoup
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    games = []
    count = 1

    for game_div in soup.select('.search_result_row')[:250]:
        title_elements = game_div.select('.title')
        price_elements = game_div.select('.discount_pct')

        title = title_elements[0].text if title_elements else "Unknown title"
        price = price_elements[0].text.strip() if price_elements else ""

        if price_elements:
            games.append(f"{count}. {title} **({price})**")
        else:
            games.append(f"{count}. {title}")
        
        count += 1

    driver.quit()  # Close the browser
    return games

# Fetch and print the top 250 games
top_games = fetch_steam_top_sellers()
for game in top_games:
    print(game)




# ------ Stock data ------

async def fetch_stock_data(ticker):
    api_key = os.getenv('ALPHA_KEY')
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.text()
    
    stock_data = json.loads(data)
    if 'Error Message' in stock_data or 'Note' in stock_data:
        return None  # Invalid symbol or rate limiting
    
    # Check if 'Time Series (Daily)' is in the response
    if 'Time Series (Daily)' not in stock_data:
        return None  # Handle this case in the main function
    
    
    # Extract daily data
    daily_data = stock_data['Time Series (Daily)']
    
    # Calculate moving averages and other statistics
    closing_prices = [float(day['4. close']) for date, day in daily_data.items()]
    latest_close = closing_prices[0]
    ma20 = sum(closing_prices[:20]) / 20
    ma50 = sum(closing_prices[:50]) / 50
    ma200 = sum(closing_prices[:200]) / 200
    percentual_change = ((latest_close - closing_prices[1]) / closing_prices[1]) * 100
    
    return {
        "latest_close": latest_close,
        "ma20": ma20,
        "ma50": ma50,
        "ma200": ma200,
        "percentual_change": percentual_change,
    }

@bot.command()
async def stock(ctx, ticker: str):
    stock_data = await fetch_stock_data(ticker.upper())
    if stock_data:
        arrow = ":arrow_up:" if stock_data['percentual_change'] >= 0 else ":arrow_down:"
        response = f"**${ticker.upper()}** ({stock_data['latest_close']:.1f}, {arrow} {stock_data['percentual_change']:.2f}%, MA20: {stock_data['ma20']:.0f}, MA50: {stock_data['ma50']:.0f}, MA200: {stock_data['ma200']:.0f})"
        await ctx.send(response)
    else:
        await ctx.send(f"Could not fetch data for {ticker.upper()} or you've hit the API rate limit.")


#@bot.command()
#async def stock(ctx, ticker: str):
#    stock_data = await fetch_stock_data(ticker.upper())
#    if stock_data:
#        embed = discord.Embed(title=f"Stock Data for {ticker.upper()}", color=0x00ff00)
#        embed.add_field(name="Latest Close", value=f"${stock_data['latest_close']:.2f}", inline=True)
#        embed.add_field(name="Percentual Change", value=f"{stock_data['percentual_change']:.2f}%", inline=True)
#        embed.add_field(name="MA20", value=f"${stock_data['ma20']:.2f}", inline=True)
#        embed.add_field(name="MA50", value=f"${stock_data['ma50']:.2f}", inline=True)
#        embed.add_field(name="MA200", value=f"${stock_data['ma200']:.2f}", inline=True)
#        await ctx.send(embed=embed)
#    else:
#        await ctx.send(f"Could not fetch data for {ticker.upper()} or you've hit the API rate limit.")
