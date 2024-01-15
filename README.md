This project is a Discord bot that provides various functionalities related to gaming and financial data. It fetches data from different sources like Steam, Placera, Swedish Financial Supervisory Authority and IG, and provides commands to interact with this data.

The bot is live at: https://discord.gg/V48pmp8Cte


## Features

1. Steam Top Sellers: The bot fetches the top selling games on Steam and provides a command to display this data.
2. Short Selling Data: The bot fetches short selling data from the Swedish Financial Supervisory Authority and provides a command to display this data for a specific company.
3. Earnings Data: The bot provides a command to display earnings data for gaming companies.
4. Index Data: The bot fetches index data from IG and provides a command to display this data.
5. Placera Updates: The bot fetches updates from Placera and sends them to a specific Discord channel.

## Setup

1. Clone the repository.
2. Install the required Python packages using pip:
```txt
pip install -r requirements.txt
```

3. Set up the necessary environment variables. You need to provide your Discord bot token and your Steam API key. You can do this by creating a .env file in the root directory of the project with the following content:
```python
BOT_TOKEN=your_discord_bot_token
STEAM_API_KEY=your_steam_api_key
```


4. Run the bot:
```txt
python main.py
```

## Commands

- !gts: Displays the top 15 global sellers on Steam.
- !short <company_name>: Displays short selling data for the specified company.
- !earnings <date>: Displays earnings data for the specified date.
- !index: Displays current index data.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
License

MIT
