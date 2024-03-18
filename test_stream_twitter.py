import discord
import requests

import discord.ext.commands as commands
import discord
import os
import json
from general_utils import log_message

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')


TWEETS_FILE = 'stored_game_tweets.json'

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents = intents)


def store_tweet(tweet_data):
    tweets = []
    if os.path.exists(TWEETS_FILE):
        with open(TWEETS_FILE, 'r') as file:
            tweets = json.load(file)
    tweets.append(tweet_data)
    with open(TWEETS_FILE, 'w') as file:
        json.dump(tweets, file, indent=4)

def get_stored_tweets():
    if os.path.exists(TWEETS_FILE):
        with open(TWEETS_FILE, 'r') as file:
            tweets = json.load(file)
        return tweets
    else:
        return []

# List of author IDs to include
AUTHOR_IDS = [1163220698542641224, 1163219371485827133]


# Sometimes tweets are from account names that contain information that are not in the tweets. Like Baldurs Gate 3 twitter account posting about a new game. Add this context to the "message" and or prompt.

@bot.event
async def on_message(message):
    if message.author.id in AUTHOR_IDS:
        if message.content.startswith('[Retweeted]') or message.content.startswith('[Tweeted]'):
            if message.embeds:
                embed = message.embeds[0]
                tweet_data = {
                    'text': embed.description,
                    'author': embed.author.name,
                    'timestamp': message.created_at.isoformat(),
                    'type': 'retweet' if message.content.startswith('Retweeted') else 'tweet'
                }
                print('Stored tweet:', tweet_data['text'])
                store_tweet(tweet_data)
        
        elif message.content.startswith('[Quoted]'):
            if len(message.embeds) >= 2:
                quote_embed = message.embeds[0]
                quoted_embed = message.embeds[1]
                tweet_data = {
                    'text': f"{quote_embed.description} <quote>{quoted_embed.description}</quote>",
                    'author': quote_embed.author.name,
                    'timestamp': message.created_at.isoformat(),
                    'type': 'quote'
                }
                print('Stored tweet:', tweet_data['text'])
                store_tweet(tweet_data)

@bot.event
async def on_command(command):
    if command == '!summarize':
        tweets = get_stored_tweets()  # Implement this function to retrieve the stored tweets
        summaries = []
        for tweet in tweets:
            summary = generate_summary(tweet['text'])  # Implement this function to call the LLM API
            summaries.append(summary)
        await print(summaries)  # Implement this function to post the summaries to Discord
        await log_message(f'Summarized {len(tweets)} tweets')

def generate_summary(tweet_text):
    # Call the LLM API to generate the summary
    api_url = 'https://api.example.com/summarize'
    response = requests.post(api_url, json={'text': tweet_text})
    summary = response.json()['summary']
    return summary

bot.run(BOT_TOKEN)