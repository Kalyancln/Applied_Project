# -*- coding: utf-8 -*-

# Twitter Data Scraping Using Tweepy

# Import necessary libraries
import tweepy
import configparser
import pandas as pd
import datetime
import os.path
import json

# Read configuration file
config = configparser.ConfigParser()
config.read('Config.ini')

# Retrieve API keys and access tokens from the configuration file
api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

# Authenticate using Tweepy
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Define a list of keywords to search for
keywords = ['Petsmart']  #, 'Petco', 'Chewy.com'

# Define the columns to include in the output CSV file
columns = ['Id', 'Date', 'Tweet', 'Location', 'Retweet Count', 'Favorite Count']

# Initialize an empty list for logs
logs = []

# For each keyword, search for tweets and save the results to a CSV file
for keyword in keywords:
    filename = '{}_twitter_data.csv'.format(keyword)

    # Check if the CSV file already exists
    if os.path.isfile(filename):
        # If the file exists, read it into a DataFrame
        df = pd.read_csv(filename)

        # Get the existing columns and their order
        existing_columns = df.columns.tolist()

        # Find the new columns to add
        new_columns = list(set(columns) - set(existing_columns))

        # Add the new columns with empty values to the DataFrame
        for col in new_columns:
            df[col] = ''

        # Reorder the columns to match the desired order
        df = df[columns]

    else:
        # If the file doesn't exist, create a new DataFrame with the required columns
        df = pd.DataFrame(columns=columns)

    # Search for tweets containing the keyword using Tweepy
    data = []
    for tweet in tweepy.Cursor(api.search_tweets, q=keyword, tweet_mode='extended').items(50):
        if not hasattr(tweet, 'retweeted_status'):
            # Extract relevant data from the tweet object
            new_data = [tweet.id, tweet.created_at, tweet.full_text, tweet.user.location, tweet.retweet_count, tweet.favorite_count]
            data.append(new_data)

    # Add new data to the DataFrame
    new_df = pd.DataFrame(data, columns=columns)
    df = pd.concat([df, new_df], axis=0, ignore_index=True)

    # Save data to CSV file
    df.to_csv(filename, index=False)

    # Log information about the keyword and the number of rows in the CSV file
    num_rows = len(df.index)
    runtime = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logs.append({'keyword': keyword, 'num_rows': num_rows, 'runtime': runtime})

# Write logs to a JSON file with a timestamp in the filename
with open('twitter_data_logs_{}.json'.format(datetime.now().strftime('%Y%m%d%H%M%S')), 'w') as f:
    json.dump(logs, f)
    f.write('\n')
