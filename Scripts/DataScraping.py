# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 14:17:27 2023

@author: kalya
"""

import tweepy
import configparser
import pandas as pd
import datetime
import os.path
import json


# Read config
config = configparser.ConfigParser()
config.read('Config.ini')

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

# Authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

keywords = ['Petsmart']  #, 'Petco', 'Chewy.com'
columns = ['Id', 'Date', 'Tweet', 'Location', 'Retweet Count', 'Favorite Count']
logs = [] # Initialize an empty list for logs

for keyword in keywords:
    filename = '{}_twitter_data.csv'.format(keyword)

    # Check if the file already exists
    if os.path.isfile(filename):
        # Read existing data and columns
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
        # Create a new DataFrame with the required columns
        df = pd.DataFrame(columns=columns)

    # Search for tweets
    data = []
    for tweet in tweepy.Cursor(api.search_tweets, q=keyword, tweet_mode='extended').items(50):
        if not hasattr(tweet, 'retweeted_status'):
            new_data = [tweet.id, tweet.created_at, tweet.full_text, tweet.user.location, tweet.retweet_count, tweet.favorite_count]
            data.append(new_data)

    # Add new data to the DataFrame
    new_df = pd.DataFrame(data, columns=columns)
    df = pd.concat([df, new_df], axis=0, ignore_index=True)

    # Save data to CSV file
    df.to_csv(filename, index=False)

    # Log information
    num_rows = len(df.index)
    runtime = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logs.append({'keyword': keyword, 'num_rows': num_rows, 'runtime': runtime}) # Append a dictionary with keyword information to logs

# Write logs to JSON file
with open('twitter_data_logs_{}.json'.format(datetime.now().strftime('%Y%m%d%H%M%S')), 'w') as f:
    json.dump(logs, f)
    f.write('\n')
