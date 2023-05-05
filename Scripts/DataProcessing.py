# -*- coding: utf-8 -*-

import pandas as pd
import datetime
import re



def remove_username_and_links(tweet):
    if pd.isnull(tweet):
        return ""
    advertize = "USA STORE ONLY"
    hiring = "#hiring"
    if((advertize in tweet) | (hiring in tweet)):
        return ""
    return re.sub(r'(@\w+)|(http\S+)|\s{2,}', ' ', str(tweet)).strip()

def convert_to_datetime(date):
    if(len(date) > 11):
        date = date[0:10]
        return datetime.strptime(date, '%Y-%m-%d').date()
    return date
    
def extract_state(location):
    if pd.isnull(location):
        return ""
    
    words = re.split(r',|\s', location.upper())

    for i in range(len(words)):
        if words[i] in state_dict:
            words[i] = state_dict[words[i]]
        if words[i] in states:
            return words[i].title()
    
    for i in range(len(words)):
        if words[i] in usa:
            return "USA"
        elif words[i] in ["Canada","Puerto Rico"]:
            return words[i]
            
    return ""

states = ['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA', 
          'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE', 'MARYLAND',
          'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA', 
          'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 
          'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH', 
          'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']

state_dict = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING"
}

usa = ['USA','US','AMERICA','STATES']

file_list=["Petsmart_Twitter_Data.csv", "Petco_twitter_data.csv", "Chewy.com_twitter_data.csv"]
for filename in file_list:
    df = pd.read_csv(filename)
    df["Tweet"] = df["Tweet"].apply(lambda tweet: remove_username_and_links(tweet))
    #df["Date"] = df["Date"].apply(lambda date: convert_to_datetime(date))
    df["Location"] = df["Location"].apply(lambda location: extract_state(location))
    df["Total Tweets Count"] = df["Retweet Count"] + 1
    df.drop(df[df["Tweet"] == ""].index, inplace=True)
    # save modified dataframe to file
    df.to_csv(filename, index=False)

