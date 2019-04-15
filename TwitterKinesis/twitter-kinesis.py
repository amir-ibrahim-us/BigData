#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 14:03:01 2019

@author: amir
"""

from TwitterAPI import TwitterAPI
import boto3
import json
import twitterCreds

# Twitter Credentials

consumer_key = twitterCreds.consumer_key
consumer_secret = twitterCreds.consumer_secret
access_token_key = twitterCreds.access_token_key
access_token_secret = twitterCreds.access_token_secret

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

kinesis = boto3.client('kinesis')

#r = api.request('statuses/filter', {'locations':'-90,-90,90,90'})
r = api.request('search/tweets', {'q':['money','food']})
tweets = []
count = 0


for item in r:
    jsonItem = json.dumps(item)
    tweets.append({'Data':jsonItem, 'PartitionKey':"filler"})
    print(tweets)
    count += 1
    if count == 100:
        kinesis.put_record(StreamName="twitterAPI", Records=tweets)
        count = 0
        tweets = []