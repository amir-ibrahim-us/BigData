#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 23:08:53 2019

@author: amir
"""

import os
import json

import boto3
import tweepy
import twitterCreds

consumer_key = twitterCreds.consumer_key
consumer_secret = twitterCreds.consumer_secret
access_token_key = twitterCreds.access_token_key
access_token_secret = twitterCreds.access_token_secret

auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
auth.set_access_token(access_token_key, access_token_secret)

kinesis_client = boto3.client('kinesis')

class KinesisStreamProducer(tweepy.StreamListener):

	def __init__(self, kinesis_client):
		self.kinesis_client = kinesis_client

	def on_data(self, data):
		tweet = json.loads(data)
		self.kinesis_client.put_record(StreamName='twitter', Data=tweet["text"], PartitionKey="key")
		print("Publishing record to the stream: ", tweet)
		return True

	def on_error(self, status):
		print("Error: " + str(status))

def main():
	mylistener = KinesisStreamProducer(kinesis_client)
	myStream = tweepy.Stream(auth = auth, listener = mylistener)
	myStream.filter(track=['#money'])

if __name__ == "__main__":
	main()
