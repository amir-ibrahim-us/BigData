#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 13:32:05 2019

@author: amir
"""

import boto3

client = boto3.client('kinesis')
response = client.create_stream(
        StreamName = 'twitterAPI',
        ShardCount = 1
        )