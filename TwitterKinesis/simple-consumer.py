#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 19:47:47 2019

@author: amir
"""
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql.types import  (StructField, StringType, StructType, IntegerType)
import boto3
import time
import json



#connect to kinesis stream
kinesis = boto3.client('kinesis')
shard_id = "shardId-000000000000"
pre_shard_it = kinesis.get_shard_iterator(StreamName="twitterAPI", ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

# Schema for twitter
schema = StructType([
        StructField("created_at", StringType()),
        StructField("text", StringType()),
        StructField("user", StructType([
            StructField("screen_name", StringType()),
            StructField("location", StringType()),
            StructField("followers_count", IntegerType()),
            StructField("friends_count", IntegerType())]))
        ])

while 1==1:
    tweets = kinesis.get_records(ShardIterator=shard_it, Limit=1)    
    shard_it = tweets["NextShardIterator"]
            
    #configure spark context
    conf = SparkConf().setAppName("kinesistwitterapi").setMaster("local[*]")\
    .set("spark.driver.memory","4G")\
    .set("spark.driver.maxResultSize", "2G")\
    .set("spark.jars.packages", "spark-nlp:2.0.1")\
    .set("spark.kryoserializer.buffer.max", "500m")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)
    appName, streamName, endpointUrl, regionName = "KinesisSparkTwitter", "twitterAPI", "kinesis.us-west-2.amazonaws.com", "us-west-2"
    DStream = KinesisUtils.createStream(
            ssc,  appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 5)
    parsedRDD = DStream.map(lambda rdd: json.loads(rdd[1]))
    kinesisDF = sc.createDataFrame(parsedRDD, schema)
    #screen_name = parsedRDD.map(lambda tweet: tweet['user']['screen_name'])
    print(kinesisDF)
    
    kinesisDF.createTempView("twitterapitable")
    result = kinesisDF.sql("SELECT created_at, text, user.screen_name, user.followers_count, user.friends_count, location\
                           from twitterapitable")
    print(result)
    
    result.writeStream.mode("append").parquet("s3a://twitterbucket123456")   
        
    ssc.start()
    ssc.awaitTerminationOrTimeout(10000)
       
