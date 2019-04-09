package com.ibrahim.amir


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions



object ElasticSearchDestination {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .master("local[*]")
      .appName("ElasticearchTwitter")
      .getOrCreate()

    val schema = StructType(List(
      StructField("created_at", StringType, false),
      StructField("text", StringType, false),
      StructField("user", StructType(List(
        StructField("screen_name", StringType, true),
        StructField("location", StringType, true),
        StructField("followers_count", LongType, true),
        StructField("friends_count", LongType, true)
      )))
    ))
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "coffee")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .load()

    val rawValues = df.selectExpr("CAST(value AS STRING)").as[String]
    val jsonValues = rawValues.select(from_json($"value", schema) as "data")
    val tweetsData = jsonValues.select("data.created_at","data.text", "data.user")

    tweetsData.createOrReplaceTempView("twitter")
    val result = spark.sql("select created_at, text, user.screen_name," +
      "user.followers_count, user.friends_count, user.location from twitter")
      .writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .trigger(ProcessingTime("1 seconds"))
      .option("checkpointLocation", "/tmp/TwitterCheckpoint/")
      .start("twitter/json")

    result.awaitTermination()
    Thread.sleep(9000)


  }
}
