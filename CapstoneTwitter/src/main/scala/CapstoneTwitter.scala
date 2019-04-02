import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object CapstoneTwitter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("TwitterAPI").getOrCreate()

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
      .option("subscribe", "money")
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
      .format("console")
      .format("json")
      .trigger(ProcessingTime("60 seconds"))
      .option("path","/home/amir/Documents/TweetFeed")
      .option("checkpointLocation", "/tmp/TwitterCheckpoint/")
      .start()

    result.awaitTermination()


  }
}

