import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType}


object SparkToHive {

  // Do : Spark Configuration and Define SC variable
  val conf: SparkConf = new SparkConf().setAppName("Patients").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  // Do : Define SqlContext
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val path = "hdfs:/user/maria_dev/patient_input/events.csv"
  val eventDataRDD = sqlContext.read.csv(path)

  val eventDataDF = eventDataRDD.toDF()

  val schema = StructType(List(
    StructField("patient_id", IntegerType, false),
    StructField("event_id", StringType, false),
    StructField("description", StringType, true),
    StructField("timestamp", StringType, false),
    StructField("value", StringType, true)
  ))

  val eventDataDFSchema = schema(eventDataDF, schema)


  eventDataDF.write.mode(SaveMode.Overwrite).saveAsTable("hive_spark.sparkToHive")


}
