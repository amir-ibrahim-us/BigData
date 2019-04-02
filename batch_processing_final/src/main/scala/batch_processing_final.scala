import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object batch_processing_final {

  def main(args:Array[String]): Unit = {

    // Do : Spark Configuration and Define SC variable
    val conf: SparkConf = new SparkConf().setAppName("Patients").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // Do : Define SqlContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Getting Data to Data Frame Directly
    val path1 = "/home/amir/Downloads/techfield_bigdata/data/mortality/events.csv"
    val path2 = "/home/amir/Downloads/techfield_bigdata/data/mortality/mortality.csv"

    val event_data_csv = sqlContext.read.option("header", false).csv(path1)
    val mortality_data_csv = sqlContext.read.option("header", false).csv(path2)

    event_data_csv.createOrReplaceTempView("event")
    mortality_data_csv.createOrReplaceTempView("mortality")

    val alive_query = "SELECT event._c0 as patient_id, COUNT(event._c1) as EventCount from event WHERE event._c0  NOT IN (Select mortality._c0 from mortality) GROUP BY event._c0 ORDER BY EventCount DESC"
    val dead_query = "SELECT event._c0 as patient_id, COUNT(event._c1) as EventCount FROM event JOIN mortality ON event._c0=mortality._c0 GROUP BY patient_id ORDER BY EventCount DESC"

    val eventCountForAlive = sqlContext.sql(alive_query)
    val eventCountForDead = sqlContext.sql(dead_query)

    val alive_result = eventCountForAlive.agg(min("EventCount") as("MinEventCount"), max("EventCount") as("MaxEventCount"), avg("EventCount") as("AvgEventCount"))
    val dead_result = eventCountForDead.agg(min("EventCount") as("MinEventCount"), max("EventCount") as("MaxEventCount"), avg("EventCount") as("AvgEventCount"))

    //Save Result to HDFS
    alive_result.write.csv("/home/amir/Documents/alive_result.csv")
    dead_result.write.csv("/home/amir/Documents/dead_result.csv")

  }

}