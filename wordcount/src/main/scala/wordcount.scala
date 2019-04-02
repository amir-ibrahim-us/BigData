import org.apache.spark._

object wordcount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("OneCar").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val lines = sc.textFile("/usr/maria_dev/wordCount_input/shakespeare.txt")

    val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    wordCount.saveAsTextFile("/usr/maria_dev/wordCount_output/")


  }
}
