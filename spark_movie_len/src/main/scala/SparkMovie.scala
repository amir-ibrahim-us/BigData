import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


object SparkMovie {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("movie_lens").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //(1)Load ratings data into RDD
    val path = "/home/amir/Downloads/data/ratings.dat"
    val data_rdd = sc.textFile(path).map(_.split("::")).map(x => Row(x(0), x(1), x(2),x(3)))

    //(2)How many lines does the data_rdd RDD contain?
    data_rdd.count() ///Answer = 1000209

    //(3) How many time rating 1 has been given
    val ratings_rdd = data_rdd.map(lines => (lines(2), 1)).reduceByKey(_+_)
    ratings_rdd.take(5)  //rating 1 = 56,174

    //(4) Count how many unique movies have been rated
    val unique = data_rdd.map(x => (x(1),1)).distinct().reduceByKey(_+_)
      unique.count()  // Answer = 3706

    //(5) user with most rating
    val user_rating = data_rdd.map(x => (x(0), 1)).reduceByKey(_+_).sortBy(_._2, false)
    user_rating.take(10)  //4169

    //(6) Which user gave most '5' ratings?
    val most_rating = data_rdd.map(x => (x(0), x(2))).map(y => y())  //if(x(2).equals(5)){(x(0), x(2))}else{(0,0)})                                //.reduceByKey(_+_).sortBy(_._2, false)
    most_rating.take(5)

    //(7) Which movie was rated most times?
    val pop_movie = data_rdd.map(x => (x(1), 1)).reduceByKey(_+_).sortBy(_._2, false)
    pop_movie.take(5)  //Answer = (2858, 3428)

    //(8) Read the movies and users into RDDs. How many records are there in each RDD
    val movies_path = "/home/amir/Downloads/data/movies.dat"
    val users_path = "/home/amir/Downloads/data/users.dat"

    val movies_count = sc.textFile(movies_path).map(_.split("::")).map(x => Row(x(0), x(1), x(2)))
    movies_count.count() //Answer = 3883

    val users_count = sc.textFile(users_path).map(_.split("::")).map(x => Row(x(0), x(1), x(2),x(3),x(4)))
    users_count.count()  //Answer = 6040

    //(9) How many of the movies are a comedy?
    val comedy = movies_count.map(x => x(2)).map(x => (x, 1)).reduceByKey(_+_).sortBy(_._2, false)
    comedy.collect()  // Answer == 521 comedy alone
    val w = comedy.reduceByKey(_+_).sortBy(_._2, false)
    w.take(10)

    //(10) Which comedy has the most ratings?
    val most_rated_comedy = movies_count.map(a => (a(0),a(2)))
    for(b <- most_rated_comedy){
      if(b == "Comedy"){array_list}
    }
    most_rated_comedy.take(10)


  }

}
