import org.apache.parquet.hadoop.codec.NonBlockedDecompressorStream

import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object MovieLensALS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.default.parallelism", "1")
    conf.setAppName("HR")
    val sc = new SparkContext(conf)

    val movieLensDir = "hdfs://localhost:9000/movie_ratings/"
    val ratingData = movieLensDir + "ratings.csv"
    val movieData = movieLensDir + "movies.csv"

    val ratings = sc.textFile(ratingData).map { line =>
      val fields = line.split(",")
      (fields(3).toLong, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc.textFile(movieData).map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(1))
    }.collect().toMap

    //sc.stop()

    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")

    val splits = ratings.randomSplit(Array(0.8,0.1,0.1))
    val training = splits(0)
    val validation = splits(1)
    val test = splits(2)
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()



    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    /*val ranks = List(8, 12)
    val lambdas = List(0.1, 10)
    val nulIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLam*/
    val ranks = 8
    val lambdas = 10.0
    val numIters = 15
    val model = ALS.train(training.values, ranks, numIters, lambdas)

    println(model.predict(1, 1))






  }
}
