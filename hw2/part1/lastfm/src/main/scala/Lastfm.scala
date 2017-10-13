import org.apache.parquet.hadoop.codec.NonBlockedDecompressorStream

import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object Lastfm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    //conf.set("spark.default.parallelism", "1")
    conf.setAppName("HR")
    val sc = new SparkContext(conf)

    val movieLensDir = "hdfs://localhost:9000/lastfm-dataset-360K/"
    val ratingData = movieLensDir + "usersha1-artmbid-artname-plays.tsv"

    val rawData = sc.textFile(ratingData).map(line => line.split("\t"))
    //val rawData = data.filter(line => line != "")
    var userMap: Map[String, Int] = Map()
    var userIdx: Int = 0
    rawData.map(line => line(0)).distinct.collect.foreach(x => {
      userMap += (x -> userIdx)
      userIdx += 1
    })

    var songMap: Map[String, Int] = Map()
    var songIdx: Int = 0
    rawData.map(line => line(1)).distinct.collect.foreach(x => {
      songMap += (x -> songIdx)
      songIdx += 1
    })

    val ratings = rawData.map(line => {
      val userId = userMap(line(0))
      val songId = songMap(line(1))
      val click = line(3).toDouble

      Rating(userId, songId, click)
    })

    val test1 = ratings.randomSplit(Array(0.1,0.9))


    val rank = 10
    val numIterations = 10
    val alpha = 0.01
    val lambda = 0.01
    val model = ALS.trainImplicit(test1(0), rank, numIterations, lambda, alpha)

    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)}

    val predictions =
      model.predict(usersProducts).map{ case Rating(user, product, rate) =>
      ((user, product), rate)}
    //predictions.collect().take(15).foreach(println)
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    model.recommendProducts(1, 20).foreach(println)
    /*val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
*/

    /*
    val ratings = sc.textFile(ratingData).map { line =>
      val fields = line.split(",")
      (fields(3).toLong, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc.textFile(movieData).map ({ line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(1))
    })

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
*/


  }

}
