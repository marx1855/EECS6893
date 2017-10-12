import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession


object News {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("tfidf")
      .master("local")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    //spark.setLogLevel("WARN")


    val textData = spark.read
        .format("csv").option("header", "true").load("hdfs://localhost:9000/part2/articles1.csv").filter(row => row.get(9) != null)
      .toDF("_c0","id","title","publication","author","date","year","month","url","content"
      ).select("content")

    textData.show(15)


    val tokenizer = new RegexTokenizer().setPattern("[~!@#$^%&*\\\\(\\\\)_+={}\\\\[\\\\]|;:\\\"'<,>.?` /\\\\\\\\-]")
      .setInputCol("content")
      .setOutputCol("words")

    val wordsData = tokenizer.transform(textData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures")

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.show()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(5)

    val Array(trainning, test) = rescaledData.randomSplit(Array(0.9, 0.1))

    val kmeans = new KMeans().setK(20).setSeed(1L)
    val model = kmeans.fit(trainning)
    //println(model.summary.toString)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    //val WSSSE = model.computeCost(trainning)
    //println(s"Within Set Sum of Squared Errors = $WSSSE")
    val res = model.setPredictionCol("label").transform(trainning)

    res.select("Content", "label").groupBy("label").count().show()






  }
}
