import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LinearSVC


object TFIDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("tfidf")
        .master("local")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory","6g")

      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    //spark.setLogLevel("WARN")


    val textData = spark.read.textFile("hdfs://localhost:9000/modified_text.txt").toDF("Content")
    val split_ = textData.randomSplit(Array(0.01, 0.99))
    val tokenizer = new Tokenizer()
      .setInputCol("Content")
      .setOutputCol("words")

    val wordsData = tokenizer.transform(split_(0))

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures")

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData).cache()
    rescaledData.show(10)


    val kmeans = new KMeans().setK(5).setSeed(1L)
    val model = kmeans.fit(rescaledData)
    //println(model.summary.toString)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    //val WSSSE = model.computeCost(trainning)
    //println(s"Within Set Sum of Squared Errors = $WSSSE")
    val res = model.setPredictionCol("label").transform(rescaledData)

    res.select("Content", "label").show(25)




    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(20)
      .fit(res)

    val Array(trainning, test) = res.select("features", "label").randomSplit(Array(0.9, 0.1))


    val dt = new DecisionTreeClassifier()
      .setLabelCol("label").setFeaturesCol("features").setMaxBins(100).setMaxDepth(10)

    val pipeline = new Pipeline()
      .setStages(Array(dt))
    val dtmodel = pipeline.fit(trainning)



    val predictions = dtmodel.transform(test)
    println("accu=")

    println(predictions.filter(line => line.get(1) != line.get(4).toString().toDouble.toInt).count())

    println(predictions.count())


    val rf = new RandomForestClassifier()
      .setLabelCol("label").setFeaturesCol("features").setMaxBins(100).setMaxDepth(10).setNumTrees(5)

    val pipeline2 = new Pipeline()
      .setStages(Array(rf))
    val rfmodel = pipeline2.fit(trainning)



    val predictions2 = rfmodel.transform(test)
    println(predictions2.count())
    println(predictions2.filter(line => line.get(1) != line.get(4).toString().toDouble.toInt).count())



  }
}
