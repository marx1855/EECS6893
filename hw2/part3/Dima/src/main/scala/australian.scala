import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.classification.SVMWithSGD



object australian {
  def main(args: Array[String]) {


    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.default.parallelism", "1")
    conf.setAppName("Aus")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    /*
   1. Number of times pregnant
   2. Plasma glucose concentration a 2 hours in an oral glucose tolerance test
   3. Diastolic blood pressure (mm Hg)
   4. Triceps skin fold thickness (mm)
   5. 2-Hour serum insulin (mu U/ml)
   6. Body mass index (weight in kg/(height in m)^2)
   7. Diabetes pedigree function
   8. Age (years)
   9. Class variable (0 or 1)
    * */
    case class Aus(
                    A1: Int,
                    A2: Double,
                    A3: Double,
                    A4: Int,
                    A5: Int,
                    A6: Int,
                    A7: Double,
                    A8: Int,
                    A9: Int,
                    A10: Double,
                    A11: Int,
                    A12: Int,
                    A13: Double,
                    A14: Double,
                    A15: Int
                   )


    def parseAus(str: String): Aus = {
      val line = str.split(" ")
      Aus(line(0).toInt, line(1).toDouble, line(2).toDouble, line(3).toInt,
        line(4).toInt, line(5).toInt, line(6).toDouble, line(7).toInt,
        line(8).toInt, line(9).toDouble, line(10).toInt,
        line(11).toInt, line(12).toDouble, line(13).toDouble, line(14).toInt
      )
    }

    val textRDD = sc.textFile("hdfs://localhost:9000/part3/australian.dat")
    //val header = textRDD.first()
    //val dropHeaderRDD = textRDD.filter(row => row != header)

    //textRDD.map(line => line.split(" ")).foreach(x => println(x.size))
    val ausRDD = textRDD.map(parseAus).cache()

    val mlprep = ausRDD.map(aus => {
      val A1 = aus.A1
      val A2 = aus.A2
      val A3 = aus.A3
      val A4 = aus.A4
      val A5 = aus.A5
      val A6 = aus.A6
      val A7 = aus.A7
      val A8 = aus.A8
      val A9 = aus.A9
      val A10 = aus.A10
      val A11 = aus.A11
      val A12 = aus.A12
      val A13 = aus.A13
      val A14 = aus.A14
      val A15 = aus.A15
      Array(A1.toDouble,
        A2.toDouble,
        A3.toDouble,
        A4.toDouble,
        A5.toDouble,
        A6.toDouble,
        A7.toDouble,
        A8.toDouble,
        A9.toDouble,
        A10.toDouble,
        A11.toDouble,
        A12.toDouble,
        A13.toDouble,
        A14.toDouble,
        A15.toDouble
      )
    })


    mlprep.collect().take(10).foreach(println)

    val mldata = mlprep.map(x => LabeledPoint(x(14), Vectors.dense(x(0), x(1),
      x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13))))

    val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.8, 0.2))(1)
    val mldata1 = mldata.filter(x => x.label != 0)
    val mldata2 = mldata0 ++ mldata1

    val splits = mldata2.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

      var categoricalFeaturesInfo = Map[Int, Int]()
      categoricalFeaturesInfo += (0 -> 2)
      categoricalFeaturesInfo += (3 -> 4)
      categoricalFeaturesInfo += (4 -> 15)
      categoricalFeaturesInfo += (5 -> 10)
      categoricalFeaturesInfo += (7 -> 2)
      categoricalFeaturesInfo += (8 -> 2)
      categoricalFeaturesInfo += (10 -> 2)
      categoricalFeaturesInfo += (11 -> 4)
    //var categoricalFeaturesInfo = Map[Int, Int]()
    val numClasses = 2
    val impurity = "gini"
    val maxDepth = 15
    val maxBins = 200



    //Desicion Tree Model
    val dtmodel = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    println(dtmodel.toDebugString)

    val labelAndPreds = testData.map { point =>
      val prediction = dtmodel.predict(point.features)
      (point.label, prediction)

    }

    val res = labelAndPreds.map(x => ((x._1, x._1.equals(x._2)), 1))
      .reduceByKey(_ + _)
    println("Decision Tree Test Result")
    res.collect().foreach(println)


    //Random Forest Model
    val rfmodel = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, 20, "auto", impurity, maxDepth, maxBins)
    val labelAndPredsRF = testData.map { point =>
      val prediction = rfmodel.predict(point.features)
      (point.label, prediction)

    }

    val resRF = labelAndPredsRF.map(x => ((x._1, x._1.equals(x._2)), 1))
      .reduceByKey(_ + _)

    println("Random Forest Test Result")
    resRF.collect().foreach(println)


    //SVM
    val svmmodel = SVMWithSGD.train(trainingData, 200)
    val labelAndPredsSVM = testData.map { point =>
      val prediction = rfmodel.predict(point.features)
      (point.label, prediction)

    }

    val resSVM = labelAndPredsSVM.map(x => ((x._1, x._2), 1))
      .reduceByKey(_ + _)

    println("SVM Test Result")
    resSVM.collect().foreach(println)

  }

}