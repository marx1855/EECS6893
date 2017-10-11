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
    conf.setAppName("Dima")
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
    case class Dima(preg: Int,
                    pgc: Int,
                    bloodp: Int,
                    tsft: Int,
                    si: Int,
                    BMI: Double,
                    Dpf: Double,
                    age: Int,
                    label: Int

                   )

    def parseDima(str: String): Dima = {
      val line = str.split(",")
      Dima(line(0).toInt, line(1).toInt, line(2).toInt, line(3).toInt,
        line(4).toInt, line(5).toDouble, line(6).toDouble, line(7).toInt,
        line(8).toInt)
    }

    val textRDD = sc.textFile("hdfs://localhost:9000/part3/pima-indians-diabetes.data")
    //val header = textRDD.first()
    //val dropHeaderRDD = textRDD.filter(row => row != header)


    val dimaRDD = textRDD.map(parseDima).cache()

    val mlprep = dimaRDD.map(dima => {
      val preg = dima.preg
      val pgc = dima.pgc
      val bloodp = dima.bloodp
      val tsft = dima.tsft
      val si = dima.si
      val bmi = dima.BMI
      val dpf = dima.Dpf
      val age = dima.age
      val label = dima.label
      Array(label.toDouble, preg.toDouble, pgc.toDouble, bloodp.toDouble,
        tsft.toDouble, si.toDouble, bmi.toDouble, dpf.toDouble, age.toDouble)
    })


    mlprep.collect().take(10).foreach(println)

    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1),
      x(2), x(3), x(4), x(5), x(6), x(7), x(8))))

    val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.8, 0.2))(1)
    val mldata1 = mldata.filter(x => x.label != 0)
    val mldata2 = mldata0 ++ mldata1

    val splits = mldata2.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    /*
      var categoricalFeaturesInfo = Map[Int, Int]()
      categoricalFeaturesInfo += (5 -> 2)
      categoricalFeaturesInfo += (6 -> 2)
      categoricalFeaturesInfo += (7 -> salesMap.size)
      categoricalFeaturesInfo += (8 -> salaryMap.size)
  */
    var categoricalFeaturesInfo = Map[Int, Int]()
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