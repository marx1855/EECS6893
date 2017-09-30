
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree


object Main {
  def main(args: Array[String]) {
    /*
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.set("spark.default.parallelism", "1")
  conf.setAppName("Airline Cancellation")
  val sc = new SparkContext(conf)
  */
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.default.parallelism", "1")
    conf.setAppName("HR")
    val sc = new SparkContext(conf)


    case class HR(id: Int,
                  sat: Double,
                  last_evl: Double,
                  num_p: Int,
                  amh: Int,
                  tsc: Int,
                  wa: Int,
                  left: Int,
                  pro: Int,
                  sales: String,
                  sala: String)

    def parseHR(str: String): HR = {
      val line = str.split(",")
      HR(line(0).toInt, line(1).toDouble, line(2).toDouble, line(3).toInt,
        line(4).toInt, line(5).toInt, line(6).toInt, line(7).toInt,
        line(8).toInt, line(9), line(10))
    }

    val textRDD = sc.textFile("hdfs://localhost:9000/user/mingyuan/input/HR/HR_comma_sep.csv")
    val header = textRDD.first()
    val dropHeaderRDD = textRDD.filter(row => row != header)


    val hrRDD = dropHeaderRDD.map(parseHR).cache()
    val salesRDD = hrRDD.map(line => (line.sales, 1)).reduceByKey(_ + _)
    salesRDD.foreach(println)

    var salesMap: Map[String, Int] = Map()
    var index: Int = 0
    hrRDD.map(hr => hr.sales).distinct.collect.foreach(x => {
      salesMap += (x -> index);
      index += 1
    })
    println(salesMap.toString())

    var salaryMap: Map[String, Int] = Map()
    var index1: Int = 0
    hrRDD.map(hr => hr.sala).distinct.collect.foreach(x => {
      salaryMap += (x -> index1);
      index1 += 1
    })
    println(salaryMap.toString())

    val mlprep = hrRDD.map(hr => {
      val sat = hr.sat
      val eval = hr.last_evl
      val proj = hr.num_p
      val amh = hr.amh
      val tsc = hr.tsc
      val wa = hr.wa //category
      val left = hr.left //category
      val pro = hr.pro //category
      val sales = salesMap(hr.sales) //category
      val sala = salaryMap(hr.sala) //category
      Array(left.toDouble, sat.toDouble, eval.toDouble, proj.toDouble,
        amh.toDouble, tsc.toDouble, wa.toDouble, pro.toDouble, sales.toDouble, sala.toDouble)
    })
    mlprep.take(10).foreach(println)

    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1),
      x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9))))

    val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.8, 0.2))(1)
    val mldata1 = mldata.filter(x => x.label != 0)
    val mldata2 = mldata0 ++ mldata1

    val splits = mldata2.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    var categoricalFeaturesInfo = Map[Int, Int]()
    categoricalFeaturesInfo += (5 -> 2)
    categoricalFeaturesInfo += (6 -> 2)
    categoricalFeaturesInfo += (7 -> salesMap.size)
    categoricalFeaturesInfo += (8 -> salaryMap.size)

    val numClasses = 2
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 5000

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    println(model.toDebugString)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)

    }

    //labelAndPreds.collect().take().foreach(println)
    val res = labelAndPreds.map(x => ((x._1, x._1.equals(x._2)), 1))
      .reduceByKey(_ + _)

    res.collect().foreach(println)
  }

}