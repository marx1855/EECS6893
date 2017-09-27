import org.apache.spark.{SparkConf, SparkContext}


object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.default.parallelism", "1")
    conf.setAppName("Airline Cancellation")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of tetx
    val textFile = sc.textFile("hdfs://localhost:9000/user/mingyuan/input/Bitcoin/bitcoin_data.txt")
    val splitRDD = textFile.map(line => line.split(","))

    //myRDD.foreach( )
    // 0: index,
    // 1: id-from,
    // 2: id-to,
    // 3: username_from,
    // 4: username_to,
    // 5: gender_from,
    // 6: gender_to,
    // 7: date,
    // 8: amount

    val amountRDD = splitRDD.filter(array => array(8) != "amount")
      .map(array => ((array(8).substring(1).toFloat / 10000).toInt * 10000 + 10000, 1))
      .reduceByKey(_ + _)

      amountRDD.sortByKey().foreach(println)
    //System.out.println("" + counts.count());
    //counts.saveAsTextFile("/home/mingyuan/airline5");
  }
}
