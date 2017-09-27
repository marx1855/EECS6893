import org.apache.spark.{SparkConf, SparkContext}


object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.default.parallelism", "1")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("hdfs://localhost:9000/user/mingyuan/input/airline/2008.csv")
    val splitRDD = textFile.map(line => line.split(","))

    //myRDD.foreach( )

    //word count
    val counts = splitRDD.filter(array => array(21) == "1")
      .map(array => (array(1), 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    //System.out.println("" + counts.count());
    counts.saveAsTextFile("/home/mingyuan/airline3");
  }
}
