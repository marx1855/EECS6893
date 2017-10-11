name := "Dima"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" % "spark-mllib_2.11" % sparkVer,
    "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
    "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

  )
}