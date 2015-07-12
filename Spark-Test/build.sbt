name := "Spark-Test"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.3.1"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.6"
libraryDependencies += "com.ning" % "async-http-client" % "1.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.0" % "test"

// libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

// libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"




