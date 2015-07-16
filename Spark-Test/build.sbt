name := "Spark-Test"
version := "1.0"
scalaVersion := "2.10.4"
mainClass in Compile := Some("com.spark.streaming.MainJava")


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.3.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.3.1" % "provided"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.6"
libraryDependencies += "com.ning" % "async-http-client" % "1.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.0" % "test"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "org.apache.commons" % "commons-email" % "1.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0"

// META-INF discarding
val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}