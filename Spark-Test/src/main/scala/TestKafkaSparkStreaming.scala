/**
 * Created by Javier Abascal on 6/7/2015.
 */

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{Path, FileSystem, FileUtil}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object TestKafkaSparkStreaming {
  // 54.154.129.60:9092 kafkaPrueba
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // S3 File System Connector
    val hadoopConf = ssc.sparkContext.hadoopConfiguration
    // s3 o s3n
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId","Introduce")
    hadoopConf.set("fs.s3.awsSecretAccessKey","Introduce")
    val hdfs = FileSystem.get(new URI("s3://javier-abascal/"),hadoopConf)

    // File Systems Paths
    val s3OutputPath = "s3://javier-abascal/u-tad_finalproject/probe_request_streaming/current-probe"
    val currentTime = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyy-mm-dd")
    val todayDate = dateFormat.format(currentTime)
    val s3FinalOutputPath = "s3://javier-abascal/u-tad_finalproject/probe_request/"

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Save the DStream on S3
    messages.saveAsTextFiles(s3OutputPath)
    FileUtil.copyMerge(hdfs,new Path(s3OutputPath),hdfs,new Path(s3FinalOutputPath),true, hadoopConf, null)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
