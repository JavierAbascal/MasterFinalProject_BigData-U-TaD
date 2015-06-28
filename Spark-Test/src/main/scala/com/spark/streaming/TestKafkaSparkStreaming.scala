package com.spark.streaming

/**
 * Created by Javier Abascal on 6/7/2015.
 */

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestKafkaSparkStreaming {

  /////////////////////////////////////////////////////////////////////////
  // CASE CLASS for probeRequest
  /////////////////////////////////////////////////////////////////////////
  case class ProbeRequest(id_counter: Integer, co_mac: String, ts_timestamp: Timestamp, qt_rssi: Integer,
                          co_BSSID: String, co_SSID: String)



  // 54.154.129.60:9092 kafkaPrueba
  /////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]) {
  /////////////////////////////////////////////////////////////////////////
    if (args.length < 2) {
      System.err.println("""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 30 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaProbeRequest")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts","true")
    val sc  = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    ssc.remember(Seconds(30))


    // S3 File System Connector  (s3n)
    val hadoopConf = ssc.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId","FILL")
    hadoopConf.set("fs.s3n.awsSecretAccessKey","FILL")
    //val hdfs = FileSystem.get(URI.create("s3n://javier-abascal/"),hadoopConf)

    // Doesn't Work, check it later
    System.setProperty("spark.hadoop.mapred.output.compress", "true")
    System.setProperty("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    System.setProperty("spark.hadoop.mapred.output.compression.type", "BLOCK")

    // S3OutputPath
    val currentTime = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyy-mm-dd")
    val todayDate = dateFormat.format(currentTime)
    val s3OutputPath = "s3n://javier-abascal/u-tad_finalproject/probe_request_streaming/current_probe_"+todayDate

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaRDD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    kafkaRDD.foreachRDD{ rdd =>
      println("New DStream Starts")

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(sc)
      import sqlContext.implicits._

      // Convert the RDD into a DATAFRAME
      val probes = rdd.map(_._2).map(_.split(";")).map(p => ProbeRequest(p(0).trim.toInt,p(1),Timestamp.valueOf(p(2)+" "+p(3).substring(0,8)),
                                                                          p(4).substring(0,3).trim.toInt,p(5),p(6))).toDF()

      // Register the DATAFRAME as table
      probes.registerTempTable("probes")

      // Queries
      val probesGroupby = sqlContext.sql("SELECT id_counter, co_mac, ts_timestamp, max(qt_rssi) from probes group by id_counter, co_mac, ts_timestamp")
      probesGroupby.collect().foreach(println)

      val probesCount = sqlContext.sql("SELECT count(*) from probes")
      probesCount.collect().foreach(println)
    }


    /*

    // Get the lines, split them into words, count the words and print
    val lines = kafkaRDD.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()


    // Save the DStream on S3
    lines.saveAsTextFiles(s3OutputPath)
    // val s3FinalOutputPath = "s3n://javier-abascal/u-tad_finalproject/probe_request/"
    // FileUtil.copyMerge(hdfs,new Path("s3n://javier-abascal/u-tad_finalproject/probe_request_streaming/"),hdfs,new Path(s3FinalOutputPath),true, hadoopConf, null)
    // Start the computation

    */
    ssc.start()
    ssc.awaitTermination()


  }






}
