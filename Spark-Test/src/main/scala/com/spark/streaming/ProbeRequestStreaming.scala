package com.spark.streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.tests.TestKafkaSparkStreaming
import TestKafkaSparkStreaming.ProbeRequest
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Javier Abascal on 6/28/2015.
 */

object ProbeRequestStreaming {

  /////////////////////////////////////////////////////////////////////////
  // CASE CLASS for probeRequest
  /////////////////////////////////////////////////////////////////////////
  case class ProbeRequest2(id_counter: Integer, co_mac: String, ts_timestamp: Timestamp, qt_rssi: Integer,
                           co_BSSID: String, co_SSID: String)


  // 54.154.129.60:9092 kafkaPrueba
  def main(args: Array[String]) {
    /////////////////////////////////////////////////////////////////////////
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)

      System.exit(1)
    }

    val Array(brokers, topics) = args
    val checkpointDirectory = "s3n://javier-abascal/u-tad_finalproject/SparkStreaming_CheckpointDirectory/"
    val sparkConf = new SparkConf().setAppName("ProbeRequestStreaming")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts","true")


    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        functionToCreateContext(brokers,topics,checkpointDirectory,sparkConf)
      })
    ssc.start()
    ssc.awaitTermination()

  }


  def functionToCreateContext(brokers: String, topics: String, checkpointDirectory: String, sparkConf: SparkConf): StreamingContext = {
    println("Creating new Context")

    // Spark Streaming Context
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    ssc.remember(Seconds(30))
    val sc  = new SparkContext(sparkConf)


    // S3 File System Connector  (s3n)
    val hadoopConf = ssc.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId","FILL")
    hadoopConf.set("fs.s3n.awsSecretAccessKey","FILL")

    // S3OutputPath
    val currentTime = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyy-mm-dd")
    val todayDate = dateFormat.format(currentTime)
    val s3OutputPath = "s3n://javier-abascal/u-tad_finalproject/probe_request_streaming/current_probe_"+todayDate

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaRDD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // SET tasks for DStreams
    kafkaRDD.foreachRDD{ rdd =>
      println("New DStream Starts")

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(sc)
      import sqlContext.implicits._

      // Convert the RDD into a DATAFRAME
      val probes = rdd.map(_._2).map(_.split(";")).map(p => ProbeRequest2(p(0).trim.toInt,p(1),Timestamp.valueOf(p(2)+" "+p(3).substring(0,8)),
        p(4).substring(0,3).trim.toInt,p(5),p(6))).toDF()

      // Register the DATAFRAME as table
      probes.registerTempTable("probes")

      // Queries
      val probesGroupby = sqlContext.sql("SELECT id_counter, co_mac, ts_timestamp, max(qt_rssi) from probes group by id_counter, co_mac, ts_timestamp")
      probesGroupby.collect().foreach(println)

      val probesCount = sqlContext.sql("SELECT count(*) from probes")
      probesCount.collect().foreach(println)
    }

    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
    ssc
  }


}
