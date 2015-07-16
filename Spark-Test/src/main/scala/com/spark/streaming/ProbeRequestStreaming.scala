package com.spark.streaming

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util
import java.util.{Arrays, Calendar}

import com.spark.InfluxAPI.Client
import com.spark.InfluxAPI.Series
import com.spark.javaemail.SendMail
import com.spark.InfluxAPI.SeriesMap
import com.spark.InfluxAPI.response
import com.spark.InfluxAPI.error
import kafka.serializer.StringDecoder
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


import scala.util.Random
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Javier Abascal on 6/28/2015.
 */

object ProbeRequestStreaming {

  /////////////////////////////////////////////////////////////////////////
  // InfluxDB Configuration
  /////////////////////////////////////////////////////////////////////////
  private var client: Client = null

  final val DB_NAME               = "FILL_IT"
  final val DB_USER               = "FILL_IT"
  final val DB_PASSWORD           = "FILL_IT"
  final val CLUSTER_ADMIN_USER    = "FILL_IT"
  final val CLUSTER_ADMIN_PASS    = "FILL_IT"

  /////////////////////////////////////////////////////////////////////////
  // Kafka Configuration
  /////////////////////////////////////////////////////////////////////////
  final val KAFKA_BROKERS = "FILL_IT"
  final val KAFKA_TOPICS  = "FILL_IT"
  
  /////////////////////////////////////////////////////////////////////////
  // Alerts Configuration
  /////////////////////////////////////////////////////////////////////////
  final val JAVI_MAC  = "FILL_IT"
  final val MIGUE_MAC = "FILL_IT"
  final val DAVID_MAC = "FILL_IT"
  var JaviLastTimeSeen  = Timestamp.valueOf("2015-01-01 00:00:00").getTime
  var MigueLastTimeSeen = Timestamp.valueOf("2015-01-01 00:00:00").getTime
  var DavidLastTimeSeen = Timestamp.valueOf("2015-01-01 00:00:00").getTime

  /////////////////////////////////////////////////////////////////////////
  // Gmail Configuration
  /////////////////////////////////////////////////////////////////////////
  final val HOST = "FILL_IT"
  final val SSL_PORT = "FILL_IT"
  final val USERNAME = "FILL_IT"
  final val PASSWORD = "FILL_IT"

  /////////////////////////////////////////////////////////////////////////
  // CASE CLASS for probeRequest
  /////////////////////////////////////////////////////////////////////////
  case class ProbeRequest(id_counter: Integer, co_mac: String, ts_timestamp: Long, qt_rssi: Integer,
                          co_BSSID: String, co_SSID: String)




  /////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]) {
  /////////////////////////////////////////////////////////////////////////
    if (args.length != 0) {
      System.err.println(s"""
                            |Usage: No Arguments Needed for This SPARK_STREAMING |
        """.stripMargin)

      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("ProbeRequestStreaming")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts","true")

    // New Random Checkpoints for debugging
    // val random = new Random().nextString(10)
    val checkpointDirectory = "s3n://FILL_IT/u-tad_finalproject/" +"SparkStreaming_CheckpointDirectory/"+"MasterBigData"+"/"

    val sc  = new SparkContext(sparkConf)
    // S3 File System Connector  (s3n)
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId","FILL_IT")
    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey","FILL_IT")

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        functionToCreateContext(KAFKA_BROKERS,KAFKA_TOPICS,checkpointDirectory,sparkConf,sc)
      })
    
    ssc.start()
    ssc.awaitTermination()

  }


  def functionToCreateContext(brokers: String, topics: String, checkpointDirectory: String, sparkConf: SparkConf, sc: SparkContext): StreamingContext = {
    println("Creating new Context")

    // Spark Streaming Context
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    ssc.remember(Seconds(30))


    /*
    // S3OutputPath and CheckpointDirectory
    val currentTime = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("yyyy-mm-dd")
    val todayDate = dateFormat.format(currentTime)
    val s3OutputPath = "s3n://javier-abascal/u-tad_finalproject/probe_request_streaming/current_probe_"+todayDate
    */

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaRDD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)



    // SET tasks for DStreams        --> SPARK STREAMING TASKS
    kafkaRDD.foreachRDD{ rdd =>
      println("New DStream Starts")

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert the RDD into a DATAFRAME
      val probes = rdd.map(_._2).map(_.split(";")).map(p =>
        ProbeRequest(p(0).trim.toInt,p(1),(Timestamp.valueOf(p(2)+" "+p(3).substring(0,8)).getTime),
        p(4).substring(0,3).trim.toInt,p(5),p(6))).toDF()

      // Register the DATAFRAME as table "probes"
      probes.registerTempTable("probes")

      // Query 1 on table "probes"
      val probesGroupbySecond = sqlContext.sql(
        "SELECT " +
        "  id_counter, " +
        "  co_mac, " +
        "  ts_timestamp, " +
        "  max(qt_rssi) as qt_rssi, " +
        "  count(*)     as qt_tracks " +
        "FROM probes " +
        "GROUP BY id_counter,co_mac,ts_timestamp")

      // Insert Data grouped by Second into an Array[Array[Any] -- For InfluxDB
      val influxData = new ArrayBuffer[Array[Any]]()
      probesGroupbySecond.collect().foreach(row =>
        // id_counter, co_mac, ts_timestamp(Long), qt_rssi, qt_tracks
        influxData += Array(row.getInt(0),row(1).toString,row.getLong(2),row.getInt(3),row.getLong(4)))

      // Use of InfluxDB API to insert influxData
      sendDataToInfluxDB(influxData.toArray)
      

      // Query 2 on table "probes"
      val distinctMac = sqlContext.sql(
        "SELECT " +
        "  distinct co_mac   as co_mac," +
        "  min(ts_timestamp) as ts_timestamp " +
        "FROM probes " +
        "GROUP BY co_mac"
      )

      //Check MACs Alerts
      distinctMac.collect().foreach(row => checkMACAlerts(row))
      
      //end of foreachRDD
    }


    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
    ssc
  }


  
  

  /////////////////////////////////////////////////////////////////////////
  def sendDataToInfluxDB(influxData : Array[Array[Any]] ) : Unit = {
  /////////////////////////////////////////////////////////////////////////  
    client = new Client()
    client.database = DB_NAME

    val track_second = Series("track_second",
      Array("id_counter", "co_mac", "time", "qt_rssi", "qt_tracks"),influxData
    )
    assert(None == client.writeSeries(Array(track_second)))

    client.close()
  }

  
  /////////////////////////////////////////////////////////////////////////
  def checkMACAlerts(row:Row) : Unit = {
  /////////////////////////////////////////////////////////////////////////
    if(row.getString(0).contains(JAVI_MAC)) {
      if(row.getLong(1)-JaviLastTimeSeen > 1200000) {  //20 minutes
        //Send EMAIL
        println("SENDING EMAIL TO JAVIER")
        val Time = new Date(row.getLong(1))
        val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        val currentTime = df.format(Time)

        sendEmail("Javier Abascal Carrasco", currentTime)
        JaviLastTimeSeen = row.getLong(1)
      } else {
        JaviLastTimeSeen = row.getLong(1)
      }
    }
    else if(row.getString(0).contains(MIGUE_MAC)) {
      if(row.getLong(1)-MigueLastTimeSeen > 1200000) {  //20 minutes
        //Send EMAIL
        println("SENDING EMAIL TO MIGUE")
        val Time = new Date(row.getLong(1))
        val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        val currentTime = df.format(Time)

        sendEmail("Miguel Angel Amo", currentTime)
        MigueLastTimeSeen = row.getLong(1)
      } else {
        MigueLastTimeSeen = row.getLong(1)
      }
    }
    else if(row.getString(0).contains(DAVID_MAC)) {
      if(row.getLong(1)-DavidLastTimeSeen > 1200000) {  //20 minutes
        //Send EMAIL
        println("SENDING EMAIL TO DAVID")
        val Time = new Date(row.getLong(1))
        val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        val currentTime = df.format(Time)

        sendEmail("David Suarez", currentTime)
        DavidLastTimeSeen = row.getLong(1)
      } else {
        DavidLastTimeSeen = row.getLong(1)
      }
    }
    else
    {
      //Nothing TOSEND
      println("NO ONE IS NEW AT HOME")
    }
  }

  /////////////////////////////////////////////////////////////////////////
  def sendEmail(person_who_has_arrived:String , currentTime:String) : Unit = {
  /////////////////////////////////////////////////////////////////////////

    // From
    val from = "RaspbberyPi2_Sensor"
    // To
    val to = Arrays.asList("javier.abascal@hotmail.com","davsuacar@gmail.com","a_mi_no_mestafes@hotmail.com")
    // Subject
    val subject = "New FlatMate at Home"
    // Body
    val body = person_who_has_arrived + " hast arrived at " + currentTime
    // Nothing to attach
    val lFiles = new util.ArrayList[String]

    new SendMail(HOST,SSL_PORT,USERNAME,PASSWORD).sendEmail(from,to,subject,body,lFiles)
  }

}
