package com.spark.batch

/**
 * Created by Javier Abascal on 6/6/2015.
 */

/* SimpleApp.scala */

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object TestMain {

  case class ProbeRequest(id_counter: Integer, co_mac: String, ts_timestamp: Timestamp, qt_rssi: Integer,
                          co_BSSID: String, co_SSID: String)

  def main(args: Array[String]) {
    val logFile = "C:\\Users\\Javier Abascal\\Desktop\\prueba.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._



    val probes = sc.textFile(logFile).map(_.split(";")).map(p => ProbeRequest(p(0).trim.toInt,p(1),Timestamp.valueOf(p(2)+" "+p(3).substring(0,8)),
                              p(4).substring(0,3).trim.toInt,p(5),p(6))).toDF()

    // Create as table
    probes.registerTempTable("probes")

    //SQL STATEMENT
    val selectall = sqlContext.sql(
      "select * from probes where ts_timestamp > '2015-06-21 14:15:00' ")
    selectall.collect().foreach(println)



    //val probeRequest = sc.textFile(logFile,2).cache()
    //print(probeRequest)


    /*
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    */
  }
}
