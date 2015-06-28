package com.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Javier Abascal on 6/28/2015.
 */

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}