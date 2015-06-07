/**
 * Created by Javier Abascal on 6/6/2015.
 */

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TestMain {

  def main(args: Array[String]) {
    val logFile = "C:\\Users\\Javier Abascal\\Desktop\\prueba.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
