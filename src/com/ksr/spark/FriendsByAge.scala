package com.ksr.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByAge {

  def main(args: Array[String]) {
//show avg number of friends by first name
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    def parseLine(line: String) = {
      val fields = line.split(",")
      val name = fields(1)
      val noFriends = fields(3).toInt
      (name, noFriends)
    }

    val lines = sc.textFile("../fakefriends.csv")
    val rdd = lines.map(parseLine)
    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => ((x._1 + y._1), (x._2 + y._2)))
    val avgByAge = totalsByAge.mapValues(x => x._1 /x._2)
    val result = avgByAge.collect()
    result.sorted.foreach(println)

  }

}
