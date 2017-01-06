package com.ksr.spark


import breeze.linalg.max
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
/**
  * Created by sreedk2 on 1/2/2017.
  */
object CustomerSpend {

  def main(input: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","CustomerSpend")

    def parseLine(line: String):(String, Float)={
      val fields = line.split(",")
      val custNo = fields(0)
      val amount = fields(2).toFloat
      (custNo, amount)
    }

    val input = sc.textFile("../customer-orders.csv")
    val customers = input.map(parseLine)
    val totalByCustomer = customers.reduceByKey((x,y) => x + y)
    val result = totalByCustomer.map(x => (x._2, x._1)).sortByKey().collect()


    result.foreach(println)


  }
}
