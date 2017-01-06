package com.ksr.spark

import java.text.SimpleDateFormat

import breeze.linalg.{max, min}
import org.apache.spark.SparkContext
import org.apache.log4j._

/**
  * Created by sreedk2 on 1/2/2017.
  */
object MinTemperatures {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinTemperature")
    def parseLine(line: String) = {
      val fields = line.split(",")
      val date = new SimpleDateFormat("yyyyMMdd").parse(fields(1))
      val stationID = fields(0)
      val entryType = fields(2)
      val temperature = fields(3).toFloat
      (stationID, entryType, temperature, date)
    }

    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parseLine)

    val minTemps =  parsedLines.filter(x => x._2 == "TMIN")
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    val minTempByStations = stationTemps.reduceByKey((x,y) => min(x,y))
    val result = minTempByStations.collect()

    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    val stationMaxTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    val maxTempByStations = stationMaxTemps.reduceByKey((x,y) => max(x,y))
    val resultMax = maxTempByStations.collect()

    val prcp = parsedLines.filter(x => x._2 == "PRCP")
    val datePrcp = prcp.map(x => (x._1, x._3))
    val maxPrcp = datePrcp.reduceByKey((x,y) => max(x, y))
    val resultMaxPrcp = maxPrcp.collect()
    for(result <- resultMaxPrcp.sorted){
      val station = result._1
      val prcp = result._2
      println(s"$station max prcp is: $prcp")
    }
    for(result <- result.sorted){
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station station minimum temperature: $formattedTemp")
    }
   }
}
