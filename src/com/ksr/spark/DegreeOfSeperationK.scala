package com.ksr.spark

import breeze.linalg.min
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sreedk2 on 1/3/2017.
  */
object DegreeOfSeperationK {

  val startCharacterID = 6148
  val targetCharacterID = 5623 //ADAM 3,031 (who?)
  //Custom DataType, BFSData contains list of connections, distance and color
  type BFSData = (Array[Int], Int, String)
  //A Node contains the connection id and the BFSData
  type BFSNode = (Int, BFSData)

  //WE make our accumulator a global varaible
  var hitCounter: Option[LongAccumulator] = None
  def createStartingRDD(sc: SparkContext): RDD[BFSNode] ={
    val lines = sc.textFile("../Test-graph.txt")
    lines.map(convertToBFS)
  }
  def convertToBFS(line: String):BFSNode ={
    val fields = line.split("\\s+")
    val id = fields(0).toInt

    var connectionList: ArrayBuffer[Int] = ArrayBuffer.empty
    for(x <- 1 to fields.length -1 ){
      connectionList += fields(x).toInt
    }
    var distance = 9999
    var color = "WHITE"
    //If the id is same as the start character, then set it to GREY
    if(id == startCharacterID){
      color = "GRAY"
      distance = 0
    }
    (id, (connectionList.toArray, distance, color) )
  }

  def bfsMap(node: BFSNode): Array[BFSNode] ={
    val id = node._1
    val data = node._2

    val connections = data._1
    var color = data._3
    val distance = data._2
    val result: ArrayBuffer[BFSNode] = ArrayBuffer.empty
    if(color == "GRAY"){
      for(connection <- connections){
        val newConnectionID = connection
        val newColor = "GRAY"
        val newDistance = distance + 1

        if(targetCharacterID == newConnectionID){
          if(hitCounter.isDefined){
            hitCounter.get.add(1)
          }
        }
        val node: BFSNode = (newConnectionID, ( Array.emptyIntArray, newDistance, newColor))
        result += node

      }
      color = "BLACK"
    }
    val thisNode =  (id, (connections, distance, color))
    result += thisNode
    result.toArray
  }

  def bfsReduce(data1: BFSData, data2: BFSData): BFSData ={
    val color1 = data1._3
    val color2 = data2._3
    val distance1 = data1._2
    val distance2 = data2._2
    val connections1 = data1._1
    val connections2 = data2._1

    var newConnections: Array[Int] = Array.empty
    val newDistance = min(distance1, distance2)
    var newColor:String  = ""
    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      newColor = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      newColor = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      newColor = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      newColor = color1
    }
    if(connections1.length > 0) newConnections ++= connections1
    if(connections2.length > 0) newConnections ++= connections2

    (newConnections, newDistance, newColor)
  }

  def main(args: Array[String])  {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","DegreeOfSeperationK")

    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var inputBFSNodes = createStartingRDD(sc)
    val a = inputBFSNodes



    //Print result of the map
     //mapResult.map(x => (x._1, x._2._1.toList.toString, x._2._2, x._2._3)).foreach(println)


    for(i <- 0 to 10) {
      var mapResult = inputBFSNodes.flatMap(bfsMap)
      //mapResult.map(x => (x._1, x._2._1.toList.toString, x._2._2, x._2._3)).foreach(println)
      var reduced = mapResult.reduceByKey(bfsReduce)

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          return
        }
      }
      println(f"$i+1 Iteration ")
      reduced.map(x => (x._1, x._2._1.toList.toString, x._2._2, x._2._3)).foreach(println)
      inputBFSNodes = reduced
    }

  }
}
