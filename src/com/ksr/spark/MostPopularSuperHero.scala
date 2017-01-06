package com.ksr.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by sreedk2 on 1/3/2017.
  */


object MostPopularSuperHero {

  def parseNames(line: String): Option[(Int, String)] ={
    val fields = line.split('\"')
    if(fields.length >1) {
      Some((fields(0).trim.toInt, fields(1)))
    }else{
      None  //flatMap will discard None
    }
  }

  def countCoOccurance(line: String): (Int, Int)={
    val fields = line.split("\\s+")
    (fields(0).trim.toInt, fields.length - 1)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MostPopularSuperHero")

    val names = sc.textFile("../Marvel-names.txt")
    val namesRDD = names.flatMap(parseNames)

    val occurance = sc.textFile("../Marvel-graph.txt")
    val pairings = occurance.map(countCoOccurance)

    val superHeroOccurance = pairings.reduceByKey((x,y) => x + y)
    val flipped = superHeroOccurance.map(x => (x._2, x._1))
    val superHeroOccuranceSorted = flipped.sortByKey()
    val mostPopularSuperHero = superHeroOccuranceSorted.max()._2
    val heroname = namesRDD.lookup(mostPopularSuperHero)(0)
    println("Most popular hero name is " + heroname)
    val topTenSuperHero = superHeroOccuranceSorted.top(10).map(_._2)
    for(result <- topTenSuperHero){
      val name = namesRDD.lookup(result)(0)
      println(name)
    }
  }
}
