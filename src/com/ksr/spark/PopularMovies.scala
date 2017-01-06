package com.ksr.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Source, Codec}

/** Find the movies with the most ratings. */
object PopularMovies {
 
  /** Our main function where the action happens */

  def loadMovieNames(): Map[Int, String]= {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map.empty
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for(line <- lines){
      val fields = line.split('|')
      if(fields.length > 0){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")   
    val movieNames = loadMovieNames()
    sc.broadcast(movieNames)
    movieNames.foreach(println)
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")

    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    val moviesCount = movies.reduceByKey((x,y) => x + y)
    val flipped = moviesCount.map(x => (x._2, x._1)).sortByKey()
    val sortedMoviesWithNames = flipped.map(x => (movieNames.get(x._2).get, x._1))
    val resultPopularMovies = sortedMoviesWithNames.collect()
    resultPopularMovies.foreach(println)
  }
  
}

