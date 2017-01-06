package com.ksr.spark

import java.nio.charset.CodingErrorAction

import breeze.linalg.split
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Source, Codec}
import scala.math._

/**
  * Created by sreedk2 on 1/5/2017.
  */
object  MovieSimilaritiesK {

  type MovieRating = (Int, Double)
  type MovieRatingPair = (Int, (MovieRating, MovieRating))
  def loadMovieNames():Map[Int,String] ={

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val input = Source.fromFile("../ml-100k/u.item").getLines()
    var movieNames: Map[Int, String] = Map.empty
    for(l <- input){
      var fields = l.split('|')
      movieNames += (fields(0).toInt -> fields(1))
    }
    movieNames
  }

  def filterDuplicates(x: MovieRatingPair): Boolean ={
    val movie1 = x._2._1._1
    val movie2 = x._2._2._1
    movie1 > movie2
  }

  def makePairs(x: MovieRatingPair): ((Int, Int),(Double, Double))={
    ((x._2._1._1, x._2._2._1),(x._2._1._2, x._2._2._2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val movieNames = loadMovieNames()
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    val data = sc.textFile("../ml-100k/u.data")
    val ratings = data.map(x => x.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))
    //ratings.sortByKey().foreach(println)
    val joinedRatings = ratings.join(ratings)
    val uniqueJoinedPairs = joinedRatings.filter(filterDuplicates)
    //Key by movie pairs ((movie1, movie2), (rating1, rating2))
    val moviePairs = uniqueJoinedPairs.map(makePairs)
    val movieRatingPair = moviePairs.groupByKey()
    val moviePairSimilarities = movieRatingPair.mapValues(computeCosineSimilarity).cache()
    if(args.length >0 ){
      val scoreThreshold = 0.97
      val coOccuranceThreshold = 50.0

      val movieID: Int = args(0).trim.toInt

      val filteredResult = moviePairSimilarities.filter{x =>  (x._1._1 == movieID || x._1._2 == movieID) && x._2._1 > scoreThreshold && x._2._2 > coOccuranceThreshold }

      val result = filteredResult.map(x => (x._2, x._1)).sortByKey(false).take(10)

      println("Top 10 movies similar to movie " + loadMovieNames().get(movieID))
      for(x <- result){
        println(loadMovieNames().get(x._2._1).getOrElse("No Match Found"))
      }
    }

  }
}
