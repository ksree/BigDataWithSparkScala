package com.ksr.spark

import org.apache.spark.SparkContext

/**
  * Created by sreedk2 on 1/2/2017.
  */
object WordCount {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "RatingsCounter")

    val input = sc.textFile("../book.txt")
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    val resultWordCountsSorted = wordCountsSorted.collect()
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- resultWordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}
