package experiments

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{desc, explode, lower, regexp_replace, split, trim}

/**
  * Created by lenovo on 6/2/2018.
  */
object ShakespeareWordCount {

  def normalizeSentence(s: String): String = {
    s.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", "").toLowerCase()
  }

  def splitSentence(sentence: String): Seq[String] = {
    sentence
      .split(" ")
      .filter(_.nonEmpty)
  }

  def main(args: Array[String]): Unit = {
    val inputLocation = args(0)
    val outputLocation = args(1)

    val spark: SparkSession = SparkSession.builder()
      .appName("PickardShakespeareWordCount")
      .getOrCreate()

    val shakespeare: RDD[String] = spark.sparkContext.textFile(inputLocation)

    val cleanSentences = shakespeare.map(normalizeSentence)
    val wordCount: RDD[(String, Int)] = cleanSentences
      .flatMap(splitSentence)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    wordCount.saveAsTextFile(outputLocation)
  }
}
