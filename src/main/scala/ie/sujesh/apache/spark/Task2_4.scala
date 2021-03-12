package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min}

object Task2_4 extends App {

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "sf-airbnb-clean.parquet"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-2_4_Spark_Dataframe_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()

  spark.sparkContext.setLogLevel(sparkConfig.getString("logLevel"))

  import spark.implicits._
  val airbnbDF = spark.read.parquet(INPUT_FILE_NAME)

  val minPrice = airbnbDF.agg(min("price")).first().getDouble(0)
  val highestRating  = airbnbDF.agg(max("review_scores_rating")).first().getDouble(0)

  println(highestRating)

  val lowestPriceHighestRatingDF = airbnbDF
    .filter($"price" === minPrice && $"review_scores_rating" === highestRating)

  val numPeople = lowestPriceHighestRatingDF.select("accommodates").first().getDouble(0)

  logger.info("Writing the number of people accommodated by the property with lowest price and highest rating to out_2_4.txt")
  Utils.writeToFile(appConfig.getString("outputDir") + "out_2_4.txt", numPeople.toString)

  spark.close

}
