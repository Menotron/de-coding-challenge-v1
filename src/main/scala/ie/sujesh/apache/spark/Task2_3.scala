package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, max, min}

object Task2_3 extends App{

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "sf-airbnb-clean.parquet"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-2_3_Spark_Dataframe_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()

  spark.sparkContext.setLogLevel(sparkConfig.getString("logLevel"))

  import spark.implicits._
  val airbnbDF = spark.read.parquet(INPUT_FILE_NAME)

  val priceReviewFilteredDF = airbnbDF.filter($"price" > 5000 && $"review_scores_rating" === 100.0)
  val out_2_3_DF = priceReviewFilteredDF.agg(
    avg("bathrooms").alias("avg_bathrooms"),
    avg("bedrooms").alias("avg_bedrooms"))

  out_2_3_DF.coalesce(1).write.option("header","true").csv(appConfig.getString("tempDir"))
  logger.info("Writing average number of bathrooms and bedrooms for properties with \n" +
    " price of > 5000 and a review score being exactly equalt to 10 to out_2_3.txt")
  Utils.renameAndMoveFile(appConfig.getString("tempDir"), "out_2_3.txt")

  spark.close

}
