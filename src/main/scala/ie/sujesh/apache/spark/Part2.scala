package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{min, max, count}

object Part2 extends App {

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "sf-airbnb-clean.parquet"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-2_Spark_Dataframe_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()

  spark.sparkContext.setLogLevel(sparkConfig.getString("logLevel"))

  try {
    if (!new java.io.File(INPUT_FILE_NAME).exists) {
      logger.info("Downloading file to " + INPUT_FILE_NAME)
      Utils.fileDownloader(appConfig.getString("part2Url"), INPUT_FILE_NAME)
    }
  }
  catch {
    case e: Exception => logger.error("Failed to download file", e)
  }

  val airbnbDF = spark.read.parquet(INPUT_FILE_NAME)
  airbnbDF.printSchema
  airbnbDF.cache.show(5)

  val priceColumn = "price"

  val out_2_2_DF = airbnbDF.agg(
    min(priceColumn).alias("min_price"),
    max(priceColumn).alias("max_price"),
    count(priceColumn).alias("row_count"))

  out_2_2_DF.coalesce(1).write.option("header","true").csv(appConfig.getString("tempDir"))
  logger.info("Writing unique list of products to out_1_2a.txt")
  Utils.renameAndMoveFile(appConfig.getString("tempDir"), "out_2_2.txt")

}
