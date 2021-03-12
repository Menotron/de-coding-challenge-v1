package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, min}

object Task2_2 extends App{

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "sf-airbnb-clean.parquet"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-2_2_Spark_Dataframe_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()

  spark.sparkContext.setLogLevel(sparkConfig.getString("logLevel"))

  import spark.implicits._
  val airbnbDF = spark.read.parquet(INPUT_FILE_NAME)

  val out_2_2_DF = airbnbDF.agg(
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    count("price").alias("row_count"))

  out_2_2_DF.coalesce(1).write.option("header","true").csv(appConfig.getString("tempDir"))
  logger.info("Writing min, max and count to out_2_2.txt")
  Utils.renameAndMoveFile(appConfig.getString("tempDir"), "out_2_2.txt")

  spark.close

}
