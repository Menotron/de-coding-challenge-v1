package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Task1_1 extends App {

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "groceries.csv"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-1_1_Spark_RDD_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
    .config("fs.file.impl", classOf[LocalFileSystem].getName)
    .getOrCreate()

  spark.sparkContext.setLogLevel(sparkConfig.getString("logLevel"))

  try {
    if (!new java.io.File(INPUT_FILE_NAME).exists) {
      logger.info("Downloading file to " + INPUT_FILE_NAME)
      Utils.fileDownloader(appConfig.getString("part1Url"), INPUT_FILE_NAME)
    }
  }
  catch {
    case e: Exception => logger.error("Failed to download file", e)
  }

  val fileRDD = spark.sparkContext.textFile(INPUT_FILE_NAME)
    .map(line => line.split(","))

  fileRDD.take(5).foreach(x => println(x.mkString(",")))

  spark.close

}
