package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Part1 extends App {

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "groceries.csv"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-1_Spark_RDD_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
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

  val listProducts = fileRDD.flatMap(item => item)
  listProducts.cache()


  listProducts.distinct.coalesce(1).saveAsTextFile(appConfig.getString("tempDir"))
  logger.info("Writing unique list of products to out_1_2a.txt")
  Utils.renameAndMoveFile(appConfig.getString("tempDir"), "out_1_2a.txt")


  val totalProducts = listProducts.count
  logger.info("Writing total count of products to out_1_2b.txt")
  Utils.writeToFile(appConfig.getString("outputDir") + "out_1_2b.txt",
    "Count:\n" + totalProducts.toString)

  val topProducts = listProducts.map(product => (product, 1))
    .reduceByKey(_ + _)
    .map(p => (p._2, p._1))
    .sortByKey(false, 1)
    .map(p => (p._2, p._1))
    .take(5)
    .map(t => s"$t")

  logger.info("Writing top 5 purchased products to out_1_3.txt")
  Utils.writeToFile(appConfig.getString("outputDir") + "out_1_3.txt",
    topProducts.mkString("\n"))

  spark.close
}
