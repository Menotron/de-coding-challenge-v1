package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Task1_2 extends App{

  val config = ConfigFactory.load("application.conf").getConfig("truata.de.challenge")
  val sparkConfig = config.getConfig("spark")
  val appConfig = config.getConfig("app")
  val logger = LogManager.getRootLogger
  val INPUT_FILE_NAME = appConfig.getString("dataDir") + "groceries.csv"

  val spark = SparkSession.builder
    .master(sparkConfig.getString("master"))
    .appName("Part-1_2_Spark_RDD_API")
    .config("spark.hadoop.validateOutputSpecs", "False")
    .getOrCreate()

  spark.sparkContext.setLogLevel(sparkConfig.getString("logLevel"))

  val fileRDD = spark.sparkContext.textFile(INPUT_FILE_NAME)
    .map(line => line.split(","))


  val listProducts = fileRDD.flatMap(item => item)
  listProducts.cache()

  listProducts.distinct.coalesce(1).saveAsTextFile(appConfig.getString("tempDir"))
  logger.info("Writing unique list of products to out_1_2a.txt")
  Utils.renameAndMoveFile(appConfig.getString("tempDir"), "out_1_2a.txt")

  val totalProducts = listProducts.count
  logger.info("Writing total count of products to out_1_2b.txt")
  Utils.writeToFile(appConfig.getString("outputDir") + "out_1_2b.txt",
    "Count:\n" + totalProducts.toString)

  spark.close

}
