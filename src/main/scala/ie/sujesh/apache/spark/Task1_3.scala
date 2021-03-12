package ie.sujesh.apache.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Task1_3 extends App{

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

  val fileRDD = spark.sparkContext.textFile(INPUT_FILE_NAME)
    .map(line => line.split(","))

  val listProducts = fileRDD.flatMap(item => item)

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
