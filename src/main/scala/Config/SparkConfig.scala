package Config

import org.apache.spark.sql.SparkSession
import scopt.{OParser, OParserBuilder}
import storingservice.StoringService

trait SparkConfig {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("AlbacrossTest")
    .config("spark.sql.warehouse.dir", "file:///")
    .getOrCreate()

}
