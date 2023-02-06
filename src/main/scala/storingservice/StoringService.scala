package storingservice

import Config.SparkConfig
import org.apache.spark.sql.DataFrame
trait StoringService extends SparkConfig {
  def readInput(): DataFrame
  def saveOutput(df: DataFrame): Unit
  def saveTestInput(df: DataFrame): Unit
}
