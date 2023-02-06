package Solution

import Config.SparkConfig
import IpAddress.IpAddress
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import storingservice.StoringService

trait Solution extends SparkConfig {


  private def removeIntersectionIpRanges(dfIpRanges: DataFrame): DataFrame = {

    val dfIpStart = dfIpRanges.select(col("start")).withColumn("flag", lit(1))
    val dfIpEnd = dfIpRanges.select(col("end")).withColumn("flag", lit(-1))


    def getNextIp(ip: String) = {
      val nextIp = IpAddress(ip).getNextIpAddress()

      nextIp match {
        case Some(value) => value.standartFormatIp
        case _ => ""
      }
    }

    val udfNextIp = udf(x => getNextIp(x))


    def getPreviousIp(ip: String) = {
      val prevIp = IpAddress(ip).getPrevIpAddress()
      prevIp match {
        case Some(value) => value.standartFormatIp
        case _ => ""
      }
    }

    val udfPrevIp = udf(x => getPreviousIp(x))


    def getStandardFormatIp(ip: String) = {
      IpAddress(ip).standartFormatIp
    }

    val udfStandardFormatIp = udf(x => getStandardFormatIp(x))

    dfIpStart
      .union(dfIpEnd)
      .sort(col("start"))
      .withColumn("cumulative-flag", sum(col("flag")).over(
        Window.orderBy(col("start"))
      ))
      .withColumn("end", lead(col("start"), 1).over(
        Window.orderBy(col("start"))
      ))
      .withColumn("cumulative-flag-next", lead(col("cumulative-flag"), 1).over(
        Window.orderBy(col("start"))
      ))
      .withColumn("cumulative-flag-prev", lag(col("cumulative-flag"), 1).over(
        Window.orderBy(col("start"))
      ))
      .filter(col("cumulative-flag") === lit(1))
      .withColumn("new-ip-start",
        when(col("cumulative-flag-prev") =!= lit(0), udfNextIp(col("start")))
          .otherwise(col("start"))
      )
      .withColumn("new-ip-end",
        when(col("cumulative-flag-next") =!= lit(0), udfPrevIp(col("end")))
          .otherwise(col("end"))
      )
      .filter(col("new-ip-end") >= col("new-ip-start"))
      .select(udfStandardFormatIp(col("new-ip-start")).as("start"), udfStandardFormatIp(col("new-ip-end")).as("end"))

  }

  def process(storeService: StoringService) = {
    val df = storeService.readInput()
    val res = removeIntersectionIpRanges(df)
    storeService.saveOutput(res)
  }
}
