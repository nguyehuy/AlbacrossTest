package storingservice.postgre

import IpAddress.IpAddress
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import pureconfig.ConfigSource
import storingservice.StoringService
import pureconfig.generic.auto._

import java.sql.DriverManager
import java.util.Properties

case class PostgreMetadata(
                            driver: String,
                            urlPrefix: String,
                            port: Int,
                            db: String,
                            username: String,
                            password: String
                          )
case class PostgreService(
                           tableInput: String,
                           tableOutput: String
                         ) extends StoringService{

  val postgreMetadata = ConfigSource.resources("metadata/db.conf").at("postgre-metadata").load[PostgreMetadata].toOption.get
  override def readInput(): DataFrame = {
    val df = spark
      .read
      .format("jdbc")
      .option("url", s"${postgreMetadata.urlPrefix}:${postgreMetadata.port}/${postgreMetadata.db}")
      .option("dbtable", tableInput)
      .option("user", postgreMetadata.username)
      .option("password", postgreMetadata.password)
      .option("driver", postgreMetadata.driver)
      .load()



    def getThreeDigitFormatIp(ip: String) = {
      IpAddress(ip).threeDigitRepresentation
    }
    val udfThreeDigitFormatIp = udf(x => getThreeDigitFormatIp(x))
    val res =df
      .withColumn("start", udfThreeDigitFormatIp(col("start_ip")))
      .withColumn("end", udfThreeDigitFormatIp(col("end_ip")))
      .select(col("start"), col("end"))

    res
  }

  override def saveOutput(df: DataFrame): Unit = {
    df
      .write
      .format("jdbc")
      .option("url", s"${postgreMetadata.urlPrefix}:${postgreMetadata.port}/${postgreMetadata.db}")
      .option("dbtable", tableOutput)
      .option("user", postgreMetadata.username)
      .option("password", postgreMetadata.password)
      .option("driver", postgreMetadata.driver)
      .save()
  }

  override def saveTestInput(df: DataFrame): Unit =
    df
      .write
      .format("jdbc")
      .option("url", s"${postgreMetadata.urlPrefix}:${postgreMetadata.port}/${postgreMetadata.db}")
      .option("dbtable", tableInput)
      .option("user", postgreMetadata.username)
      .option("password", postgreMetadata.password)
      .option("driver", postgreMetadata.driver)
      .save()
}
