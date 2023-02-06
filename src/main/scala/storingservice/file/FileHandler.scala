package storingservice.file

import IpAddress.IpAddress
import org.apache.spark.sql.{DataFrame, SaveMode}
import storingservice.StoringService

import java.io.File

case class FileHandler(
                        pathInput: String,
                        pathOutput: String
                   ) extends StoringService{

  override def readInput(): DataFrame = {
    val lines = spark.sparkContext.textFile(s"input-data/$pathInput")
    val input = lines.map(line => {
      val splited = line.split(',')
      (IpAddress(splited.head).threeDigitRepresentation, IpAddress(splited.last).threeDigitRepresentation)
    })
    val columns = Seq("start", "end")
    spark.createDataFrame(input).toDF(columns: _*)
  }

  override def saveOutput(df: DataFrame): Unit =
    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", ", ")
      .csv(s"output-data/$pathOutput")

  override def saveTestInput(df: DataFrame): Unit =
    df
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", ", ")
      .csv(s"input-data/$pathInput")

}
