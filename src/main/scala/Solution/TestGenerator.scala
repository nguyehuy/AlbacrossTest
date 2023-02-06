package Solution

import Config.SparkConfig
import IpAddress.utils
import org.apache.commons.lang3.RandomUtils
import org.apache.spark.sql.DataFrame
import storingservice.StoringService

object TestGenerator extends SparkConfig{
  val min = 0L
  val max = 4294967295L

  private def generateRandomIpv4(start: Long, end: Long): Long = RandomUtils.nextLong(start, end)

  private def generateRandomIpRangeType1(startIp: Long, endIp: Long): (Long, Long) = {
    val generatedEndIp = generateRandomIpv4(min, startIp)
    (generateRandomIpv4(min, generatedEndIp), generatedEndIp)
  }

  private def generateRandomIpRangeType2(startIp: Long, endIp: Long): (Long, Long) = (generateRandomIpv4(min, startIp), startIp)

  private def generateRandomIpRangeType3(startIp: Long, endIp: Long): (Long, Long) = (generateRandomIpv4(min, startIp), generateRandomIpv4(startIp, endIp))

  private def generateRandomIpRangeType4(startIp: Long, endIp: Long): (Long, Long) = (startIp, generateRandomIpv4(startIp, endIp))

  private def generateRandomIpRangeType5(startIp: Long, endIp: Long): (Long, Long) = (startIp, generateRandomIpv4(endIp, max))

  private def generateRandomIpRangeType6(startIp: Long, endIp: Long): (Long, Long) = {
    val generatedStartId = generateRandomIpv4(startIp, endIp)
    (generatedStartId, generateRandomIpv4(generatedStartId, endIp))
  }

  private def generateRandomIpRangeType7(startIp: Long, endIp: Long): (Long, Long) = (generateRandomIpv4(startIp, endIp), generateRandomIpv4(endIp, max))

  private def generateRandomIpRangeType8(startIp: Long, endIp: Long): (Long, Long) = (endIp, generateRandomIpv4(endIp, max))

  private def generateRandomIpRangeType9(startIp: Long, endIp: Long): (Long, Long) = {
    val generatedStartId = generateRandomIpv4(endIp + 1, max)
    (generatedStartId, generateRandomIpv4(generatedStartId, max))
  }

  private def generateIpRanges(n: Int): Seq[(Long, Long)] = {
    var ipRanges = scala.collection.mutable.Seq[(Long, Long)]()
    val fistStartIp = generateRandomIpv4(min + n, max - 2 * n)
    val firstEndIp = generateRandomIpv4(fistStartIp, max - n)
    ipRanges = ipRanges :+ (fistStartIp, firstEndIp)
    val rand = new scala.util.Random
    for (i <- 1 to n - 1) {
      val randomIpType = rand.nextInt(9) + 1
      randomIpType match {
        case 1 => ipRanges = ipRanges :+ generateRandomIpRangeType1(ipRanges.last._1, ipRanges.last._2)
        case 2 => ipRanges = ipRanges :+ generateRandomIpRangeType2(ipRanges.last._1, ipRanges.last._2)
        case 3 => ipRanges = ipRanges :+ generateRandomIpRangeType3(ipRanges.last._1, ipRanges.last._2)
        case 4 => ipRanges = ipRanges :+ generateRandomIpRangeType4(ipRanges.last._1, ipRanges.last._2)
        case 5 => ipRanges = ipRanges :+ generateRandomIpRangeType5(ipRanges.last._1, ipRanges.last._2)
        case 6 => ipRanges = ipRanges :+ generateRandomIpRangeType6(ipRanges.last._1, ipRanges.last._2)
        case 7 => ipRanges = ipRanges :+ generateRandomIpRangeType7(ipRanges.last._1, ipRanges.last._2)
        case 8 => ipRanges = ipRanges :+ generateRandomIpRangeType8(ipRanges.last._1, ipRanges.last._2)
        case 9 => ipRanges = ipRanges :+ generateRandomIpRangeType9(ipRanges.last._1, ipRanges.last._2)
      }
    }
    ipRanges
  }

  def generateTestInput(n: Int, storingService: StoringService): DataFrame = {
    val ipLongFormatRanges = generateIpRanges(n)
    val ipRanges = ipLongFormatRanges.map {
      ipLongFormatRange => {
        val startIp = utils.longToipv4(ipLongFormatRange._1)
        val endIp = utils.longToipv4(ipLongFormatRange._2)
        var startIpString = ""
        var endIpString = ""
        startIp match {
          case Some(value) => startIpString = value
          case None =>
        }
        endIp match {
          case Some(value) => endIpString = value
          case None =>
        }
        (startIpString, endIpString)
      }
    }
    val columns = Seq("start_ip", "end_ip")
    val res = spark.createDataFrame(ipRanges).toDF(columns: _*)
    storingService.saveTestInput(res)
    res
  }


}
