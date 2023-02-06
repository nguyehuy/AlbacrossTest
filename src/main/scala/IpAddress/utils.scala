package IpAddress

import scala.util.Try

object utils {

  def ipv4ToLong(ip: String): Option[Long] = Try(
    ip.split('.').ensuring(_.length == 4)
      .map(_.toLong).ensuring(_.forall(x => x >= 0 && x < 256))
      .reverse.zip(List(0, 8, 16, 24)).map(xi => xi._1 << xi._2).sum
  ).toOption

  def longToipv4(ip: Long): Option[String] = if (ip >= 0 && ip <= 4294967295L) {
    Some(List(0x000000ff, 0x0000ff00, 0x00ff0000, 0xff000000).zip(List(0, 8, 16, 24))
      .map(mi => ((mi._1 & ip) >> mi._2)).reverse
      .map(_.toString).mkString("."))
  } else None
}
