package IpAddress

case class IpAddress(ip: String){
  val (part1, part2, part3, part4) = getIpParts()
  val threeDigitRepresentation = s"${formatNumberThreeDigits(part1)}.${formatNumberThreeDigits(part2)}.${formatNumberThreeDigits(part3)}.${formatNumberThreeDigits(part4)}"
  val standartFormatIp = s"${part1}.${part2}.${part3}.${part4}"
  private def getIpParts(): (Int, Int, Int, Int) = {
    val split = ip.trim().split('.')
    (split.head.toInt, split(1).toInt, split(2).toInt, split.last.toInt)
  }
  def getNextIpAddress(): Option[IpAddress] = {
    val newPart4 = (part4 + 1) % 256
    if (newPart4 != 0) return Option(IpAddress(s"${part1}.${part2}.${part3}.${newPart4}"))
    val newPart3 = (part3 + 1) % 256
    if (newPart3 != 0) return Option(IpAddress(s"${part1}.${part2}.${newPart3}.0"))
    val newPart2 = (part2 + 1) % 256
    if (newPart2 != 0) return Option(IpAddress(s"${part1}.${newPart2}.0.0"))
    val newPart1 = (part1 + 1) % 256
    if (newPart1 != 0) return Option(IpAddress(s"${newPart1}.0.0.0"))
    None
  }
  def getPrevIpAddress(): Option[IpAddress] = {
    val newPart4 = part4 - 1
    if (newPart4 != -1) return Option(IpAddress(s"${part1}.${part2}.${part3}.${newPart4}"))
    val newPart3 = part3 - 1
    if (newPart3 != -1) return Option(IpAddress(s"${part1}.${part2}.${newPart3}.255"))
    val newPart2 = part2 - 1
    if (newPart2 != -1) return Option(IpAddress(s"${part1}.${newPart2}.255.255"))
    val newPart1 = part1 - 1
    if (newPart1 != -1) return Option(IpAddress(s"${newPart1}.255.255.255"))
    None
  }

  private def formatNumberThreeDigits(num: Int): String = {
    if (0 <= num && num < 10) s"00${num}"
    else if (10 <= num && num < 99) s"0${num}"
    else s"${num}"
  }


}

object IpAddress{
  def apply(ip: String): IpAddress = {
    val split = ip.trim().split('.')

    if (split.size== 4
      && checkIsValid(split.head)
      && checkIsValid(split(1))
      && checkIsValid(split(2))
      && checkIsValid(split.last)) {

      new IpAddress(ip)
    }else{throw new Exception(s"Invalid Ip Address: ${ip}")}
  }
  def checkIsValid(ipPart: String) : Boolean = ipPart.forall(Character.isDigit) && 0<= ipPart.toInt  && ipPart.toInt <= 255

}
