import Config.JobConfig
import Solution.{Solution, TestGenerator}
import scopt.OParser
import storingservice.StoringService
import storingservice.file.FileHandler
import storingservice.postgre.PostgreService
import scala.util.Random
import scala.sys.exit
object RemoveIntersection extends Solution{
  val GEN_TEST = "process-with-gen-test"
  val PROCESS_INPUT = "process-with-existing-test"
  val LOCAL_FILE_MODE = "local-file"
  val POSTGRE = "postgre"

  val builder = OParser.builder[JobConfig]

  val argParser = {
    import builder._

    OParser.sequence(
      parser = programName("Remove-intersection"),
      parsers = head(xs = "Remove-intersection", "v1"),

      opt[String]('g', name = "generate tests or process existing input")
        .required()
        .validate(
          g => {
            g match {
              case GEN_TEST => success
              case PROCESS_INPUT => success
              case _ => failure("Either generate test or process existing input")
            }
          }
        )
        .action((g, c) => c.copy(genTestOrProcessInput = g))
        .text("generate tests or process existing input")
        .children(
          opt[String]('t', name = "type of database")
            .required()
            .validate(
              t => {
                t match {
                  case LOCAL_FILE_MODE => success
                  case POSTGRE => success
                  case _ => failure("Type is either local file or postgre")
                }
              }
            )
            .action((t, c) => c.copy(type_db = t))
            .text("Type of database")
            .children(
              opt[String]('i', name = "path or table name input")
                .required()
                .action((i, c) => c.copy(pathOrTableInput = i))
                .text("Table name or path of input file"),

              opt[String]('o', name = "path or table name output")
                .required()
                .action((o, c) => c.copy(pathOrTableOutput = o))
                .text("Table name or path of input file"),
            )
        )


    )
  }


  def main(args: Array[String]): Unit = {
    OParser.parse(argParser, args, JobConfig()) match {
      case Some(config) =>
        config.genTestOrProcessInput match {
          case GEN_TEST =>
            val r = 1 + new Random().nextInt(100)
            config.type_db match {
              case LOCAL_FILE_MODE =>
                val storeFile = FileHandler(config.pathOrTableInput, config.pathOrTableOutput)
                TestGenerator.generateTestInput(r, storeFile)
                process(storeFile)
              case POSTGRE =>
                val storePostgre = PostgreService(config.pathOrTableInput, config.pathOrTableOutput)
                TestGenerator.generateTestInput(r, storePostgre)
                process(PostgreService(config.pathOrTableInput, config.pathOrTableOutput))
            }

          case PROCESS_INPUT =>
            config.type_db match {
              case LOCAL_FILE_MODE =>
                val storeFile = FileHandler(config.pathOrTableInput, config.pathOrTableOutput)
                process(storeFile)
              case POSTGRE =>
                val storePostgre = PostgreService(config.pathOrTableInput, config.pathOrTableOutput)
                process(PostgreService(config.pathOrTableInput, config.pathOrTableOutput))
            }


          case _ =>
            exit(1)
        }
    }
  }
}
