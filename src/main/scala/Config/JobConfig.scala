package Config
case class JobConfig(
                      genTestOrProcessInput: String = "gen-test",
                      type_db: String = "local-file",
                      pathOrTableInput: String = "src/main/resources/input/input.txt",
                      pathOrTableOutput: String = "src/main/resources/output",
                    )
