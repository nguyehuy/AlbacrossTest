ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "com.github.pureconfig" %% "pureconfig" % "0.17.1",
  "org.postgresql"     % "postgresql"   % "42.2.25.jre7",
  "com.github.scopt" %% "scopt" % "4.0.1",
)

lazy val root = (project in file("."))
  .settings(
    name := "AlbacrossTest"
  )

assembly / assemblyShadeRules := Seq(ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll)
assembly / assemblyJarName := "remove-intersection-ip.jar"
assembly / assemblyOutputPath := file(s"./src/${(assembly/assemblyJarName).value}")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case _                        => MergeStrategy.first
}