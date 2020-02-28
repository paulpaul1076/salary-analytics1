name := "salary-analytics"

version := "0.1"

scalaVersion := "2.11.10"

mainClass in Compile := Some("com.company.Solution")
assemblyJarName in assembly := "SalaryAnalytics.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.json4s" %% "json4s-jackson" % "3.6.7",
  "junit" % "junit" % "4.12" % Test,
  "commons-cli" % "commons-cli" % "1.4",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.hadoop" % "hadoop-common" % "2.6.5",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"
)

dependencyOverrides ++= Seq(
  "commons-cli" % "commons-cli" % "1.4",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
