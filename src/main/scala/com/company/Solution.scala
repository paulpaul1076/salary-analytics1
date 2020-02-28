package com.company

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import scala.util.{Failure, Success, Try}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object Solution {
  private val logger = Logger.getLogger("Solution")

  case class Person(name: String, salary: String)

  def main(args: Array[String]): Unit = {
    val options = new Options

    options.addRequiredOption("p", "paths", true, "Comma separated absolute paths to data")
    options.addRequiredOption("csv", "csvOutputFileName", true, "The name of the csv file to write to")
    options.addOption("l", "runLocal", false, "Specify this if this job supposed to run in local mode")

    val parser = new DefaultParser
    val cmdArgs = Try(parser.parse(options, args))

    cmdArgs match {
      case Success(cmdArgs) =>
        runCalculations(
          cmdArgs.getOptionValue("paths"),
          cmdArgs.getOptionValue("csvOutputFileName"),
          cmdArgs.hasOption("runLocal")
        )

      case Failure(exception) =>
        logger.error(exception.getMessage)
        logger.error(options.getOptions)
    }
  }

  def runCalculations(paths: String, csvOutputFileName: String, isRunLocal: Boolean): Unit = {
    val spark =
      if (isRunLocal) SparkSession.builder().master("local[*]").getOrCreate()
      else SparkSession.builder().getOrCreate()

    import spark.implicits._

    val unionedData = paths.split(",").par // Читаем в параллель из списка путей
      .map(path => extractDataFrameFromPath(spark, path))
      .fold(Seq.empty[(String, String)].toDF("department", "employee info"))(_ unionByName _)

    val newData = unionedData.map(row => {
      val person = convertFromJson(row.getAs[String](1))
      (row.getAs[String]("department"), person.name, person.salary.toInt)
    }).toDF("department", "name", "salary").cache()

    newData.write.csv(csvOutputFileName) // запишет в csv список всех работников c полями : name, salary, department

    newData.createOrReplaceTempView("employees")

    spark.sql("select * from employees order by salary desc limit 1").show() // выведет на экран работника с самой большой зарплатой
    spark.sql("select sum(salary) from employees").show() // выведет на экран сумму зарплат всех работников

    spark.stop()
  }

  def convertFromJson(json: String): Person = {
    implicit val jsonDefaultFormats = DefaultFormats
    JsonMethods.parse(json).extract[Person]
  }

  def extractDataFrameFromPath(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    Seq(readDataFromPath(path)).toDF("department", "employee info")
  }

  def isPathGood(path: String): Boolean = {
    throw new Exception
  }

  def readDataFromPath(pathStr: String): (String, String) = { // нельзя использовать spark wildcards + spark.read.json/text для чтения файликов.
    val path = new Path(pathStr)
    val fileSystem = FileSystem.get(path.toUri, new Configuration())
    val stream = fileSystem.open(path)
    def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))

    (extractDepartment(pathStr), readLines.takeWhile(_ != null).fold("")(_+_))
  }

  def extractDepartment(path: String): String = {
    val departmentRegex = "([^/]*)/(?:[^/]*)$".r
    departmentRegex.findFirstMatchIn(path) match {
      case Some(m) => m.group(1)
      case None => ""
    }
  }
}
