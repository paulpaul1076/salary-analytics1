package com.company

import org.apache.commons.cli.{DefaultParser, Options}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.util.{Failure, Success, Try}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * Объект с основной логикой задания.
 */
object Solution {
  /**
   * Логирует ошибки связанные с аргументами.
   */
  private val logger = Logger.getLogger("Solution")

  /**
   * Объект для парсинга json'a.
   * @param name имя сотрудника
   * @param salary зарплата сотрудника
   */
  case class Employee(name: String, salary: String)

  /**
   * DTO объект который содержит ответы на все, что нужно было вычислить в задании.
   *
   * @param allEmployees все работники (записывается в csv)
   * @param employeeWithTheHighestSalary работник с самой высокой ЗП (выводится на экран)
   * @param sumOfAllSalaries сумма всех ЗП (выводится на экран)
   */
  case class Answer(allEmployees: DataFrame, employeeWithTheHighestSalary: DataFrame, sumOfAllSalaries: DataFrame)

  /**
   * Entry point. Использует библиотеку apache.commons.cli для парсинга входных аргументов.
   * Если все нормально, создает sparkSession и запускает метод с вычислениями,
   * иначе выводит сообщение об ошибке с аргументами и списком аргументов, которые нужно передать,
   * чтобы программа работала
   *
   * @param args cmd args.
   */
  def main(args: Array[String]): Unit = {
    val options = new Options

    options.addRequiredOption("p", "paths", true, "Comma separated absolute paths to data")
    options.addRequiredOption("csv", "csvOutputFileName", true, "The name of the csv file to write to")
    options.addOption("l", "runLocal", false, "Specify this if this job supposed to run in local mode")

    val parser = new DefaultParser
    val cmdArgs = Try(parser.parse(options, args))

    cmdArgs match {
      case Success(cmdArgs) =>

        val paths = cmdArgs.getOptionValue("paths")
        val csvOutputFileName = cmdArgs.getOptionValue("csvOutputFileName")
        val isRunLocal = cmdArgs.hasOption("runLocal")

        val spark =
          if (isRunLocal) SparkSession.builder().master("local[*]").getOrCreate()
          else SparkSession.builder().getOrCreate()

        val answer = runCalculations(paths, spark)

        answer.allEmployees.write.mode(SaveMode.Overwrite).csv(csvOutputFileName)
        answer.employeeWithTheHighestSalary.show()
        answer.sumOfAllSalaries.show()

        spark.stop()

      case Failure(exception) =>
        logger.error(exception.getMessage)
        logger.error(options.getOptions)
    }
  }

  /**
   * Принимает на вход пути, из которых нужно читать, и вычисляет все вещи, которые нужно вычислить в задании.
   * Читает пути в параллель, используется ForkJoinPool, иначе метод ".par" будет запускать количество потоков,
   * равное количеству виртуальных ядер на компьютере, если таких четыре, то всего будет запускаться по 4 потока,
   * а файлов может быть больше. Для кол-ва потоков используемого для чтения можно ввести отдельный параметр, но я
   * этого не делал, и просто использую parallelism = кол-ву файлов.
   *
   * После чтения файлов у нас есть массив DataFrame'ов, которые мы объединяем трансформацией unionByName. Все операции
   * lazy, поэтому данные с датафреймов не будут переполнять драйвер.
   *
   * Далее я трансформирую прочтенные данные с помощью библиотеки для парсинга json'a.
   *
   * В конце я возвращаю датафреймы с ответами на задания.
   *
   * Если какого-то из путей не существует, то программа его пропускает и выводит в error log сообщение о том, что
   * из этого пути не удалось прочитать (например, потому что он не существует).
   *
   * @param paths строка с путями к файлам.
   * @param spark sparkSession
   * @return dto объект Answer с результатами вычислений.
   */
  def runCalculations(paths: String, spark: SparkSession): Answer = {
    import spark.implicits._

    val pathsArray = paths.split(",").par
    pathsArray.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(pathsArray.size))

    val unionedData = pathsArray // Читаем в параллель из списка путей, т.к. pathsArray -- параллельная коллекция
      .map(path => PathProcessor.extractDataFrameFromPath(spark, path))
      .fold(Seq.empty[(String, String)].toDF("department", "employee info"))(_ unionByName _)

    val departmentNameSalaryData = unionedData.map(row => { // Вытаскиваем с помощью json4s данные об имени и зарплате из json'a
      val employee = convertFromJson(row.getAs[String](1))
      (row.getAs[String]("department"), employee.name, employee.salary.toInt)
    }).toDF("department", "name", "salary").cache()

    departmentNameSalaryData.createOrReplaceTempView("employees")

    Answer(
      allEmployees = departmentNameSalaryData,
      employeeWithTheHighestSalary = spark.sql("select * from employees order by salary desc limit 1"),
      sumOfAllSalaries = spark.sql("select sum(salary) from employees")
    )
  }

  /**
   * Конвертируем из json в Employee содержимое файлов.
   * @param json json строка.
   * @return объект типа Employee.
   */
  def convertFromJson(json: String): Employee = {
    implicit val jsonDefaultFormats = DefaultFormats
    JsonMethods.parse(json).extract[Employee]
  }
}
