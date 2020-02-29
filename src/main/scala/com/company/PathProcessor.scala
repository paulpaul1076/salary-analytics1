package com.company

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * Объект для работы с путями.
 */
object PathProcessor {
  /**
   * Логирует ошибки, когда не получилось прочитать из файла.
   */
  private val logger = Logger.getLogger("PathProcessor")

  /**
   * Превращает путь к файлу в датафрейм.
   *
   * @param spark sparkSession
   * @param path путь к файлу
   * @return DataFrame с содержимым файла
   */
  def extractDataFrameFromPath(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    readDataFromPath(path).toDF("department", "employee info")
  }

  /**
   * Читает данные о департаменте и json с данными о работнике из пути, используя
   * org.apache.hadoop.fs.FileSystem т.к. в задании написано, что нельзя использовать spark.read.text/spark.read.json
   *
   * Если пути не существует, то возвращается пустой Seq, и выводится в error log сообщение об ошибке.
   *
   * @param pathStr строка с путем к файлу
   * @return Seq с парами (департамент, json)
   */
  def readDataFromPath(pathStr: String): Seq[(String, String)] = { // нельзя использовать spark wildcards + spark.read.json/text для чтения файликов.
    val path = new Path(pathStr)
    val fileSystem = FileSystem.get(path.toUri, new Configuration())
    val stream = Try(fileSystem.open(path))
    stream match {
      case Success(stream) =>
        def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
        Seq((extractDepartment(pathStr), readLines.takeWhile(_ != null).fold("")(_+_)))
      case Failure(exception) =>
        logger.error(exception.getLocalizedMessage)
        Seq.empty[(String, String)]
    }
  }

  /**
   * Вытаскивается департамент из пути к файлу, регулярное выражение смотрит на такую подстроку
   * пути, где первая capturing group захватывает все, что находится до последнего слэша, и не является слэшем.
   * пример: extractDepartment("path/to/department/json.json") вернет "department".
   *
   * @param path путь к файлу, откуда читаем
   * @return департамент
   */
  def extractDepartment(path: String): String = {
    val departmentRegex = "([^/]*)/(?:[^/]*)$".r
    departmentRegex.findFirstMatchIn(path) match {
      case Some(m) => m.group(1)
      case None => ""
    }
  }
}
