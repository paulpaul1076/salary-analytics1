package com.company

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test
import org.junit.Assert._
import DataFrameExtensions._

class PathProcessorTest {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  @Test def extractDataFrameFromPath(): Unit = {
    val actual = PathProcessor.extractDataFrameFromPath(spark, "src/test/resources/emp1/developers/111.json")
    val expected = Seq(("developers", "{  \"name\" : \"Van Basten\",  \"salary\": \"100\"}")).toDF("department", "employee info")

    assertTrue(expected.isEqualTo(actual))
  }

  @Test def readDataFromPath(): Unit = {
    val departmentAndEmployee = PathProcessor.readDataFromPath("src/test/resources/emp1/developers/111.json")
    assertEquals(Seq(("developers", "{  \"name\" : \"Van Basten\",  \"salary\": \"100\"}")), departmentAndEmployee)

    val readFromBadPath = PathProcessor.readDataFromPath("this/is/a/bad/path")
    assertEquals(Seq.empty[(String, String)], readFromBadPath)
  }

  @Test def extractDepartment(): Unit = {
    val path = "/data/emp/developers/111.json"
    val department = PathProcessor.extractDepartment(path)

    assertEquals("developers", department)
  }
}
