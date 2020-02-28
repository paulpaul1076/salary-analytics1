package com.company

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test
import org.junit.Assert._


class SolutionTest {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  @Test def runCalculations(): Unit = {
  }

  @Test def extractDataFrameFromPath(): Unit = {
    val actual = Solution.extractDataFrameFromPath(spark, "src/test/resources/emp1/developers/111.json")
    val expected = List(("developers", "{  \"name\" : \"Van Basten\",  \"salary\": \"100\"}")).toDF("department", "employee info")

    assertTrue(areDataFramesEqual(expected, actual))
  }

  private def areDataFramesEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    df1.except(df2).isEmpty && df2.except(df1).isEmpty
  }

  @Test def readDataFromPath(): Unit = {
    val departmentAndEmployee = Solution.readDataFromPath("src/test/resources/emp1/developers/111.json")
    assertEquals(("developers", "{  \"name\" : \"Van Basten\",  \"salary\": \"100\"}"), departmentAndEmployee)
  }

  @Test def extractDepartment(): Unit = {
    val path = "/data/emp/developers/111.json"
    val department = Solution.extractDepartment(path)

    assertEquals("developers", department)
  }
}