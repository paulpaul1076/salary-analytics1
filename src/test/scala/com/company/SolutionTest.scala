package com.company

import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.junit.Assert._
import DataFrameExtensions._

class SolutionTest {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  @Test def runCalculations(): Unit = {
    val basePath1 = "src/test/resources/emp1/"
    val paths1 = Seq("developers/111.json","hr/555.json","managers/777.json")

    val basePath2 = "src/test/resources/emp2/"
    val paths2 = Seq("developers/111.json","hr/555.json","managers/777.json", "i/do/not/exist")

    val paths = paths1.map(basePath1 + _).mkString(",") + "," + paths2.map(basePath2 + _).mkString(",")

    val answer = Solution.runCalculations(paths, spark)

    val expectedAllEmployees = Seq(
      ("developers", "Van Basten", 100),
      ("hr", "Diego Maradona", 800),
      ("managers", "Rud Gulit", 150),
      ("developers", "Van Basten2", 100),
      ("hr", "Diego Maradona2", 900),
      ("managers", "Rud Gulit2", 150)
    ).toDF("department", "name", "salary")

    val expectedEmployeeWithTheHighestSalary = Seq(
      ("hr", "Diego Maradona2", 900)
    ).toDF("department", "name", "salary")

    val expectedSumOfAllSalaries = Seq(2200).toDF("sum(salary)")

    assertTrue(expectedAllEmployees.isEqualTo(answer.allEmployees))
    assertTrue(expectedEmployeeWithTheHighestSalary.isEqualTo(answer.employeeWithTheHighestSalary))
    assertTrue(expectedSumOfAllSalaries.isEqualTo(answer.sumOfAllSalaries))
  }
}
