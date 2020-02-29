package com.company

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test
import org.junit.Assert._
import DataFrameExtensions._

/**
 * Тесты для extension методов для датафреймов.
 */
class DataFrameExtensionsTest {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  @Test def isEqualTo(): Unit = {
    val df1 = Seq("abc", "dfg").toDF("strings")
    val df2 = Seq("dfg", "abc").toDF("string")
    val df3 = Seq("abc").toDF("strings")

    assertTrue(df1.isEqualTo(df2))
    assertFalse(df1.isEqualTo(df3))
  }
}
