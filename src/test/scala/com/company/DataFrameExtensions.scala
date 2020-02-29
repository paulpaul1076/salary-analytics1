package com.company

import org.apache.spark.sql.DataFrame

/**
 * Extension методы для датафрейма.
 */
object DataFrameExtensions {
  class RichDataFrame(df1: DataFrame) {
    /**
     * Метод для сравнения 2х датафреймов.
     * @param df2 второй датафрейм.
     * @return равны ли датафреймы? true/false
     */
    def isEqualTo(df2: DataFrame): Boolean = {
      df1.except(df2).isEmpty && df2.except(df1).isEmpty
    }
  }

  implicit def richDataFrame(df: DataFrame): RichDataFrame = new RichDataFrame(df)
}
