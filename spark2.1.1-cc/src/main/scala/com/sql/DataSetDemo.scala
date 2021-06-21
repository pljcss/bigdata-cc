package com.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author: cs
 * @Date: 2021/4/10 11:24 上午
 * @Desc:
 */
object DataSetDemo {
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataSetDemo")
      .getOrCreate()

    import spark.implicits._
    val caseClassDS = List(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "spark2.1.1-cc/input/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

    spark.stop()

  }

}
