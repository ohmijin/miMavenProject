package com.mi

import org.apache.spark.sql.SparkSession

object funcEx {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")

    def yearWeek(date:String, delimeter:String): (String,String) = {
      var arrayYW = date.split(delimeter)
      var yearValue = arrayYW(0)
      var weekValue = arrayYW(1)
      (yearValue, weekValue)
    }
    var date = "2018;03"
    var delimeter = ";"
    yearWeek(date,delimeter)

   //선생님 코드

    

  }

}
