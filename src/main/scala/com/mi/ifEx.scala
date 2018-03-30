package com.mi

import org.apache.spark.sql.SparkSession

object ifEx {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")

    var currentYear = 2018
    var deltaYear = 0
    var validYear = 2015

    if(deltaYear !=0){
      validYear = currentYear - deltaYear
    }else{
      validYear
    }
    println(validYear)

    var score = 9
    var level = "기타"
    if(score > 9 ) {
      level = "수"
    }else if ((score > 8)&&(score <=9)){
      level = "우"
    }else{
      level = "기타"
    }
    println(level)


  }

}
