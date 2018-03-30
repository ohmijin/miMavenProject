package com.mi

import org.apache.spark.sql.SparkSession

object forEx {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")

    var priceData = Array(1000.0, 1200.0, 1300.0, 1500.0, 10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size
    var i =0

    while(i < priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
      i=i+1
      println(priceData(i))
      }
  }

}
