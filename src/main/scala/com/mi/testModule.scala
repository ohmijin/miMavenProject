package com.mi

import org.apache.spark.sql.SparkSession

object testModule {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")
    //Load data
    var testData= spark.read.text("c:/spark/README.md")
    //Save data
    testData.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      save("test") // 저장파일명
    println("spark test completed")

   // 반복하기
    var priceData = Array(1000.0, 1200.0, 1300.0, 1500.0, 10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size


    for(i <-0 until priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
    }

    var priceData2 = Array(1000.0, 1200.0, 1300.0, 1500.0, 10000.0)

    priceData2.map(x=>{
      x - (x*promotionRate)
    })

    priceData2.filter(x=>{
      x>1000
    })


  }
}






