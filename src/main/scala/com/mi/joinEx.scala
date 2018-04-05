package com.mi

import org.apache.spark.sql.SparkSession
object joinEx {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var mainFile = "kopo_channel_seasonality_ex.csv"
    var subFile = "kopo_product_mst.csv"
    var dataPath = "c:/spark/bin/data/"
    // 상대경로 입력
    var mainData = spark.read.format("csv").
      option("header", "true").load(dataPath + mainFile)
    var subData = spark.read.format("csv").
      option("header", "true").load(dataPath + subFile)

    //dataframe
    var mainDataDf = spark.read.format("csv").
      option("header","true").load(dataPath + subData)

    //dataframe
    var subDataDf = spark.read.format("csv").
      option("header","true").load(dataPath + subData)


    mainData.createOrReplaceTempView("maindata")
    subData.createOrReplaceTempView("subdata")

    var leftJoinData = spark.sql("select a.*, b.productname " +
      "from maindata a left outer join subdata b " +
      "on a.productgroup = b.productid")
  }
}
