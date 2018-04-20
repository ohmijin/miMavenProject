package com.mi

import org.apache.spark.sql.SparkSession

object db_connection {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    println("spark test")

    //하이디
    // 접속정보 설정
//    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
//    var staticUser = "kopo"
//    var staticPw = "kopo"
//    var selloutDb = "kopo_channel_seasonality"

//    // jdbc (java database connectivity) 연결
//    val selloutDataFromPg = spark.read.format("jdbc").
//      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
//
//    // 메모리 테이블 생성
//    selloutDataFromPg.createOrReplaceTempView("selloutTable")

    //파일설정
//    var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
//    var staticUser = "root"
//    var staticPw = "P@ssw0rd"
//    var selloutDb = "KOPO_PRODUCT_VOLUME"

//    // jdbc (java database connectivity)
//    val selloutDataFromMysql= spark.read.format("jdbc").
//      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

//    selloutDataFromMysql.createOrReplaceTempView("selloutTable")

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productDameDb = "kopo_product_mst"

//    val selloutDataFromOracle = spark.read.format("jdbc").
//      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
//
//    val productMasterDataFromOracle = spark.read.format("jdbc").
//      options(Map("url" -> staticUrl, "dbtable" -> productDameDb, "user" -> staticUser, "password" -> staticPw)).load

//    println(selloutDataFromOracle.createOrReplaceTempView("selloutTable"))
//    println(productMasterDataFromOracle.createOrReplaceTempView("mstTable"))
//
//
//    println(selloutDataFromOracle.show())
//    println(productMasterDataFromOracle.show())





  }

}