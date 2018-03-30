package com.mi

import org.apache.spark.sql.SparkSession

object db_conn_sqlserver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     SqlServer(MySQL) 데이터 로딩 ////////////////////////////////////
    // 파일설정
    var staticUrl = "jdbc:sqlserver://127.0.0.1;databaseName=kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality"

    // jdbc (java database connectivity) 연결
    val selloutDataFromSqlserver= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromSqlserver.createOrReplaceTempView("selloutTable")


  }

}
