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




    mainData.createOrReplaceTempView("maindata")
    subData.createOrReplaceTempView("subdata")

    var leftJoinData = spark.sql("select a.*, b.productname " +
      "from maindata a left outer join subdata b " +
      "on a.productgroup = b.productid")

    var innerJoinData = spark.sql("select a.*, b.productname " +
      "from maindata a inner join subdata b " +
      "on a.productgroup = b.productid")

    //데이터 프레임에 담아보자
    //dataframe
    var mainDataDf = spark.read.format("csv").
      option("header","true").load(dataPath + subData)

    //dataframe
    var subDataDf = spark.read.format("csv").
      option("header","true").load(dataPath + subData)

    mainDataDf.createOrReplaceTempView("mainTable")
    subDataDf.createOrReplaceTempView("subTable")

    //곱하기가 된다
    spark.sql("select a.regionid, b.productgroup, a.yearweek, a.qty "  +
    "from mainTable a " + "left join subTable b "
    )

    var leftJoinDataDf = spark.sql("select a.regionid, b.productgroup, a.yearweek, a.qty" +
      "from mainTable a " + "left join subTable b "+
      "on a.productgroup = b.productid")

    var innerJoinDataDf = spark.sql("select a.regionid, b.productgroup, a.yearweek, a.qty" +
      "from mainTable a " + "inner join subTable b "+
      "on a.productgroup = b.productid")

    //오라클 접속 DB 두개를 조인해보자

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_region_mst"

    // jdbc (java database connectivity) 연결
    val oracleSeasonData= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load

    val oracleRegionData= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    oracleSeasonData.createOrReplaceTempView("oracleSeasonTable")
    oracleSeasonData.show

    oracleRegionData.createOrReplaceTempView("oracleRegionTable")
    oracleRegionData.show

    var innerJoinDataEx = spark.sql("select a.*, b.* " +
      "from oracleRegionTable a inner join oracleSeasonTable b " +
      "on a.regionid = b.regionid")

    println(innerJoinDataEx.show(5))

    var leftJoinDataEx = spark.sql("select a.*, b.* " +
      "from oracleRegionTable a left join oracleSeasonTable b " +
      "on a.regionid = b.regionid")

    println(leftJoinDataEx.show(5))

    println(innerJoinDataEx.count)
    println(leftJoinDataEx.count)
  }

}
