package com.mi
import org.apache.spark.sql.SparkSession
object joinEx2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    //oracle 연결
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productDameDb = "kopo_product_master"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val productMasterDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productDameDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    productMasterDataFromOracle.createOrReplaceTempView("mstTable")


    selloutDataFromOracle.show()
    productMasterDataFromOracle.show()

    //join
    var middleResult = spark.sql("select concat(a.regionid, " +
      "concat('_',a.product)), " +
      "a.regionid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.productname " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.productid")


    var middleResult1 = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.productname " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.productid")

    //배열로 바꿔서 index 달기
    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.productname " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.productid")


    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productidNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")





  }

}
