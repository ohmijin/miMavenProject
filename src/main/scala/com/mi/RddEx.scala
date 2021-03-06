package com.mi
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType,DoubleType,StructField,StructType}

object RddEx {
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

var rawRdd = rawData.rdd

var filteredRdd = rawRdd.filter(x=>{
  var checkValid = true
  var weekValue = x.getString(yearweekNo).substring(4)
  if(weekValue.toInt >= 53){
    checkValid = false
  }
  checkValid
})


//분석대상 제품군 등록
var productArray = Array("PRODUCT1","PRODUCT2")

//세트 타입으로 변환
var productSet = productArray.toSet

//
var resultRdd = filteredRdd.filter(x=>{
  var checkValid = true
  var productInfo = x.getString(productidNo);
  if(productSet.contains(productInfo)){
    checkValid = true
  }
  checkValid
})

//두번째 답!!
//    var resultRdd = filteredRdd.filter(x=>{
//      var checkValid = true
//      var productInfo = x.getString(productidNo)
//      if((productInfo == "PRODUCT1") ||
//      (productInfo == "PRODUCT2" )){
//        checkValid = true
//      }
//        checkValid
//    })

resultRdd.collect.foreach(println)
val finalResultDf = spark.createDataFrame(resultRdd,
  StructType(
    Seq(
      StructField("KEY", StringType),
      StructField("REGIONID", StringType),
      StructField("PRODUCT", StringType),
      StructField("YEARWEEK", StringType),
      StructField("VOLUME", StringType),
      StructField("PRODUCT_NAME", StringType))))
}
}
