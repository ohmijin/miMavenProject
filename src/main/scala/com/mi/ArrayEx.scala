package com.mi
import org.apache.spark.sql.SparkSession
object ArrayEx {
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

    //만일 내가 컬럼 값을 정확히 알고 있을 경우 세트타입으로 바꾼 뒤
    var testSet = rawDataColumns.toSet
    //contains로 이 컬럼이 진짜로 있는지 어디에 있는지
    testSet.contains("abc")
    testSet.contains("qty")

    //데이터프레임에서 컬럼명을 바꿀때
    var testDf = rawData.toDF("AA","BB","CC","DD","EE","FF")
    testDf.show
    var testDf2 = rawData.toDF("aa","bb","cc","dd","ee","ff")
    testDf2.show
    var test1="aabbDDD"
    var test2="AABBDDD"
    test1.equalsIgnoreCase(test2)

    //소문자로 모두 변경시켜라
    test2.toLowerCase()
    test1.toLowerCase()

    //대문자로 모두 변경시켜라
    test2.toUpperCase()
    test1.toUpperCase()

    //RDD로 변환
    var rawRdd= rawData.rdd
    var rawExRdd = rawRdd.filter(x=>{
      var checkValid = false
      checkValid
    })
      rawExRdd.count //0개

    var rawRddEx = rawRdd.filter(x=>{
      var checkValid = true
      //설정 부적합 로직
      if(x.getString(3).length != 6){
        checkValid = false
      }
      checkValid
    })
    rawRddEx.count //124658개

    //교안 예제
    //데이터로딩

    var df = spark.sql("select * from sample_data")
    var df_index=df.columns

    var modelCN=df_index.indexOf("Model")
    var priceCN = df_index.indexOf("Price")
    var disCN = df_index.indexOf("Discount")

    //데이터 정제
    /* 유효 데이터 필터링 */
    //Price - Discount > 0

//    filterDF = df.rdd.filter(x=>{
//      var pri = x.getDouble(priceCN)
//      var dis = x.getDouble(disCN)
//      (pri>dis)
//    })


    //데이터분석
    /*프로모션 비율 산출*/
    //Rate = Discount/Price * 100
//    resultMap = filterDF.map(x=>{
//      var model = x.getString(priceCN)
//      var pri = x.getDouble(priceCN)
//      var rate = dis/pri * 100
//      (moedel,pri,dis,rate)
//    })



  }

}
