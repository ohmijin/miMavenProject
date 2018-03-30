package com.mi

object funcEx {

  def main(args: Array[String]): Unit = {

    def yearWeek(date:String, delimeter:String): (String,String) = {
      var arrayYW = date.split(delimeter)
      var yearValue = arrayYW(0)
      var weekValue = arrayYW(1)
      (yearValue, weekValue)
    }
    var date = "2018;03"
    var delimeter = ";"
    println( yearWeek(date,delimeter) )

   //선생님 코드



  }

}
