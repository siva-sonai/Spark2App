package com.sparkapp.test.wc

import org.apache.spark.sql.SparkSession


object WordCount {
  
   def main(args: Array[String]){
     
      val sparkSession = SparkSession.
              builder
              .master("local")
              .appName("WordCount")
              .getOrCreate()
              
   import sparkSession.implicits._
   val data = sparkSession.read.text("/usr/local/spark/README.md").as[String]
   val words = data.flatMap(value => value.split("\\s+"))
    val groupedwords = words.groupByKey(_.toLowerCase)
    val counts = groupedwords.count()
    counts.show()
   
     
   }
  
  
}