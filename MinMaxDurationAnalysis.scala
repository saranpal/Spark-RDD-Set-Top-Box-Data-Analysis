//3. Filter all the record with event_id=118
//i. Get the min and maximum duration
package com.saran.stb

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.lang.String
import java.lang.Float
import scala.xml.Elem
import org.apache.spark.sql.SparkSession

object MinMaxDurationAnalysis {
  
   def main(args: Array[String]) {
      
      System.setProperty("hadoop.home.dir", "/Users/saranpal/Documents/setups/hadoop-2.5.0-cdh5.3.2")
      System.setProperty("spark.sql.warehouse.dir", "file:/Users/saranpal/Documents/setups/spark-2.0.2-bin-hadoop2.6/spark-warehouse")
      
      val spark = SparkSession
                  .builder
                  .appName("Top Five Devices")
                  .master("local")
                  .getOrCreate()
                  
       val data = spark.read.textFile("file:/Users/saranpal/Desktop/data-spark/Set_Top_Box_Data.txt").rdd
       
       val result = data.filter {
          line => {
            val tokens = line.split("\\^")
            val evId = Integer.parseInt(tokens(2).toString())
            evId == 118 
         }
      }
      .map {
          line => {
            val tokens = line.split("\\^")
            var duration_value = 0
            val body = tokens(4).toString()
            val xml = XML.loadString(body)
            for (nv <- xml.child) {
              //println("Rows :"+nv)
              val oneNV = XML.loadString(nv.toString())
              val oneNVName = oneNV.attribute("n").fold("")(_.toString)
              val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
              //println("oneNVValue :"+oneNVValue)
              if(oneNVName == "DurationSecs" && oneNVValue!="") {
                duration_value = Integer.parseInt(oneNVValue)
              }
            }
            (duration_value)
         }
      };
      /* 
       * One way
        val max_result = result.sortByKey(false)
        .take(1)
        max_result.foreach(println)
        
        println("Max duration :"+max_result)
      * 
      */
      println("Max duration :"+result.max())
      
      /* 
       * One way
        val min_result = result.sortByKey(true)
        .take(1)
        min_result.foreach(println)
        println("Min duration :"+min_result)
       * 
       */
      println("Min duration :"+result.min())
      
      spark.stop
    
  }
}