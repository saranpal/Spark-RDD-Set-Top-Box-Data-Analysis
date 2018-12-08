//7. Filter all the record with Event 115/118
//i.Get the  duration group by program_id
package com.saran.stb

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.lang.String
import java.lang.Float
import scala.xml.Elem
import org.apache.spark.sql.SparkSession
object DurationGroupByProgramAnanysis {
  
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
            evId == 115 || evId == 118 
         }
      }
      .filter { line => {
        val tokens = line.split("\\^")
        val body = tokens(4).toString()
        body.contains("<d>")
      }}
      .map {
          line => {
            val tokens = line.split("\\^")
            var duration_value = 0
            var program_id_value = ""
            val body = tokens(4).toString()
            //println("body :"+body)
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
              if(oneNVName == "ProgramId") {
                program_id_value = oneNVValue
              }
            }
            (program_id_value, duration_value)
         }
      }
      .groupByKey()
      //.map( rec => (rec._1, rec._2.max))
      //.sortBy(rec => (rec._2), false)
      //.take(5)
      result.foreach(println)
      
      spark.stop
    
  }
}