//5. Filter all the record with Event 0
//i. Calculate how many junk records are their having BadBlocks in XML column

package com.saran.stb

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.lang.String
import java.lang.Float
import scala.xml.Elem
import org.apache.spark.sql.SparkSession
object JunkRecordAnalysis {
  
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
            evId == 0 
         }
      }
      .filter { 
        line => {
          val tokens = line.split("\\^")
          val body = tokens(4).toString()
          body.contains("<d>")
        }
      }
      .map {
          line => {
            val tokens = line.split("\\^")
            var badblocks = ""
            val body = tokens(4).toString()
            val dvId = tokens(5).toString()
            //println("body :"+body)
            val xml = XML.loadString(body)
            for (nv <- xml.child) {
              //println("Rows :"+nv)
              val oneNV = XML.loadString(nv.toString())
              val oneNVName = oneNV.attribute("n").fold("")(_.toString)
              val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
              //println("oneNVValue :"+oneNVValue)
              if(oneNVName == "BadBlocks") {
                badblocks = oneNVName
              }
            }
            //println("badblocks :"+line+" : "+badblocks)
            (badblocks, line)
         }
      }
      .filter(x => (x._1 =="BadBlocks"))
      //.take(5)
      
      result.foreach(println)
      println("Total junk records are thier having BadBlocks in xml column : " + result.count())
      
      spark.stop
    
  }
}