package spark.demo.sql

import java.util
import java.util.{ ArrayList, List }
//import java.util.{List=>JList}

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.control._
import org.apache.commons.lang3.StringUtils

import java.text.SimpleDateFormat
import java.util.Date



object PVCount {
  
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ReMainUser").setMaster("local[2]")
    val sc = new SparkContext(conf)



    val sparkSession = SparkSession.builder().appName("PVCount").enableHiveSupport.getOrCreate()
    val lineRDD = sc.textFile("resources/pvuv.txt")
    val rows2 = lineRDD.collect()
    
    for (s <- rows2) {
      println(s)
    }
       
    val rowsRDD = lineRDD.filter(StringUtils.isNotEmpty(_)).map(line => {
      val str = line.split("\t")      
      RowFactory.create(str(0).trim(),str(1).trim(),str(2).trim(),str(3).trim(),str(4).trim(),str(5).trim(),str(6).trim())
    })
    println("===============================")
    val rows1 = rowsRDD.collect()
    for (s <- rows1) {
      println(s)
    }

    val fields = collection.mutable.ListBuffer[StructField]()
    fields += DataTypes.createStructField("ip", DataTypes.StringType, true)
    fields += DataTypes.createStructField("province", DataTypes.StringType, true)
    fields += DataTypes.createStructField("date", DataTypes.StringType, true)
    fields += DataTypes.createStructField("uid", DataTypes.StringType, true)
    fields += DataTypes.createStructField("xx", DataTypes.StringType, true)
    fields += DataTypes.createStructField("from", DataTypes.StringType, true)
    fields += DataTypes.createStructField("page", DataTypes.StringType, true)



    val schema = DataTypes.createStructType(fields.toArray)
    schema.printTreeString()

    val dataSet = sparkSession.createDataFrame(rowsRDD, schema)
    dataSet.createTempView("eventinfo")

    //PV
    sparkSession.sql("select date,count(*) as pv from eventinfo group by date").show(1000,false)
   
    //UV
    sparkSession.sql("SELECT date,COUNT(*) as uv FROM (select uid,date from eventinfo group by uid,date) group by date").show(1000,false)
   
  }

}
