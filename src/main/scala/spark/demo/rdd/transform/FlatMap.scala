package spark.demo.rdd.transform

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }

object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]")
    val sc = new SparkContext(conf)    
    val datas = List("aa,bb,cc", "cxf,spring,struts2", "java,C++,javaScript")
    val rdd = sc.parallelize(datas)

    rdd.flatMap(str => {
      str.split(",")
    }).foreach(t => {
      println(t)
    })
    
  }
}