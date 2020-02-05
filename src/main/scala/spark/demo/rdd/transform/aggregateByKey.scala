package spark.demo.rdd.transform

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }


object aggregateByKey {
   def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]")
    val sc = new SparkContext(conf)    
    val datas = List((1, 3), (2, 6), (1, 4), (2, 6),(1, 13),(3, 3),(3, 13))
    val rdd = sc.parallelize(datas, 2)
    val tuples1 = rdd.aggregateByKey(0)((sum: Int, value: Int) => {
      println("seq:" + sum + "\t" + value)
      sum + value
    }, (sum: Int, value: Int) => {
      println("comb:" + sum + "\t" + value)
      sum + value
    }).collect()
    for (t <- tuples1) {
      println(t._1 + "   " + t._2)
    }

    val tuples2 = rdd.reduceByKey((sum: Int, value: Int) => {
      println("sum:" + sum + "\t" + "value:" + value)
      sum + value
    }).collect()
    for (t <- tuples2) {
      println(t._1 + "   " + t._2)
    }
    
    
   }
  
}