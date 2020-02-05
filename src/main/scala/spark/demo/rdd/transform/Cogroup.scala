package spark.demo.rdd.transform

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }

object Cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)    
    val datas1 = List((1, "x"), (2, "y"), (3, "z"), (4, "v"))

    val datas2 = List((1, 7), (2, 3), (3, 8), (4, 3))

    val datas3 = List((1, "7,8"), (2, "3"), (3, "8"), (4, "3"), (4, "4"), (4, "5"), (4, "6"))

    val rdd1 = sparkContext.parallelize(datas1)
    val rdd2 = sparkContext.parallelize(datas2)
    val rdd3 = sparkContext.parallelize(datas3)

    val rdd = rdd1.cogroup(rdd2,rdd3)
    rdd.foreach(tuple => {
      println("key:" + tuple._1 + "\tvalue:" + tuple._2)
    })

  }


 
  
}