package spark.demo.rdd.transform

import Array._
import scala.collection.mutable.Set

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
//import scala.collection.JavaConverters._

object RDDTest { 
 
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array((0, 5), (0, 4), (2, 3), (2, 2), (0, 1)))
    //val javaRequest: java.util.List[s] = request.asJava
    val r = rdd.collect()
    r.foreach(println)

    val rddmapValues = rdd.mapValues { x => x * 10 }
    val rm = rddmapValues.collect()

    //reduceByKey
    val rddreduceByKey = rdd.reduceByKey((x, y) =>
      {
        val tname = Thread.currentThread().getName
        println(tname)
        x + y
      })
    val rrb = rddreduceByKey.collect()

    val rddx = sc.parallelize(Array((0, 9), (0, 4), (2, 3), (2, 6), (0, 1), (0, 6), (4, 1), (4, 3)))

    //join
    val rddJoin = rdd.join(rddx)
    val prj = rddJoin.collect()
    prj.foreach(println)
    /**
     * (0,(5,9))
     * (0,(5,4))
     * (0,(5,1))
     * (0,(5,6))
     * (0,(4,9))
     * (0,(4,4))
     * (0,(4,1))
     * (0,(4,6))
     * (0,(1,9))
     * (0,(1,4))
     * (0,(1,1))
     * (0,(1,6))
     * (2,(3,3))
     * (2,(3,6))
     * (2,(2,3))
     * (2,(2,6))
     * *
     */

    val rddy = rdd.union(rddx)
    rddy.cache() //持久化

    val rddRroupByK = rddy.groupByKey(2)
    val rrddRroupByK = rddRroupByK.collect()
    rrddRroupByK.foreach(println)

    //combineByKey 下面的操作结果和groupByKey类似效果，下面的a是value
    val createCombiner = (a: Int) => List(a) //将5转成List(5)
    val mergeValue = (a: List[Int], b: Int) => a.::(b) //将List(5)和4操作成 List(5,4)
    val mergeCombiners = (a: List[Int], b: List[Int]) => a.:::(b) //将List(4,5)和List(1)操作得List(1,4,5)
    val rdd1 = rddy.combineByKey(createCombiner, mergeValue, mergeCombiners)

    val r1 = rdd1.collect()
    println("=================")
    r1.foreach(println)
    /**
     *
     * (4,List(3, 1))
     * (0,List(6, 1, 4, 9, 1, 4, 5))
     * (2,List(6, 3, 2, 3))
     */


  }

}
