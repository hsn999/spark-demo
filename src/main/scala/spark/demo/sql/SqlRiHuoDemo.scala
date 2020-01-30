package spark.demo.sql

import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.{ SparkConf, SparkContext }
import spark.demo.MockData
import org.apache.spark.sql.SQLContext

object SqlRiHuoDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().appName("JSONDataSource").getOrCreate()
    
    val sqlc =  new SQLContext(sc)
    
    MockData.mock(sc, sqlc)

    //user_visit_action_new,调用java生成的测试数据
    val xDF = sparkSession.sql("SELECT *  FROM user_visit_action_new")
    xDF.show()
    
    
  }

}