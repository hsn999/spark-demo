package spark.demo.sql

import java.util
import java.util.{ ArrayList, List }
//import java.util.{List=>JList}

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }

object RDD2DataFrameReflection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)



    val sparkSession = SparkSession.builder().appName("RDD2DataFrameReflection").enableHiveSupport.getOrCreate()

    // val lineRDD = sparkContext.textFile(Constant.LOCAL_FILE_PREX + "/data/resources/people.txt")
    val lineRDD = sc.textFile("resources/people.txt")
    val rowsRDD = lineRDD.map(line => {
      val str = line.split(",")
      RowFactory.create(str(0), Integer.valueOf((str(1).trim())))
    })
    println("===============================")
    //rowsRDD.collect()

    val fields = collection.mutable.ListBuffer[StructField]()
    fields += DataTypes.createStructField("name", DataTypes.StringType, true)
    fields += DataTypes.createStructField("age", DataTypes.IntegerType, true)

    val schema = DataTypes.createStructType(fields.toArray)
    schema.printTreeString()

    val dataSet = sparkSession.createDataFrame(rowsRDD, schema)
    dataSet.createTempView("person")

    val persons = sparkSession.sql("select * from person")
    val rows = persons.collect()

    for (s <- rows) {
      println(s)
    }

    // /tmp hadoop fs -chmod 777 /tmp  save to hdfs
    persons.write.parquet("hdfs://192.168.1.123:8020/tmp/p.parquet")

  }

}
