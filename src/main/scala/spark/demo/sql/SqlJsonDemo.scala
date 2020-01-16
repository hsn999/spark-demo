package spark.demo.sql

import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.{ SparkConf, SparkContext }

object SqlJsonDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().appName("JSONDataSource").getOrCreate()
    val dataFrameReader = sparkSession.read
    //    val dataset = dataFrameReader.json("/data/resources/people.json")
    val dataset = dataFrameReader.json("resources/people.json")
    dataset.printSchema

    // зЂВс
    dataset.createTempView("people")
    val teenagers = sparkSession.sql("SELECT name FROM people WHERE age >= 13 AND age <= 99")
    val names = teenagers.rdd.map(row => {
      "name:" + row.getString(0)
    })
    names.foreach(println)

    //dataset.write.format("json").mode(SaveMode.Overwrite).save("student")

    import sparkSession.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "resources/people.json"
    val peopleDF = sparkSession.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = sparkSession.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    // +------+
    // |  name|
    // +------+
    // |Justin|
    // +------+

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a Dataset[String] storing one JSON object per string
    val otherPeopleDataset = sparkSession.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = sparkSession.read.json(otherPeopleDataset)
    otherPeople.show()
    
    otherPeople.createOrReplaceTempView("peopleT")
    otherPeople.printSchema()
    
    val result = sparkSession.sql("SELECT * FROM peopleT ")
    
    result.show()
  }

}