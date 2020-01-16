package spark.demo.sql

import java.io.Serializable
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{ SparkConf, SparkContext }


object RDD2DataFrameReflectionDynamic {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    
    val sparkSession = SparkSession.builder().appName("RDD2DataFrameReflectionDynamic").getOrCreate()

    val lineRDD = sc.textFile("resources/people.txt")

    val personsRDD = lineRDD.map(line => {
      val parts = line.split(",")
      val person = new Person
      person.setName(parts(0))
      person.setAge(parts(1).trim.toInt)
      person
    })


    val personDataset: Dataset[Row] = sparkSession.createDataFrame(personsRDD, classOf[RDD2DataFrameReflectionDynamic.Person]) 
    personDataset.createTempView("person")
    val teenagers = sparkSession.sql("select * from person where age >= 13 and age <= 19")
    teenagers.printSchema()
    val persons = teenagers.rdd.map(row => {

      val age:Int = row.getAs("age")
      val name:String = row.getAs("name")
      val person = new Person
      person.setName(name)
      person.setAge(age)
      person
    }).collect
    persons.foreach(println)
  }

  class Person extends Serializable {
    private var name: String = _
    private var age: Int = 0

    def getName: String = name

    def setName(name: String): Unit = {
      this.name = name
    }

    def getAge: Int = age

    def setAge(age: Int): Unit = {
      this.age = age
    }

    override def toString: String = "Person [name=" + name + ", age=" + age + "]"
  }

}
