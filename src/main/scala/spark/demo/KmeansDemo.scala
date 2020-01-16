package spark.demo

import org.apache.spark.{ SparkConf, SparkContext }
// $example on$
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {

  def main(args: Array[String]) {
    
    //kmeans_data.txt
    //0.1 0.1 0.1
    //0.2 0.2 0.2
    //4.6 6.5 5.1
    //6.3 5.4 3.6
    //5.6 5.5 4.3
    //9.0 9.0 9.0
    //9.1 9.1 9.1
    //9.2 9.2 9.2

    //val conf = new SparkConf().setAppName("KMeansExample")
    //val sc = new SparkContext(conf)

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data = sc.textFile("c:\\kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into THREE classes using KMeans
    val numClusters = 3
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    //clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    //val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    //3个类簇的质心
    println("Cluster centers:")
    for (c <- clusters.clusterCenters) {
      println(c.toString)
    }

    //测试单点数据
    println(" ")
    val v1 = Vectors.dense("6.5 4.444 3.0".split(" ").map(_.toDouble))

    println(s"v1=(${v1.apply(0)},${v1.apply(1)},${v1.apply(2)}) is belong to cluster:" + clusters.predict(v1))

    sc.stop()
  }
}