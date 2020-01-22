package spark.demo.sql

import java.util
import java.util.{ ArrayList, List }
//import java.util.{List=>JList}

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame 
import scala.util.control._
import org.apache.commons.lang3.StringUtils

import java.text.SimpleDateFormat
import java.util.Date



object RemainUserWeek {
  
  var sc1 : SparkContext = null
  var dataSet1 : DataFrame = null
  
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }
  
    def StringToTime(tm:String) :Date={
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val dt = fm.parse(tm.trim().toString())
    dt
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ReMainUser").setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc1 = sc

    val sparkSession = SparkSession.builder().appName("ReMainUser").enableHiveSupport.getOrCreate()
    val lineRDD = sc.textFile("resources/loginData.txt")
    val rows2 = lineRDD.collect()
    
    for (s <- rows2) {
      println(s)
    }
       
    val rowsRDD = lineRDD.filter(StringUtils.isNotEmpty(_)).map(line => {
      val str = line.split(" ")      
      RowFactory.create(str(0).trim(),str(1).trim(),str(2).trim(),str(3).trim(),str(4).trim(),str(5).trim(),str(6).trim(),str(7).trim(),str(8).trim(),str(9).trim(),str(10).trim(),str(11).trim(),str(12).trim() )
    })
    println("===============================")
    val rows1 = rowsRDD.collect()
    for (s <- rows1) {
      println(s)
    }

    val fields = collection.mutable.ListBuffer[StructField]()
    fields += DataTypes.createStructField("channel", DataTypes.StringType, true)
    fields += DataTypes.createStructField("appkey", DataTypes.StringType, true)
    fields += DataTypes.createStructField("date", DataTypes.StringType, true)
    fields += DataTypes.createStructField("date1", DataTypes.StringType, true)
    fields += DataTypes.createStructField("appver", DataTypes.StringType, true)
    fields += DataTypes.createStructField("platform", DataTypes.StringType, true)
    fields += DataTypes.createStructField("udid", DataTypes.StringType, true)
    fields += DataTypes.createStructField("attr1", DataTypes.StringType, true)
    fields += DataTypes.createStructField("attr2", DataTypes.StringType, true)
    fields += DataTypes.createStructField("attr3", DataTypes.StringType, true)
    fields += DataTypes.createStructField("attr4", DataTypes.StringType, true)
    fields += DataTypes.createStructField("attr5", DataTypes.StringType, true)
    fields += DataTypes.createStructField("attr6", DataTypes.StringType, true)


    val schema = DataTypes.createStructType(fields.toArray)
    schema.printTreeString()

    val dataSet = sparkSession.createDataFrame(rowsRDD, schema)
    
    dataSet1 = dataSet
    
    //val eventinfo = dataSet.createTempView("eventinfo")
    dataSet.createTempView("eventinfo")

/*    val persons = sparkSession.sql("select * from eventinfo")
    val rows = persons.collect()

    for (s <- rows) {
      println(s)
    }
    
    
    
     * val sqlx = "SELECT b.appkey, b.platform, b.appver, b.channel, b.udid, b.login_time,c.first_week " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间，并按照这些字段分组
      "FROM " +
      "(SELECT appkey,platform,appver,channel,udid, date as login_time FROM eventinfo GROUP BY 1,2,3,4,5,6) b " + //获取应用、平台、版本、渠道、用户、所有登录时间，并按照这些字段分组
      "LEFT JOIN (" +
      "SELECT appkey,platform,appver,channel,udid,min(login_time) first_week " + //获取应用、平台、版本、渠道、用户、首次登陆时间，并按照这些字段分组
      "FROM (select appkey,platform,appver,channel,udid,date_sub(next_day(date, 'monday'), 7) login_time FROM eventinfo group by 1,2,3,4,5,6) a group by 1,2,3,4,5 " +
      ") c " +
      "on b.udid = c.udid order by 1,2,3,4,5,6"
    
    sparkSession.sql(sqlx).show()
    
    //  ROUND(datediff(login_time,first_week)/7)   CEILING 向上!!!   FLOOR向下！！！
    
    
    val sqlxx="SELECT  appkey, platform, appver, channel, udid,first_week, FLOOR(datediff(login_time,first_week)/7) as by_week " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间,时间差，并按照这些字段分组
      "FROM " +
      "(SELECT b.appkey, b.platform, b.appver, b.channel, b.udid, b.login_time,c.first_week " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间，并按照这些字段分组
      "FROM " +
      "(SELECT appkey,platform,appver,channel,udid, date as login_time FROM eventinfo GROUP BY 1,2,3,4,5,6) b " + //获取应用、平台、版本、渠道、用户、所有登录时间，并按照这些字段分组
      "LEFT JOIN (" +
      "SELECT appkey,platform,appver,channel,udid,min(login_time) first_week " + //获取应用、平台、版本、渠道、用户、首次登陆时间，并按照这些字段分组
      "FROM (select appkey,platform,appver,channel,udid,date_sub(next_day(date, 'monday'), 7) login_time FROM eventinfo group by 1,2,3,4,5,6) a group by 1,2,3,4,5 " +
      ") c " +
      "on b.udid = c.udid order by 1,2,3,4,5,6) e group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6"
        
    sparkSession.sql(sqlxx).show()
    
   */
    remainWeek(sparkSession,"resources/loginData.txt");

  }

  
  
   def remainWeek (spark :SparkSession, eventPath : String): Unit ={
    //val eventinfoDF = spark.read.format("parquet").load(eventPath)
    //val lineRDD = sc1.textFile(eventPath)
    //dataSet1.createTempView("eventinfo")
    //eventinfoDF.createOrReplaceTempView("eventinfo")
    val remainSql = "SELECT appkey, platform, appver, channel, first_week," +
      "sum(case when by_week = 0 then 1 else 0 end) week_0," +
      "sum(case when by_week = 1 then 1 else 0 end) week_1," +
      "sum(case when by_week = 2 then 1 else 0 end) week_2," +
      "sum(case when by_week = 3 then 1 else 0 end) week_3," +
      "sum(case when by_week = 4 then 1 else 0 end) week_4," +
      "sum(case when by_week = 5 then 1 else 0 end) week_5," +
      "sum(case when by_week = 6 then 1 else 0 end) week_6," +
      "sum(case when by_week = 7 then 1 else 0 end) week_7," +
      "sum(case when by_week = 14 then 1 else 0 end) week_14," +
      "sum(case when by_week = 30 then 1 else 0 end) week_30 "+
      " FROM " +
      "(" +
      "SELECT  appkey, platform, appver, channel, udid, first_week, FLOOR(datediff(login_time,first_week)/7) as by_week " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间,时间差，并按照这些字段分组
      "FROM " +
      "(SELECT b.appkey, b.platform, b.appver, b.channel, b.udid, b.login_time,c.first_week " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间，并按照这些字段分组
      "FROM " +
      "(SELECT appkey,platform,appver,channel,udid, date as login_time FROM eventinfo GROUP BY 1,2,3,4,5,6) b " + //获取应用、平台、版本、渠道、用户、所有登录时间，并按照这些字段分组
      "LEFT JOIN (" +
      "SELECT appkey,platform,appver,channel,udid,min(login_time) first_week " + //获取应用、平台、版本、渠道、用户、首次登陆时间，并按照这些字段分组
      "FROM (select appkey,platform,appver,channel,udid,date_sub(next_day(date, 'monday'), 7) login_time FROM eventinfo group by 1,2,3,4,5,6) a group by 1,2,3,4,5 " +
      ") c " +
      "on b.udid = c.udid order by 1,2,3,4,5,6) e group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6" +
      ") f " +
      "group by 1,2,3,4,5 order by 1,2,3,4,5"
    spark.sql(remainSql).show(1000,false)
  }
  
}
