package spark.demo.sql

import java.util
import java.util.{ ArrayList, List }
//import java.util.{List=>JList}

import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.apache.spark.sql.{ RowFactory, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.control._
import org.apache.commons.lang3.StringUtils

import java.text.SimpleDateFormat
import java.util.Date



object ReMainUser {
  
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
    dataSet.createTempView("eventinfo")

    val persons = sparkSession.sql("select * from eventinfo")
    val rows = persons.collect()

    for (s <- rows) {
      println(s)
    }
    
    
   val sql1 = "SELECT b.appkey, b.platform, b.appver, b.channel, b.udid, b.login_time,c.first_day " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间，并按照这些字段分组
                   "FROM " +
                          "(SELECT appkey,platform,appver,channel,udid,date login_time FROM eventinfo GROUP BY 1,2,3,4,5,6) b " + //获取应用、平台、版本、渠道、用户、所有登录时间，并按照这些字段分组
                   "LEFT JOIN (" +
                          "SELECT appkey,platform,appver,channel,udid,min(login_time) first_day " + //获取应用、平台、版本、渠道、用户、首次登陆时间，并按照这些字段分组
                                                 "FROM (select appkey,platform,appver,channel,udid,date login_time FROM eventinfo group by 1,2,3,4,5,6) a group by 1,2,3,4,5 " +
                              ") c " +
                   "on b.udid = c.udid order by 1,2,3,4,5,6"
                              
  val re = sparkSession.sql(sql1)
  
  re.printSchema()
  re.createOrReplaceTempView("eventinfo1")
  re.show()
  //import org.apache.spark.sql.functions.to_json  
  //eventinfo1.select(to_json(struct($"c1", $"c2", $"c3")))
  
  //re.se
  
  /*val shapeTime = udf((time:String) => {
     time.replaceAll("\\+"," ")
   })*/
  
  import org.joda.time.{DateTimeZone}
  import org.joda.time.format
  
  val xxx = sparkSession.sql("SELECT login_time,to_date(concat(xc.login_time,' ')),to_timestamp(xc.login_time,'yyyy-MM-dd') aaa,login_time from eventinfo1 xc")
  xxx.printSchema()
  xxx.show()
  /*val sql2 ="SELECT  appkey, platform, appver, channel, udid, trim(first_day) as first_day, to_timestamp(trim(first_day),'yyyy-MM-dd') as logdate, datediff(to_timestamp(login_time,'yyyy-MM-dd'),to_timestamp(first_day,'yyyy-MM-dd')) as by_day " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间,时间差，并按照这些字段分组
          "FROM " +
                  "(SELECT b.appkey, b.platform, b.appver, b.channel, b.udid, b.login_time,c.first_day " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间，并按照这些字段分组
                   "FROM " +
                          "(SELECT appkey,platform,appver,channel,udid,date login_time FROM eventinfo GROUP BY 1,2,3,4,5,6) b " + //获取应用、平台、版本、渠道、用户、所有登录时间，并按照这些字段分组
                   "LEFT JOIN (" +
                          "SELECT appkey,platform,appver,channel,udid,min(login_time) first_day " + //获取应用、平台、版本、渠道、用户、首次登陆时间，并按照这些字段分组
                                                 "FROM (select appkey,platform,appver,channel,udid,date login_time FROM eventinfo group by 1,2,3,4,5,6) a group by 1,2,3,4,5 " +
                              ") c " +
                   "on b.udid = c.udid order by 1,2,3,4,5,6) e order by 1,2,3,4,5,6" 
      
      sparkSession.sql(sql2).show(1000,false) */
      
      
    sparkSession.sql("SELECT to_date('2016-12-31', 'yyyy-MM-dd')").show()

    
   /* val remainSql = "SELECT appkey, platform, appver, channel, first_day," +
      "sum(case when by_day = 0 then 1 else 0 end) day_0," +
      "sum(case when by_day = 1 then 1 else 0 end) day_1," +
      "sum(case when by_day = 2 then 1 else 0 end) day_2," +
      "sum(case when by_day = 3 then 1 else 0 end) day_3," +
      "sum(case when by_day = 4 then 1 else 0 end) day_4," +
      "sum(case when by_day = 5 then 1 else 0 end) day_5," +
      "sum(case when by_day = 6 then 1 else 0 end) day_6," +
      "sum(case when by_day = 7 then 1 else 0 end) day_7," +
      "sum(case when by_day = 14 then 1 else 0 end) day_14," +
      "sum(case when by_day = 30 then 1 else 0 end) day_30 "+
      " FROM " +
      "(" +
          "SELECT  appkey, platform, appver, channel, udid, first_day, datediff(login_time,first_day) as by_day " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间,时间差，并按照这些字段分组
          "FROM " +
                  "(SELECT b.appkey, b.platform, b.appver, b.channel, b.udid, b.login_time,c.first_day " + //获取应用、平台、版本、渠道、用户、所有登录时间、首次登陆时间，并按照这些字段分组
                   "FROM " +
                          "(SELECT appkey,platform,appver,channel,udid,date login_time FROM eventinfo GROUP BY 1,2,3,4,5,6) b " + //获取应用、平台、版本、渠道、用户、所有登录时间，并按照这些字段分组
                   "LEFT JOIN (" +
                          "SELECT appkey,platform,appver,channel,udid,min(login_time) first_day " + //获取应用、平台、版本、渠道、用户、首次登陆时间，并按照这些字段分组
                                                 "FROM (select appkey,platform,appver,channel,udid,date login_time FROM eventinfo group by 1,2,3,4,5,6) a group by 1,2,3,4,5 " +
                              ") c " +
                   "on b.udid = c.udid order by 1,2,3,4,5,6) e order by 1,2,3,4,5,6" +
      ") f " +
      "group by 1,2,3,4,5 order by 1,2,3,4,5"
    val dayData = sparkSession.sql(remainSql).show(1000,false)*/
    
    

    
    // /tmp hadoop fs -chmod 777 /tmp  save to hdfs
    //persons.write.parquet("hdfs://192.168.1.123:8020/tmp/p.parquet")

  }

}
