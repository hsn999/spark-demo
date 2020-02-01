# spark-demo

**Spark SQL **  
~~~
Spark SQL是用于结构化数据处理的Spark模块。与基本的Spark RDD API不同，Spark SQL提供的接口为Spark提供了有关数据结构和正
在执行的计算的更多信息。在内部，Spark SQL使用这些额外的信息来执行额外的优化。与Spark SQL交互的方法有多种，包括SQL和Dataset API。
计算结果时，将使用相同的执行引擎，而与要用来表达计算的API /语言无关。这种统一意味着开发人员可以轻松地在不同的API之间来回切换，
从而提供最自然的方式来表达给定的转换。
~~~
* Spark以反射方式创建DataFrame
~~~
val fields = collection.mutable.ListBuffer[StructField]()
    fields += DataTypes.createStructField("name", DataTypes.StringType, true)
    fields += DataTypes.createStructField("age", DataTypes.IntegerType, true)

    val schema = DataTypes.createStructType(fields.toArray)
    schema.printTreeString() 

    val dataSet = sparkSession.createDataFrame(rowsRDD, schema)

~~~
* Spark以反射方式创建DataFrame

~~~
埋点采集，一般是基于日志的计算方式居多，那么日志里记录哪些信息，主要根据你要统计的指标、维度、最后需要的报表结果来定，
理论上前期采集日志里携带的信息越多，后续想要增加一些维度指标时，就越不需要再动埋点处的代码，并且可以有历史日志现成的数据使用，
但所需记录的信息太多也会造成日志量体积变大，甚至影响业务性能，比如社区用户发布一条回答时，假设你希望在日志中同时记录下发布
当时的用户等级级别信息，而业务原本发布一条回答是不需要额外查这个用户等级数据的，如果为了你日志需要还做额外的查询，这就可能
会影响业务执行效率和稳定性了，就要综合考虑这个信息的采集是否值得。

前端埋点和后端埋点，举个例子，如果你要统计用户点击发布回答按钮的点击率，一般是前端点击按钮行为发送点击日志，页面按钮展示
发送pv展示日志，然后点击量/展示量，就是点击率，这里就要前端埋点。而如果你要统计回答发布成功量，那就要在后端发布回答的接口处埋点，
因为只有执行发布回答的接口后端服务，才知道回答成功发布没有。如果你用的是点击按钮的前端埋点方式去统计，比如用户发布一条回答时，
他在前端连续快速点了n次发布回答按钮，发送了n条点击日志，那你统计得到的发布数量是n，这就不准确了，实际发布一条回答，
后端埋点会统计得到发布量就是1。

还有一些统计是要前后端都埋点，配合来用的，这时可能使用一个前后端统一定义的唯一TrackID，
比如要得知有多少用户从A页面跳转到B页面，再有多少成功下订单，成功下的订单里，有多少是来自A页面的，
就要在A页面就开始把跟踪ID带到B页面，前端埋点发送页面跳转日志，再由B页面将跟踪ID传给订单接口，再由订单接口执行成功时发出后端日志。

~~~


**日计算留存率**  ReMainUserDay.scala
* 留存率=新增用户中登录用户数/新增用户数*100%（一般统计周期为天）
* 新增用户数：在某个时间段（一般为第一整天）新登录应用的用户数；
* 登录用户数：登录应用后至当前时间，至少登录过一次的用户数；
* 第N日留存：指的是新增用户日之后的第N日依然登录的用户占新增用户的比例
* 第1日留存率（即“次留”）：（当天新增的用户中，新增日之后的第1天还登录的用户数）/第一天新增总用户数；
* 第2日留存率：（当天新增的用户中，新增日之后的第2天还登录的用户数）/第一天新增总用户数；
* 第3日留存率：（当天新增的用户中，新增日之后的第3天还登录的用户数）/第一天新增总用户数；
* 第7日留存率：（当天新增的用户中，新增日之后的第7天还登录的用户数）/第一天新增总用户数；
* 第30日留存率：（当天新增的用户中，新增日之后的第30天还登录的用户数）/第一天新增总用户数；

**思路：**
* 拿到用户的所有登录时间（用户的登录记录都在hadoop上）
* 增加一列用户的首次登陆时间（实际情况，可以增加一个用户登录记录表）
* 增加一列用户登录的时间差 （计算出时间差，按日记）
* 增加几列case when 获取业务留存天数
* 计算留存与所用用户比例

~~~
    val remainSql = "SELECT appkey, platform, appver, channel, first_day," +
      "sum(case when by_day = 0 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_0," +
      "sum(case when by_day = 1 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_1," +
      "sum(case when by_day = 2 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_2," +
      "sum(case when by_day = 3 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_3," +
      "sum(case when by_day = 4 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_4," +
      "sum(case when by_day = 5 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_5," +
      "sum(case when by_day = 6 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_6," +
      "sum(case when by_day = 7 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_7," +
      "sum(case when by_day = 14 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_14," +
      "sum(case when by_day = 30 then 1 else 0 end)/sum(case when by_day = 0 then 1 else 0 end) day_30 "+
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
    val dayData = sparkSession.sql(remainSql).show(1000,false)
~~~

**周计算留存率**  RemainUserWeek.scala
**思路：**
* 拿到用户的所有登录时间，计算出周一
* 增加一列用户的首次登陆时间，计算出周一
* 增加一列用户登录的时间差 （ FLOOR(datediff(login_time,first_week)/7) 计算出时间差，按周记）
* 增加几列case when 获取业务留存周数
* 计算留存与所用用户比例
~~~
    val remainSql = "SELECT appkey, platform, appver, channel, first_week," +
      "sum(case when by_week = 0 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_0," +
      "sum(case when by_week = 1 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_1," +
      "sum(case when by_week = 2 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_2," +
      "sum(case when by_week = 3 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_3," +
      "sum(case when by_week = 4 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_4," +
      "sum(case when by_week = 5 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_5," +
      "sum(case when by_week = 6 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_6," +
      "sum(case when by_week = 7 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_7," +
      "sum(case when by_week = 14 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_14," +
      "sum(case when by_week = 30 then 1 else 0 end)/sum(case when by_week = 0 then 1 else 0 end) week_30 "+
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
~~~

