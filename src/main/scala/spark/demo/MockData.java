package spark.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import spark.demo.utils.DateUtils;
import spark.demo.utils.StringUtils;

/**
 * 模拟数据程序
 * 
 * @author Administrator
 *
 */
public class MockData {

	/**
	 * 模拟数据
	 * 
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc, SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();

		String[] searchKeywords = new String[] { "火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
				"呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉" };

		// 生成的数据日期为当天日期，设置搜索参数时应注意搜索时间范围
		String date = DateUtils.getTodayDate();
		String[] actions = new String[] { "search", "click", "order", "pay" };
		Random random = new Random();

		for (int i = 0; i < 100; i++) {
			long userid = random.nextInt(100);

			for (int j = 0; j < 10; j++) {
				String sessionid = UUID.randomUUID().toString()
						.replace("-", "");
				String baseActionTime = date + " " + random.nextInt(23);

				for (int k = 0; k < random.nextInt(100); k++) {
					long pageid = random.nextInt(10);
					String actionTime = baseActionTime
							+ ":"
							+ StringUtils.fulfuill(String.valueOf(random
									.nextInt(59)))
							+ ":"
							+ StringUtils.fulfuill(String.valueOf(random
									.nextInt(59)));
					String searchKeyword = null;
					Long clickCategoryId = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;

					String action = actions[random.nextInt(4)];
					if ("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];
					} else if ("click".equals(action)) {
						clickCategoryId = Long.valueOf(String.valueOf(random
								.nextInt(100)));
						clickProductId = Long.valueOf(String.valueOf(random
								.nextInt(100)));
					} else if ("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if ("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));
						payProductIds = String.valueOf(random.nextInt(100));
					}

					Row row = RowFactory.create(date, userid, sessionid,
							pageid, actionTime, searchKeyword, clickCategoryId,
							clickProductId, orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds);
					rows.add(row);
				}
			}
		}

		JavaRDD<Row> rowsRDD = sc.parallelize(rows);

		StructType schema = DataTypes
				.createStructType(Arrays.asList(DataTypes.createStructField(
						"date", DataTypes.StringType, true),
						DataTypes.createStructField("user_id",
								DataTypes.LongType, true), DataTypes
								.createStructField("session_id",
										DataTypes.StringType, true), DataTypes
								.createStructField("page_id",
										DataTypes.LongType, true), DataTypes
								.createStructField("action_time",
										DataTypes.StringType, true), DataTypes
								.createStructField("search_keyword",
										DataTypes.StringType, true), DataTypes
								.createStructField("click_category_id",
										DataTypes.LongType, true), DataTypes
								.createStructField("click_product_id",
										DataTypes.LongType, true), DataTypes
								.createStructField("order_category_ids",
										DataTypes.StringType, true), DataTypes
								.createStructField("order_product_ids",
										DataTypes.StringType, true), DataTypes
								.createStructField("pay_category_ids",
										DataTypes.StringType, true), DataTypes
								.createStructField("pay_product_ids",
										DataTypes.StringType, true)));

		Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema);

		df.registerTempTable("user_visit_action");
		df.createOrReplaceTempView("user_visit_action_new");

		JavaRDD<Row> rowJavaRDD = df.javaRDD();
		df.show();

		/**
		 * ==================================================================
		 */

		rows.clear();
		String[] sexes = new String[] { "male", "female" };
		for (int i = 0; i < 100; i++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];

			Row row = RowFactory.create(userid, username, name, age,
					professional, city, sex);
			rows.add(row);
		}

		rowsRDD = sc.parallelize(rows);

		StructType schema2 = DataTypes
				.createStructType(Arrays.asList(
						DataTypes.createStructField("user_id", DataTypes.LongType, true), 
						DataTypes.createStructField("username", DataTypes.StringType,true), 
						DataTypes.createStructField("name",DataTypes.StringType, true), 
						DataTypes.createStructField("age", DataTypes.IntegerType, true),
						DataTypes.createStructField("professional",DataTypes.StringType, true), 
						DataTypes.createStructField("city",DataTypes.StringType, true), 
						DataTypes.createStructField("sex", DataTypes.StringType,true)));

		Dataset<Row> df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		df2.show();

		df2.registerTempTable("user_info");
	}

	public static void userInfo(JavaSparkContext sc, SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();
		String[] birthYear = new String[] { "1994", "1996", "1998", "1986",
				"1974", "1988", "1999", "1976", "1964", "1993" };
		String[] city = new String[] { "上海", "北京", "深圳", "西安", "重庆", "桂林",
				"广州", "杭州", "成都", "西塘", "香港", "台北" };
		String[] login = new String[] { "2018-4-5", "2018-5-5", "2018-4-6",
				"2018-4-7", "2018-4-9", "2018-4-12", "2018-4-13", "2018-4-20",
				"2018-4-21", "2018-4-22", "2018-4-24", "2018-4-29" };

		Random random = new Random();
		for (int i = 0; i < 100; i++) {
			long userid = i;
			String userName = "name" + i;
			String userBirthYear = birthYear[random.nextInt(10)];
			String userCity = city[random.nextInt(12)];
			String userFirstLoginTime = login[random.nextInt(12)];

			Row row = RowFactory.create(userid, userName, userBirthYear,
					userCity, userFirstLoginTime);
			rows.add(row);
		}
		/*SparkSession sparkSession = SparkSession.builder()
				.appName("sparkAnalysis").master("local[*]")
				.config("spark.driver.memory", "2147480000").getOrCreate();
		DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);*/
		
		StructType schema = DataTypes
				.createStructType(Arrays.asList(
						DataTypes.createStructField("userid", DataTypes.LongType, true), 
						DataTypes.createStructField("userName", DataTypes.StringType,true), 
						DataTypes.createStructField("userBirthYear",DataTypes.StringType, true), 
						DataTypes.createStructField("userCity",DataTypes.StringType, true), 
						DataTypes.createStructField("userFirstLoginTime",DataTypes.StringType, true)));

		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		Dataset<Row> df = sqlContext.createDataFrame(rowsRDD, schema);
		df.show();
		df.createOrReplaceTempView("userInfo");

	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlc = new SQLContext(sc);
		// 生成模拟测试数据
		mock(sc, sqlc);
		userInfo(sc, sqlc);
	}

}
