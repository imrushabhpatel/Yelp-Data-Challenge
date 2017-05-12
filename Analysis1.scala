// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table1")


val MainData = spark.sql("select business_id, state, categories from business_table1")

val flattened = MainData.select($"business_id", $"state", explode($"categories").as("cat_flat"))

flattened.createOrReplaceTempView("flattened_table")

val filter_data = spark.sql("Select * from flattened_table where cat_flat = 'Restaurants'")

filter_data.createOrReplaceTempView("business_table")




val reviews = spark.read.json("/FileStore/tables/5rgsav7f1494285261654/review.json")

reviews.createOrReplaceTempView("reviews_table")

val JoinedData = spark.sql("select business_table.business_id, business_table.state, business_table.cat_flat, reviews_table.date from business_table,  reviews_table where business_table.business_id = reviews_table.business_id and state = 'AZ'")

val monthtable = JoinedData.select($"business_id", expr("(split(date, '-'))[1]").cast("integer").as("month"))

monthtable.createOrReplaceTempView("months_table")

val counting = spark.sql("select business_id, month, count(*) as chk from months_table group by business_id, month")

counting.createOrReplaceTempView("counting_table")

val counting1 = spark.sql("select business_id, month, chk*100 as visitor  from counting_table")

display(counting1)
counting1.createOrReplaceTempView("business_table1")


// COMMAND ----------

val weather = spark.read.csv("/FileStore/tables/civg65kl1494288156074/KPHX.csv")
weather.createOrReplaceTempView("weather_table")

val FilterWeather = spark.sql("select _c0 as date, _c1 as temp from weather_table where _c0 not like 'date'")

val WeatherMonth = FilterWeather.select(expr("(split(date, '-'))[1]").cast("integer").as("month"),$"temp")

WeatherMonth.createOrReplaceTempView("WeatherMonth_table")

val temperature = spark.sql("select month, avg(temp) as avg_temp from WeatherMonth_table group by month")

temperature.createOrReplaceTempView("temp_table")

val JoinMainData = spark.sql("select business_table1.business_id, business_table1.month, temp_table.avg_temp, business_table1.visitor from business_table1, temp_table where business_table1.month = temp_table.month and business_table1.business_id = 'PBVjimhaGmeMC4sYIEiqQw' order by month asc")

display(JoinMainData)


// COMMAND ----------


