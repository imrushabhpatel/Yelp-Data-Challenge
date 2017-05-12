// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val FilteredData = spark.sql("select business_id, city, state, categories from business_table where city ='Las Vegas' or city = 'Madison' or city = 'Phoenix' or city = 'Charlotte' or city = 'Pittsburgh' or city = 'Cleveland'")

val flattened1 = FilteredData.select($"business_id", $"city", $"state", explode($"categories").as("cat_flat"))

flattened1.createOrReplaceTempView("flattened1_table")

val businessRestaurants = spark.sql("Select * from flattened1_table where cat_flat = 'Restaurants'")

businessRestaurants.createOrReplaceTempView("business_Restaurants")

//checkin file
var df = spark.read.json("/FileStore/tables/opxair3p1493746853047/yelp_academic_dataset_checkin.json")

val flatten_checkin = df.select($"business_id", explode($"time").as("time_flatten"))

val res = flatten_checkin.select($"business_id", expr("(split(time_flatten,'-'))[1]").cast("string").as("time_hours"))

val res1 = res.select($"business_id", expr("(split(time_hours,':'))[0]").cast("integer").as("time_interval"), expr("(split(time_hours,':'))[1]").cast("integer").as("total_checkin"))

res1.createOrReplaceTempView("checkin_result")


val JoinedData = spark.sql("select business_Restaurants.city , sum(checkin_result.total_checkin) as totalcheckin from business_Restaurants,checkin_result where business_Restaurants.business_id = checkin_result.business_id group by business_Restaurants.city")

display(JoinedData)

JoinedData.createOrReplaceTempView("businessfinal_table")


var income = spark.read.csv("/FileStore/tables/jgfdj90m1494188664808/income.csv")

income.createOrReplaceTempView("income_table")

val JoinedData1 = spark.sql("Select _c0 as city, _c1 as population, _c5 as income from income_table where _c0 ='Las Vegas' or _c0 = 'Madison' or _c0 = 'Phoenix' or _c0 = 'Charlotte' or _c0 = 'Pittsburgh' or _c0 = 'Cleveland'")

JoinedData1.createOrReplaceTempView("incomefinal_table")


// COMMAND ----------

val result = spark.sql("select businessfinal_table.city, incomefinal_table.population, incomefinal_table.income, businessfinal_table.totalcheckin from businessfinal_table,incomefinal_table where businessfinal_table.city = incomefinal_table.city order by businessfinal_table.totalcheckin")

display(result)





// COMMAND ----------

display(result)


// COMMAND ----------


