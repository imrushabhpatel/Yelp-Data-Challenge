// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val FilteredData = spark.sql("select business_id, city, categories from business_table where city = 'Las Vegas' OR city = 'Phoenix'")

val flattened1 = FilteredData.select($"business_id", $"city", explode($"categories").as("cat_flat"))

flattened1.createOrReplaceTempView("flattened_business_table")


var checkin = spark.read.json("/FileStore/tables/opxair3p1493746853047/yelp_academic_dataset_checkin.json")

val flatten_checkin = checkin.select($"business_id", explode($"time").as("time_flatten"))

val res = flatten_checkin.select($"business_id", expr("(split(time_flatten,'-'))[1]").cast("string").as("time_hours"))

val checkin_split = res.select($"business_id", expr("(split(time_hours,':'))[0]").cast("integer").as("time_interval"), expr("(split(time_hours,':'))[1]").cast("integer").as("total_checkin"))

checkin_split.createOrReplaceTempView("checkin_split_table")

val JoinedData = spark.sql("select flattened_business_table.city, flattened_business_table.cat_flat,  checkin_split_table.time_interval, sum(checkin_split_table.total_checkin) as total_user from flattened_business_table, checkin_split_table where flattened_business_table.business_id = checkin_split_table.business_id and cat_flat = 'Restaurants' group by flattened_business_table.city, flattened_business_table.cat_flat, checkin_split_table.time_interval")

JoinedData.createOrReplaceTempView("JoinedData_table")

val result = spark.sql("select city, time_interval, total_user from JoinedData_table order by time_interval")
 
display(result)

// COMMAND ----------

display(result)


// COMMAND ----------


