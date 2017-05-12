// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val FilteredData = spark.sql("select business_id, state, categories from business_table where latitude<49.384472 AND latitude>24.520833 AND longitude<-66.950 AND longitude>-124.766667 and state <> 'QC' and state <> 'ON'")

val flattened1 = FilteredData.select($"business_id", $"state", explode($"categories").as("cat_flat"))

flattened1.createOrReplaceTempView("flattened1_table")

val businessRestaurants = spark.sql("Select * from flattened1_table where cat_flat = 'Restaurants'")

businessRestaurants.createOrReplaceTempView("business_Restaurants")

//checkin file
var df = spark.read.json("/FileStore/tables/opxair3p1493746853047/yelp_academic_dataset_checkin.json")

val flatten_checkin = df.select($"business_id", explode($"time").as("time_flatten"))

val res = flatten_checkin.select($"business_id", expr("(split(time_flatten,'-'))[1]").cast("string").as("time_hours"))

val res1 = res.select($"business_id", expr("(split(time_hours,':'))[0]").cast("integer").as("time_interval"), expr("(split(time_hours,':'))[1]").cast("integer").as("total_checkin"))

res1.createOrReplaceTempView("checkin_result")


val JoinedData = spark.sql("select business_Restaurants.business_id,  checkin_result.time_interval, checkin_result.total_checkin from business_Restaurants,checkin_result where business_Restaurants.business_id = checkin_result.business_id")

JoinedData.createOrReplaceTempView("final_table")

val groupedTable = spark.sql("select time_interval, SUM(total_checkin) from final_table group by time_interval order by time_interval asc")

display(groupedTable)


// COMMAND ----------

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val FilteredData = spark.sql("select business_id, state, categories from business_table where latitude>49.309008 and latitude<59.342994 AND longitude>-10.942383 and longitude<2.241211")

val flattened1 = FilteredData.select($"business_id", $"state", explode($"categories").as("cat_flat"))

flattened1.createOrReplaceTempView("flattened1_table")

val businessRestaurants = spark.sql("Select * from flattened1_table where cat_flat = 'Restaurants'")

businessRestaurants.createOrReplaceTempView("business_Restaurants")

//checkin file
var df = spark.read.json("/FileStore/tables/opxair3p1493746853047/yelp_academic_dataset_checkin.json")

val flatten_checkin = df.select($"business_id", explode($"time").as("time_flatten"))

val res = flatten_checkin.select($"business_id", expr("(split(time_flatten,'-'))[1]").cast("string").as("time_hours"))

val res1 = res.select($"business_id", expr("(split(time_hours,':'))[0]").cast("integer").as("time_interval"), expr("(split(time_hours,':'))[1]").cast("integer").as("total_checkin"))

res1.createOrReplaceTempView("checkin_result")


val JoinedData = spark.sql("select business_Restaurants.business_id,  checkin_result.time_interval, checkin_result.total_checkin from business_Restaurants,checkin_result where business_Restaurants.business_id = checkin_result.business_id")

JoinedData.createOrReplaceTempView("final_table")

val groupedTable = spark.sql("select time_interval, SUM(total_checkin) from final_table group by time_interval order by time_interval asc")

display(groupedTable)

// COMMAND ----------

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val FilteredData = spark.sql("select business_id, state, categories from business_table where  latitude>47.181313 and latitude<54.232327 AND longitude>6.130371 and longitude<14.831543")

val flattened1 = FilteredData.select($"business_id", $"state", explode($"categories").as("cat_flat"))

flattened1.createOrReplaceTempView("flattened1_table")

val businessRestaurants = spark.sql("Select * from flattened1_table where cat_flat = 'Restaurants'")

businessRestaurants.createOrReplaceTempView("business_Restaurants")

//checkin file
var df = spark.read.json("/FileStore/tables/opxair3p1493746853047/yelp_academic_dataset_checkin.json")

val flatten_checkin = df.select($"business_id", explode($"time").as("time_flatten"))

val res = flatten_checkin.select($"business_id", expr("(split(time_flatten,'-'))[1]").cast("string").as("time_hours"))

val res1 = res.select($"business_id", expr("(split(time_hours,':'))[0]").cast("integer").as("time_interval"), expr("(split(time_hours,':'))[1]").cast("integer").as("total_checkin"))

res1.createOrReplaceTempView("checkin_result")


val JoinedData = spark.sql("select business_Restaurants.business_id,  checkin_result.time_interval, checkin_result.total_checkin from business_Restaurants,checkin_result where business_Restaurants.business_id = checkin_result.business_id")

JoinedData.createOrReplaceTempView("final_table")

val groupedTable = spark.sql("select time_interval, SUM(total_checkin) from final_table group by time_interval order by time_interval asc")

display(groupedTable)

// COMMAND ----------


