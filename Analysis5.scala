// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val MainData = spark.sql("select business_id, city, state, postal_code, categories, stars, review_count from business_table")

val flattened = MainData.select($"business_id", $"state", $"postal_code", explode($"categories").as("cat_flat"), $"stars", $"review_count")

flattened.createOrReplaceTempView("flattened_table")

val filter_data = spark.sql("Select * from flattened_table where cat_flat = 'Restaurants'")

filter_data.createOrReplaceTempView("filter_data_table")

// COMMAND ----------

val resultData = spark.sql("Select state, count(business_id) as Total_no_Restaurants from filter_data_table group by state order by state")
display(resultData) 


// COMMAND ----------

val resultData = spark.sql("Select state, avg(stars) as avg_stars from filter_data_table group by state order by state")
display(resultData)


// COMMAND ----------


val resultData = spark.sql("Select state, sum(review_count) as total_reviews from filter_data_table group by state order by state")
display(resultData)

// COMMAND ----------


