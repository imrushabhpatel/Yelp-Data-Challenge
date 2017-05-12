// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val counting = spark.sql("select state, count(business_id) as no_of_business, sum(review_count) as total_review, avg(stars) as avg_stars from business_table where latitude<49.384472 AND latitude>24.520833 AND longitude<-66.950 AND longitude>-124.766667 and is_open = 1 group by state order by avg_stars desc")

display(counting)

// COMMAND ----------

display(counting)

// COMMAND ----------


