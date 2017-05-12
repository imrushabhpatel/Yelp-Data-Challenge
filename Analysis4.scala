// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val MaxReviews = spark.sql("select state, sum(review_count) as total_reviews from business_table where latitude<49.384472 AND latitude>24.520833 AND longitude<-66.950 AND longitude>-124.766667 and state != 'ON' and state != 'QC' group by state order by total_reviews desc limit 3")

MaxReviews.createOrReplaceTempView("MaxReviews_Table")

val topbusiness =  spark.sql("select business_table.business_id, business_table.name, business_table.state, business_table.review_count from business_table, MaxReviews_Table where MaxReviews_Table.state = business_table.state")

topbusiness.createOrReplaceTempView("topbusiness_table")

val top_business =  spark.sql("SELECT * FROM (SELECT business_id, name, state, review_count, dense_rank() OVER (PARTITION BY state ORDER BY review_count DESC) as rank FROM topbusiness_table) tmp WHERE rank <= 3")

display(top_business)

// COMMAND ----------


