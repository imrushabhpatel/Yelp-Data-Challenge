// Databricks notebook source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr

var business = spark.read.json("/FileStore/tables/nlpmwbpi1493755425122/yelp_academic_dataset_business.json")

business.createOrReplaceTempView("business_table")

val reviews = spark.read.json("/FileStore/tables/5rgsav7f1494285261654/review.json")

reviews.createOrReplaceTempView("reviews_table")

val JoinedData = spark.sql("select business_table.business_id, business_table.city, business_table.categories, business_table.attributes, reviews_table.stars from business_table,  reviews_table where business_table.business_id = reviews_table.business_id")

val flattened = JoinedData.select($"business_id", $"city", explode($"categories").as("cat_flat"), $"attributes", $"stars" )

flattened.createOrReplaceTempView("flattened_table")

val resultData = spark.sql("Select * from flattened_table where cat_flat = 'Restaurants'")

val flattened2 = resultData.select($"business_id", $"city", explode($"attributes").as("attribute_flat"), $"stars" )

flattened2.createOrReplaceTempView("flattened_table2")

val resultData2 = spark.sql("Select * from flattened_table2 where attribute_flat LIKE '%Ambience%'")


val result3 = resultData2.select($"business_id", $"city" , expr("(split(attribute_flat, 'Ambience: '))[1]").cast("string").as("Ambience"), $"stars")


val finalresult = result3.select($"business_id", $"city", expr("(split(Ambience, ', '))[0]").cast("string").as("Romantic"), expr("(split(Ambience, ', '))[1]").cast("string").as("Intimate"), expr("(split(Ambience, ', '))[2]").cast("string").as("Classy"), expr("(split(Ambience, ', '))[3]").cast("string").as("Hipster"), expr("(split(Ambience, ', '))[4]").cast("string").as("Touristy"), expr("(split(Ambience, ', '))[5]").cast("string").as("Trendy"), expr("(split(Ambience, ', '))[6]").cast("string").as("Upscale"),  expr("(split(Ambience, ', '))[7]").cast("string").as("casual"),$"stars")

val finalresult1 = finalresult.select($"business_id", $"city" , expr("(split(Romantic, ': '))[1]").cast("string").as("Romantic"), expr("(split(Intimate, ': '))[1]").cast("string").as("Intimate"), expr("(split(Classy, ': '))[1]").cast("string").as("Classy"), expr("(split(Hipster, ': '))[1]").cast("string").as("Hipster"), expr("(split(Touristy, ': '))[1]").cast("string").as("Touristy"), expr("(split(Trendy, ': '))[1]").cast("string").as("Trendy"), expr("(split(Upscale, ': '))[1]").cast("string").as("Upscale"), expr("(split(casual, ': '))[1]").cast("string").as("casual"), $"stars")

finalresult1.createOrReplaceTempView("result_table")

val result2 = spark.sql("Select business_id, city avg(stars) as avg_stars from result_table where Romantic = 'True' and city = 'Las Vegas' group by business_id, city order by avg_stars desc")

display(result2)

// COMMAND ----------

val result2 = spark.sql("Select business_id, city, avg(stars) as avg_stars from result_table where Romantic = 'True' and city = 'Phoenix' group by business_id, city order by avg_stars desc")

display(result2)

// COMMAND ----------


