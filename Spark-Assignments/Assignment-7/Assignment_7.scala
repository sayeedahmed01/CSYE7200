// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// schema of the movies dataset
val moviesSchema = StructType(Array(
  StructField("movieId", IntegerType, true),
  StructField("title", StringType, true),
  StructField("genres", StringType, true)))

// schema of the ratings dataset
val ratingsSchema = StructType(Array(
  StructField("userId", IntegerType, true),
  StructField("movieId", IntegerType, true),
  StructField("rating", DoubleType, true),
  StructField("timestamp", LongType, true)))

// COMMAND ----------

// Read the movies dataset into a Spark DataFrame
val movies = spark.read.format("csv")
  .option("header", "true")
  .schema(moviesSchema)
  .load("dbfs:/FileStore/shared_uploads/ahmed.say@northeastern.edu/movies.csv")

// Read the ratings dataset into a Spark DataFrame
val ratings = spark.read.format("csv")
  .option("header", "true")
  .schema(ratingsSchema)
  .load("dbfs:/FileStore/shared_uploads/ahmed.say@northeastern.edu/ratings.csv")

// COMMAND ----------

// Join the movies and ratings datasets on movieId
val movieRatings = ratings.join(movies, Seq("movieId"), "left_outer")

// COMMAND ----------

// Calculate the mean and standard deviation of ratings for each movie
val movieStats = movieRatings.groupBy("movieId", "title")
  .agg(avg("rating").alias("mean_rating"), stddev("rating").alias("stddev_rating"))
  .orderBy("movieId")

// COMMAND ----------

// Show the results
movieStats.show()

// COMMAND ----------

//Test Suit
//Test case 1: Check if the Spark Session is running

// Create a SparTest case 1:k Session for testing
val sparkTest = SparkSession.builder()
  .appName("test")
  .master("local[*]")
  .getOrCreate()

// Verify that the Spark Session is running
assert(sparkTest != null)
assert(sparkTest.version.startsWith("3."))

//Test case 2: Check if the movies DataFrame is loaded correctly.
assert(movies.count() == 9742)

//Test case 3: Check if the ratings DataFrame is loaded correctly.
assert(ratings.count() == 100836)

//Test case 4: Check if the join operation is performed correctly.
assert(movieRatings.count() == 100836)

//Test case 5: Check if the mean and standard deviation of ratings are calculated correctly.

// Select a movie with known mean and standard deviation
val testMovieId = 1
val testMovieStats = movieStats.filter($"movieId" === testMovieId).select("mean_rating", "stddev_rating").head()

// Compare the calculated values with the expected values
assert(Math.abs(testMovieStats.getDouble(0) - 3.9209302325581397) < 0.0001)
assert(Math.abs(testMovieStats.getDouble(1) - 0.8348591407114045) < 0.0001)
