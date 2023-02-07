// Databricks notebook source
//Imports
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

//Importing data set
val df = spark.read.option("header", "true")
                    .option("inferSchema", "true")
                     .csv("/FileStore/shared_uploads/ahmed.say@northeastern.edu/train.csv")
df.show(5)

// COMMAND ----------

//1)What is the average ticket fare for each Ticket class?
df.groupBy("Pclass").agg(avg("Fare")).show()

// COMMAND ----------

//What is the survival percentage for each Ticket class? Which class has the highest survival rate?
val totalDF = df.count
val survivalDF = df.filter($"survived" === 1).groupBy("pclass").count
val survivalPercentDF = survivalDF.withColumn("percentage", col("count")/totalDF *100)
survivalPercentDF.show()
val highestSurvival = survivalPercentDF.agg(max("percentage")).first().getDouble(0)
val highestSurvivalClass = survivalPercentDF.select($"pClass").filter($"percentage" === highestSurvival).first().getInt(0)
println(s"The class with the highest survival rate is: $highestSurvivalClass")

// COMMAND ----------

//Rose DeWitt Bukater was 17 years old when she boarded the titanic. She is traveling with her mother and fiance( they are not married yet, so they are not related). She is traveling first class. With the information of her age, gender, class she is traveling in, and the fact that she is traveling with one parent, find the number of passengers who could possibly be Rose.
val possiblyRose = df.filter(df("Age") === 17 && df("Sex") === "female").filter(col("pClass") === 1).filter(col("parch") === 1).filter(col("SibSp") === 0).count()
println(s"The number of passengers who could possibly be Rose: $possiblyRose")

// COMMAND ----------

//Jack Dawson born in 1892 died on April 15, 1912. He is either 20 or 19 years old. He travels 3rd class and has no relatives onboard. Find the number of passengers who could possibly be Jack? 
val jackDF = df.filter(col("pclass") === 3).filter(col("Sex") === "male").filter((col("age") === 19)||(col("age") === 20)).filter(col("parch") === 0).filter(col("SibSp") === 0)
jackDF.show(10)
val jackPossible  = jackDF.count


// COMMAND ----------

// Split the age into age groups of 10 years
val ageGroupDF = df.withColumn("ageGroup", when(col("age") <= 10, "0-10").when(col("age") <= 20, "11-20")
                               .when(col("age") <= 30, "21-30").when(col("age") <= 40, "31-40")
                               .when(col("age") <= 50, "41-50").when(col("age") <= 60, "51-60").when(col("age") <= 70, "61-70").otherwise("70+"))
ageGroupDF.show(10)

// COMMAND ----------

// Calculating the average fare for each age group
val avgFareByAge = ageGroupDF.groupBy("ageGroup").agg(avg("fare"))
avgFareByAge.show()

// COMMAND ----------

// Calculating the age group most likely survived
val maxSurvivedAgeGroup = ageGroupDF.groupBy("ageGroup").agg(avg("survived").as("Average"))
                                    .sort(col("Average").desc).first().getString(0)
println(s"The age group with the highest survival rate is: $maxSurvivedAgeGroup")
