// Databricks notebook source
//Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// COMMAND ----------

val spark = SparkSession.builder().appName("TitanicEDA").getOrCreate()
val trainData = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/shared_uploads/ahmed.say@northeastern.edu/train.csv")
val testData = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/shared_uploads/ahmed.say@northeastern.edu/test-2.csv")

// COMMAND ----------

// DBTITLE 1,Exploratory Data Analysis


// COMMAND ----------

// view the schema of the DataFrame
trainData.printSchema()

// COMMAND ----------

// get summary statistics for numerical columns
trainData.describe().show()

// COMMAND ----------

// check for missing values
val null_counts = trainData.select(trainData.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*)
null_counts.show()

// COMMAND ----------

// check the number of distinct values in each column
trainData.select(trainData.columns.map(c => countDistinct(col(c)).alias(c)): _*).show()

// COMMAND ----------

// Count the number of passengers in each category 
trainData.groupBy("sex").count().show()
trainData.groupBy("pclass").count().show()
trainData.groupBy("embarked").count().show()

// COMMAND ----------

// Create a pivot table to show the survival rate by sex and class
val pivotDF = trainData.filter(col("survived").isNotNull).groupBy("sex").pivot("pclass").agg(avg("survived"))
pivotDF.show()

// COMMAND ----------

// DBTITLE 1,Feature Engineering


// COMMAND ----------

// Drop irrelevant columns
val trainData2 = trainData.drop("PassengerId", "Ticket", "Name", "Cabin")
val testData2 = testData.drop("PassengerId", "Ticket", "Name", "Cabin")

// Create a new feature "FamilySize"
val trainData3 = trainData2.withColumn("FamilySize", col("SibSp") + col("Parch") + 1)
val testData3 = testData2.withColumn("FamilySize", col("SibSp") + col("Parch") + 1)

// Create a new feature "IsAlone"
val trainData4 = trainData3.withColumn("IsAlone", when(col("FamilySize") === 1, 1).otherwise(0))
val testData4 = testData3.withColumn("IsAlone", when(col("FamilySize") === 1, 1).otherwise(0))

// COMMAND ----------

trainData4.show(5)
testData4.show(5)

// COMMAND ----------

// DBTITLE 1,Prediction


// COMMAND ----------

// Convert the string columns to numerical indices
val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex").setHandleInvalid("skip")
val embarkedIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkedIndex").setHandleInvalid("skip")

// Convert the numerical indices to binary vectors
val sexEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec")
val embarkedEncoder = new OneHotEncoder().setInputCol("EmbarkedIndex").setOutputCol("EmbarkedVec")

// Vectorize the features
val assembler = new VectorAssembler()
  .setInputCols(Array("Pclass", "SexVec", "Age", "SibSp", "Parch", "Fare", "EmbarkedVec", "FamilySize", "IsAlone"))
  .setOutputCol("features")
  .setHandleInvalid("skip")

// COMMAND ----------

// Create a pipeline
val pipeline = new Pipeline().setStages(Array(sexIndexer, embarkedIndexer, sexEncoder, embarkedEncoder, assembler))

// Fit the pipeline to the training data
val pipelineModel = pipeline.fit(trainData4)

// COMMAND ----------

// Transform the training and test data
val trainDataFinal = pipelineModel.transform(trainData4).select("features", "Survived")
val testDataFinal = pipelineModel.transform(testData4).select("features")

// Split the data into training and testing sets
val Array(trainingData, testingData) = trainDataFinal.randomSplit(Array(0.7, 0.3))

// Create the Random Forest Classifier model
val rf = new RandomForestClassifier().setLabelCol("Survived").setFeaturesCol("features")

// Train the model
val model = rf.fit(trainingData)

// Make predictions on the testing set
val predictions = model.transform(testingData)

// Evaluate the model using MulticlassClassificationEvaluator
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Survived").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)

// COMMAND ----------

println(s"Accuracy: ${accuracy*100}%")

// COMMAND ----------

// Make predictions on the test data and save the results
val finalPredictions = model.transform(testDataFinal).select("prediction").withColumnRenamed("prediction", "Survived")
finalPredictions.write.option("header", "true").csv("/FileStore/shared_uploads/ahmed.say@northeastern.edu/titanic_predictions1.csv")
