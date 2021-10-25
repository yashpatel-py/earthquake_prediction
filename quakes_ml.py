import pyspark
from pyspark.ml.linalg import Matrix
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import Evaluator, RegressionEvaluator
import numpy as np

# Creating pyspark session
spark = SparkSession\
    .builder\
    .master("local[2]")\
    .appName("quakes_ml")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

"""
Data Preprocessing
"""

# Loading test data file
df_test = spark.read.csv("./query.csv", header=True)

# Loading quakes data from mongodb
df_train = spark.read.format("mongo")\
    .option('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/Quake.quakes').load()

# Select fields we are going to use from df_test
df_test_clean = df_test['time', 'latitude', 'longitude', 'depth', 'mag']

# Rename fields in df_test_clean
df_test_clean = df_test_clean.withColumnRenamed('time', 'Date')\
    .withColumnRenamed('latitude', 'Latitude')\
    .withColumnRenamed('longitude', 'Longitude')\
    .withColumnRenamed('depth', 'Depth')\
    .withColumnRenamed('mag', 'Magnitude')

# Cast string fields to double
df_test_clean = df_test_clean.withColumn('Latitude', df_test_clean['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_test_clean['Longitude'].cast(DoubleType()))\
    .withColumn('Depth', df_test_clean['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df_test_clean['Magnitude'].cast(DoubleType()))

# Create training and testing dataframes
df_testing = df_test_clean.select('Latitude', 'Longitude', 'Depth', 'Magnitude')
df_training = df_train.select('Latitude', 'Longitude', 'Depth', 'Magnitude')

# removing null values from our datasets
df_training = df_training.dropna()
df_testing = df_testing.dropna()

"""
Building the machine learning model
"""

# Create feature vector
assembler = VectorAssembler(inputCols=['Latitude', 'Longitude', 'Depth'], outputCol='features')

# Create the model
# We will predict magnitrude value
model_reg = RandomForestRegressor(featuresCol='features', labelCol="Magnitude")

# Chain assembler and model into a pipeline
pipeline = Pipeline(stages=[assembler, model_reg])

# Training the model
model = pipeline.fit(df_training)

# making the prediction
pred_results = model.transform(df_testing)

# Evaluating the model
evaluator = RegressionEvaluator(labelCol="Magnitude", predictionCol="prediction", metricName='rmse')
rmse = evaluator.evaluate(pred_results)

"""
Create the predction dataset
"""
df_pred_results = pred_results["Latitude", "Longitude", "prediction"]

# Renaming the prediction field to pred_Magnitude
df_pred_results = df_pred_results.withColumnRenamed('prediction', 'Pred_Magnitude')

# adding more colimns
df_pred_results = df_pred_results.withColumn('Year', lit(2017))\
    .withColumn('RMSE', lit(rmse))

# Loading the prection data into Mongodb
df_pred_results.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.pred_results').save()

print(df_pred_results.show(5))
print("SUCESS: Job is done successfully")
print("")