import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Creating the spark session
spark = SparkSession\
    .builder\
    .master('local[4]')\
    .appName('quake_etl')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

# Load the dataset
df_load = spark.read.csv(r'C:\1_My_things\1. Sem 7\BDAD\database.csv', header=True)

# Remove all fields we don't need
lst_dropped_columns = [
    'Depth Error',
    'Time',
    'Depth Seismic Stations',
    'Magnitude Error',
    'Magnitude Seismic Stations',
    'Azimuthal Gap',
    'Horizontal Distance',
    'Horizontal Error',
    'Root Mean Square',
    'Source',
    'Location Source',
    'Magnitude Source',
    'Status'
]

# Drop all the columns listed above and assign new dataframe to df_drop
df_load = df_load.drop(*lst_dropped_columns)

# Creating a year field and add it to the df_load dataframe
df_load = df_load.withColumn('Year', year(to_timestamp('Date', 'dd/MM/yyyy')))

# Creating the quakes freq dataframe from the year and count values
df_quake_freq = df_load.groupBy('Year').count().withColumnRenamed('count', 'Count')

# cast some fielts from string into numeric types
df_load = df_load.withColumn('Latitude', df_load['Latitude'].cast(DoubleType()))\
    .withColumn('Longitude', df_load['Longitude'].cast(DoubleType()))\
    .withColumn('Depth', df_load['Depth'].cast(DoubleType()))\
    .withColumn('Magnitude', df_load['Magnitude'].cast(DoubleType()))

# Create avg and max magnitude fields and add to df_quake_freq
# creating avg magnitude and max magnitude fields and add to df_quake_feq
df_max = df_load.groupBy('Year').max('Magnitude').withColumnRenamed('max(Magnitude)', 'Max_Magnitude')
df_avg = df_load.groupBy('Year').avg('Magnitude').withColumnRenamed('avg(Magnitude)', 'Avg_Magnitude')

# joining df_max and df_avg to main df_quake_freq
df_quake_freq = df_quake_freq.join(df_avg, ['Year']).join(df_max, ['Year'])

# Removing null values
df_load.dropna()
df_quake_freq.dropna()

# building collection in mongodb
# Write df_load to mongodb
df_load.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.quakes').save()


# write df_quake_freq to mongodb
df_quake_freq.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.quake_freq').save()

# Printing dataframe heads
print(df_quake_freq.show(5))
print(df_load.show(5))

print('INFO: Job ran successfully')
print('')