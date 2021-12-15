# Creating SQL DataBases and Tables
from pyspark.sql import SparkSession

# Create a SparkSession

spark = (SparkSession
         .builder
         .appName('BasicQuery')
         .getOrCreate())

# Path to DataSet

csv_file = 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'

# Read and create a temporary view
# Inferschema (note that for larger files you may want to specify the schema

df = (spark.read.format('csv')
      .option('inferSchema', 'true')
      .option('header', 'true')
      .load(csv_file))

spark.sql('CREATE DATABASE learn_spark_db')
spark.sql('USE learn_spark_db')

spark.sql("""CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING,
destination STRING)""")
# In Python
df_sfo = spark.sql("""SELECT date, delay, origin, destination FROM 
 us_delay_flights_tbl WHERE origin = 'SFO'""")
df_jfk = spark.sql("""SELECT date, delay, origin, destination FROM 
 us_delay_flights_tbl WHERE origin = 'JFK'""")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")