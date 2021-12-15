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

# Create a managed table
spark.sql("""CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING,
destination STRING)""")

# API DataFrame
# schema as defined in the preceding example
# schema = 'date STRING, delay INT, distance INT, origin STRING, destination STRING'
# flights_df = spark.read.csv(csv_file, schema = schema)
# flights_df.write.saveAsTable('managed_us_delay_flights_tbl)

# Create an unmanaged table
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
distance INT, origin STRING, destination STRING)
USING csv OPTIONS (PATH 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'""")

# API DataFrame
#(flights_df
#    .write
#    .option("path", "/tmp/data/us_flights_delay")
#    .saveAsTable('us_delay_flights_tbl'))

# In Python
df_sfo = spark.sql("""SELECT date, delay, origin, destination FROM 
 us_delay_flights_tbl WHERE origin = 'SFO'""")
df_jfk = spark.sql("""SELECT date, delay, origin, destination FROM 
 us_delay_flights_tbl WHERE origin = 'JFK'""")
# Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")
