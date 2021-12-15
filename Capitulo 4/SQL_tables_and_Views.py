from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession

spark = (SparkSession
         .builder
         .appName('Capitulo4')
         .getOrCreate())

csv_file = 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'  # Path to data set

# Read and create a temporary view
# Infer schema (note that for larger files you may want to specify the schema)
df = (spark.read.format('csv')
      .option('inferSchema', 'true')
      .option('header', 'true')
      .load(csv_file))

df.createOrReplaceTempView('us_delay_flights_tbl')

# sql
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)

spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)

# Anteriores consultas en API DataFrame

(df.select('distance', 'origin', 'destination')
 .where(col('distance') > 1000)
 .orderBy(desc('distance'))).show(10)

(df.select('date', 'delay', 'origin', 'destination')
 .where(col('delay') > 120)
 .where(col('origin') == 'SFO')
 .where(col('destination') == 'ORD')
 .orderBy(desc('delay'))).show(10)

(df.select('delay', 'origin', 'destination', when(col('delay') > 360, 'Very Long Delays')
           .when(col('delay') > 120, 'Long Delays')
           .when(col('delay') > 60, 'Short Delays')
           .when(col('delay') > 0, 'Tolerable Delay')
           .when(col('delay') == 0, 'No Delays')
           .otherwise('Early').alias('flightsDelays'))).show(10)

# Creating SQL DataBases and Tables
spark.sql('CREATE DATABASE learn_spark_db')
spark.sql('USE learn_spark_db')

# Creating a managed table
# spark.sql('CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)')

# Creating a managed table
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

flights_df = spark.read.csv(csv_file, schema=schema)

flights_df.write.saveAsTable('managed_us_delay_flights_tbl')

# Creating an unmanaged table
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, 
 distance INT, origin STRING, destination STRING) 
 USING csv OPTIONS (PATH 
 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")

# (flights_df
#    .write('path', 'tmp/data/us_flights_delay')
#    .saveAsTable('us_delay_flights_tbl'))

# Creating Views
# Creating a temporary and global temporary view
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

spark.sql('SELECT * FROM us_origin_airport_JFK_tmp_view')
# spark.read.table('us_origin_airport_JFK_tmp_view')

# Soltar una vista como si fuera una tabla
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# Viewing the metadata
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns('us_delay_flights_tbl')

# Reading tables into DataFrames
us_flight_df = spark.sql('SELECT * FROM us_delay_flights_tbl')
us_flights_df2 = spark.table('us_delay_flights_tbl')

# Read Parquet format
df = spark.read.format("parquet").load(
    'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet')

# Reading Parquet files into a Spark SQL Table
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# Save Parquet
(df.write.format('parquet')
 .mode('overwrite')
 .option('compression', 'snappy')
 .save('tmp/data/parquet/df_parquet'))

# Read JSON format
file = 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/json/*'
df = spark.read.format('json').load(file)
# Reading a JSON into a Spark SQL Table
spark.sql('SELECT * FROM us_delay_flights_tbl').show()

# Writing DataFrames to JSON files
(df.write.format("json")
 .mode('overwrite')
 .option('compression', 'snappy')
 .save('/tmp/data/json/df_json'))

# CSV
file = 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data//summary-data/csv/*'

schema = 'DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT'

df = (spark.read.format('csv')
      .option('header', 'true')
      .schema(schema)
      .option('mode', 'FAILFAST')  # Exist if any errors
      .option('nullValue', '')  # Replace any null data field with quotes
      .load(file))

# DataFrame using SQL
spark.sql('SELECT * FROM us_delay_flights_tbl').show(10)

# Writing DataFrames to CSv files
df.write.format('csv').mode('overwrite').save('tmp/data/csv/df_csv')

# AVRO
df = (spark.read.format('avro')
      .load(
    'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/avro/userdata1.avro'))

df.show(truncate=False)

(df.write
 .format('avro')
 .mode('overwrite')
 .save('/tmp/data/avro/df_avro'))

# ORC

file = 'C:/Users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*'

df = spark.read.format('orc').option('path', file).load()


