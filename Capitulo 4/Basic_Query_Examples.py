from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ =='__main__':
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

    df.createOrReplaceTempView('us_delay_flights_tbl')

    spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)

    spark.sql("""SELECT date, delay, origin, destination
    FROM us_Delay_flights_tbl
    WHERE delay > 120 AND origin = 'SFO' and destination = 'ORD'
    ORDER BY delay DESC""").show(10)

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


# Usando API con los comandos SQL anteriores

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