from pyspark.ml import image
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName('Images')
         .getOrCreate())


image_dir = "C:/users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/cctvVideos/train_images"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

images_df.select('image.height', 'image.width', 'image.nChannels', 'image.mode',
                 'label').show(5, truncate=False)

path = "C:/users/borja.martin/Desktop/LearningSparkV2-master/databricks-datasets/learning-spark-v2/cctvVideos/train_images"

binary_files_df = (spark.read.format('binaryFile')
                   .option('pathGlobFiter', '*.jpg')
                    .option('recusiveFileLookup', 'true')
                   .load(path))
binary_files_df.show(5)