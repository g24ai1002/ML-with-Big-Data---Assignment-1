from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import os

spark = SparkSession.builder.appName("BookMetadata").getOrCreate()

# Load each book as one row
rdd = spark.sparkContext.wholeTextFiles(
    "/mnt/c/Users/shubh/Downloads/D184MB/D184MB/*.txt"
)

books_df = rdd.toDF(["file_path", "text"])

# Extract file name only
def get_filename(path):
    return os.path.basename(path)

get_filename_udf = udf(get_filename, StringType())

books_df = books_df.withColumn("file_name", get_filename_udf("file_path"))

books_df = books_df.select("file_name", "text")

books_df.printSchema()
books_df.show(5, truncate=False)

spark.stop()
