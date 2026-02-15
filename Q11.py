from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import os

spark = SparkSession.builder.appName("TFIDFSimilarity").getOrCreate()

# Load books (one row per file)
rdd = spark.sparkContext.wholeTextFiles(
    "/mnt/c/Users/shubh/Downloads/D184MB/D184MB/*.txt"
)

books_df = rdd.toDF(["file_path", "text"])

# Extract file name
def get_filename(path):
    return os.path.basename(path)

get_filename_udf = udf(get_filename, StringType())

books_df = books_df.withColumn("file_name", get_filename_udf("file_path"))
books_df = books_df.select("file_name", "text")

# Clean text
books_df = books_df.withColumn(
    "clean_text",
    lower(regexp_replace(col("text"), "[^a-zA-Z ]", ""))
)

# Tokenize
tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
words_df = tokenizer.transform(books_df)

# Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_df = remover.transform(words_df)

# Compute TF
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
featurized_df = hashingTF.transform(filtered_df)

# Compute IDF
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurized_df)
tfidf_df = idfModel.transform(featurized_df)

print("TF-IDF Computation Completed")
tfidf_df.select("file_name", "features").show(5, truncate=False)

spark.stop()
