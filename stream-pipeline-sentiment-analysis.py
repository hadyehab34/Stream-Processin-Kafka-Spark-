from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os
from textblob import TextBlob
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Set up Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("tweet", StringType(),True)
])

# Define the Kafka topic to consume from
topic_name = "doge_tweets"

# Define the Kafka broker URL
bootstrap_servers = "localhost:9092"

# Read data from Kafka topic using Structured Streaming
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Clean the data
import re
from pyspark.sql.functions import udf # user-defined-function
from pyspark.sql.types import StringType

def clean_tweet(tweet):
    """Cleans the tweet text by removing links, users, hashtags, and punctuation"""
    tweet = re.sub(r'http\S+', '', str(tweet)) # removes URLs
    tweet = re.sub(r'@[A-Za-z0-9_]+', '', str(tweet)) # removes Twitter usernames (which start with @) 
    tweet = re.sub(r'#', '', str(tweet)) # removes hashtags
    tweet = re.sub(r'[^\w\s]', '', str(tweet)) # removes any non-alphanumeric or whitespace characters 
    tweet = re.sub(r"\n",'',str(tweet))# removes \n or \r  newline characters
    tweet = tweet.strip()
    return tweet

udf_clean_tweet = udf(clean_tweet, StringType())
df_cleaned = df.select(udf_clean_tweet(col("tweet")).alias("tweet"))
raw_tweets = df_cleaned.withColumn('processed_text', df_cleaned["tweet"])

# Start the Spark Streaming query
# query = (
#     df_cleaned.writeStream.format("console")
#     .option("truncate", "false")
#     .outputMode("append")
#     .start()
# )
# Wait for the query to finish
# query.awaitTermination()

# Load the pre-trained sentiment analysis model
from pyspark.ml import PipelineModel
from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sentiment_analysis").getOrCreate()

# Load the pre-trained sentiment analysis model
model = PipelineModel.load("sentiment_analysis_model")

# Tokenize the cleaned tweets
tokenizer = Tokenizer(inputCol="tweet", outputCol="tokens")
tokenized_tweets = tokenizer.transform(df_cleaned)
# tokenized_tweets.show()
# Apply the sentiment analysis model to the tokenized tweets
sentiment_scores = model.transform(tokenized_tweets.select("tweet", "tokens"))

# Select the relevant columns and display the results
sentiment_results = sentiment_scores.select("tweet", "prediction")

# Write the results to a CSV file
#supports these formats : csv, json, orc, parquet
query = sentiment_results.writeStream \
    .format("csv") \
    .option("checkpointLocation", "data/") \
    .option("path", "csv/") \
    .outputMode("append") \
    .start()
    

query.awaitTermination(10)
query.stop()

query2 = sentiment_results.writeStream \
            .format("console") \
            .outputMode("append") \
            .start()

# Wait for the query to terminate
query2.awaitTermination()



# from pymongo import MongoClient

# client = MongoClient()
# db = client["mydb"]
# collection = db["sentiment_results"]
# # Convert the DataFrame to a list of dictionaries

# # Define the foreachBatch function to write to MongoDB
# def write_to_mongodb(batch_df, batch_id):
#     # Convert the DataFrame to a list of dictionaries
#     documents = batch_df.select("tweet", "prediction").toPandas().to_dict('records')
#     # Write the documents to MongoDB
#     collection.insert_many(documents)

# # Write streaming data to MongoDB
# query = sentiment_results \
#     .writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_to_mongodb) \
#     .start()

# # Wait for the query to terminate
# query.awaitTermination()

# # Close the MongoDB connection
# client.close()