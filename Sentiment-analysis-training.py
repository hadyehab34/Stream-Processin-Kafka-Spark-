from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("model").getOrCreate()

# Load the data from a DataFrame with columns "tweet" and "label"
data = spark.read.format("csv").option("header", "true").load("sentiment_tweets_data123.csv")
data.show()
# data = data.dropna(subset=["label"])
# Convert the "label" column to a numeric type
data = data.withColumn("label", data["label"].cast("double"))
data = data.dropna(subset=["label"])


# Split the data into training and test sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# Define the pipeline stages
tokenizer = Tokenizer(inputCol="tweet", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Train the model on the training data
model = pipeline.fit(train_data)

# Evaluate the model on the test data
predictions = model.transform(test_data)
accuracy = predictions.filter(col("label") == col("prediction")).count() / float(test_data.count())

# Print the accuracy of the model
print(f"Accuracy: {accuracy}")

# Save the trained model to disk
model.write().overwrite().save("sentiment_analysis_model")