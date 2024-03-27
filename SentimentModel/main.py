import math
import os

import pandas as pd
import numpy as np
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import *

import findspark
findspark.init()

sc = SparkSession.builder.getOrCreate()
sqlContext = SQLContext(sc)
customSchema = StructType([
    StructField("Sentence", StringType()),
    StructField("Emotion", StringType())])


#modi_data.csv file contains 10000 tweets with seach query modi C:/Truc/sentiment/Sentiment/data/test_nor_811.csv
filename1 = 'data/train_nor_811.csv'
filename2 = 'data/test_nor_811.csv'
filename3 = 'data/valid_nor_811.csv'
# filename1 = 'C:/Truc/sentiment/Sentiment-Analysis-using-Pyspark-on-Multi-Social-Media-Data/redt_dataset.csv'
# filename2 = 'C:/Truc/sentiment/Sentiment-Analysis-using-Pyspark-on-Multi-Social-Media-Data/twtr_dataset.csv'
df1 = sqlContext.read.format("csv").option("header", "true").option("encoding", "UTF-8").schema(customSchema).load(filename1)

df1.count()

df2 = sqlContext.read.format("csv").option("header", "true").option("encoding", "UTF-8").schema(customSchema).load(filename2)
df2.count()

df3 = sqlContext.read.format("csv").option("header", "true").option("encoding", "UTF-8").schema(customSchema).load(filename3)
df3.count()


df = df1.union(df2)#, emp_acc_LoadCsvDF("acc_id").equalTo(emp_info_LoadCsvDF("info_id")), "inner").selectExpr("acc_id", "name", "salary", "dept_id", "phone", "address", "email")
df.count()

data = df
data.show(5)
data.printSchema()
print("type: ",type(data))
from pyspark.sql.functions import col
print("cout emotion")
data.groupBy("Emotion").count().orderBy(col("count").desc()).show()

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression

# regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="Sentence", outputCol="words", pattern="\\s")

# stop words
add_stopwords = []
with open('data/vietnamese-stopwords.txt', 'r',encoding='UTF-8') as f:
    datalist = f.readlines()
    add_stopwords.append(datalist[1])
stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)

# bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=30000, minDF=5)

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

label_stringIdx = StringIndexer(inputCol = "Emotion", outputCol = "label")
# label_stringIdx = df.withColumn("Label", when(col("Label") == "C", -1).otherwise(col("Label")))

# TF-IDF
from pyspark.ml.feature import HashingTF, IDF

hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=8000)
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=2) #minDocFreq: remove sparse terms
pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, hashingTF, idf, label_stringIdx])
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)

(trainingData, testData) = dataset.randomSplit([0.8, 0.2], seed = 100)
lr = LogisticRegression(maxIter=300, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData)

predictions = lrModel.transform(testData)

predictions.filter(predictions['prediction'] != 0) \
    .select("Sentence","Emotion","probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
# nbAccuracy = evaluator.evaluate(predictions)
print(accuracy)

from pyspark.sql.functions import lit
def predic(df4):
    print("Start predict")
    df4_1 = df4.withColumn("Emotion", lit(0))
    dt= df4_1
    dt.show(5)
    print("type dt", type(dt))
    dtset = pipelineFit.transform(dt)
    dtset.show(5)

    # model = LogisticRegression.load("C:/Truc/sentiment/Sentiment/lr_model")
    pred = lrModel.transform(dtset)
    out = pred.select("uid", "prediction", "Sentence","prediction")
    print("OUTPUT ==============================")
    print(out.show(5))
    return out

from kafka import KafkaConsumer, KafkaProducer
import threading

topic_name = "quickstart"
consumere = KafkaConsumer(topic_name)
# Initialize Kafka producer
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         bootstrap_servers='localhost:9092')

import json 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import functions as F

# Define schema for DataFrame
schema_receive = StructType([
    StructField("UserID", StringType(), True),
    StructField("Sentence", StringType(), True)
])

def ProcessArrayComment(data_byte):
    store_array = []
    # Decode byte message to string
    decoded_message_str = data_byte.decode("utf-8")

    # Convert string to JSON
    decoded_message_json = json.loads(decoded_message_str)

    # Extract "comments" field
    comments_array = decoded_message_json['comments']

    for comment in comments_array:
        store_array.append(comment)
        
    return store_array

def ConvertArrayToDF(store_array):
    # Create an RDD from the array
    rdd = sc.sparkContext.parallelize(store_array)

    # Create DataFrame from RDD
    df = sc.createDataFrame(rdd, schema_receive)
    return df
    
# Assuming `pipelineFit` is pipeline model fitted on training data
# and `model` is trained model

def prepare_data(df):
    """
    Prepare the data using the same feature engineering steps used during training.
    """
    # Add a unique identifier
    df = df.withColumn("uid", F.monotonically_increasing_id())
    
    # Transform the DataFrame using the pipeline
    transformed_df = pipelineFit.transform(df)
    
    return transformed_df

def predict_labels(df, model):
    """
    Use the trained model to make predictions.
    """
    predictions = model.transform(df)
    
    # Select required columns along with the unique identifier
    return predictions.select("uid", "prediction")

    
try:
    for mes in consumere:
        store = mes.value
        array = ProcessArrayComment(store)
        new_df = ConvertArrayToDF(array)
        new_df.show(10)
        # Add a unique identifier for joining later
        new_df = new_df.withColumn("uid", F.monotonically_increasing_id())     
          
        # Prepare the new DataFrame
        #prepared_df = prepare_data(new_df)
        
        # Make predictions
        prediction_df = predic(new_df)
        
        # Join the prediction DataFrame with the original DataFrame based on the unique identifier
        final_df = new_df.join(prediction_df, "uid").drop("uid")
        
        # Show or store the results
        final_df.show()
        
        # Convert the final DataFrame to JSON string
        final_json = final_df.toJSON().collect()
        message = {
        'comments' : final_json
        }    
        # Produce the JSON string to the "Dashboard" topic
        producer.send('Dashboard',value=message)
        print("Send data")
        producer.flush()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    sc.stop()
    producer.close()