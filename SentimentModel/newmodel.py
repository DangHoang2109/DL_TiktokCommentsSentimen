import math
import os

import pandas as pd
import numpy as np
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SQLContext
from pyspark import SparkContext

from pyspark.sql.types import *
from pyvi import ViTokenizer

sc =SparkContext()
sqlContext = SQLContext(sc)
customSchema = StructType([
    StructField("Sentence", StringType()),
    StructField("Emotion", StringType())])

#modi_data.csv file contains 10000 tweets with seach query modi C:/Truc/sentiment/Sentiment/data/test_nor_811.csv
filename1 = 'C:/Truc/sentiment/Sentiment/data/train_nor_811.csv'
filename2 = 'C:/Truc/sentiment/Sentiment/data/test_nor_811.csv'
filename3 = 'C:/Truc/sentiment/Sentiment/data/valid_nor_811.csv'
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
with open('C:/Truc/sentiment/vietnamese-stopwords.txt', 'r',encoding='UTF-8') as f:
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
def predic(filename):
    ctSchema = StructType([
        StructField("Sentence", StringType())])
    df4 = sqlContext.read.format("csv").option("header", "true").option("encoding", "UTF-8").schema(customSchema).load(
        filename)
    df4_1 = df4.withColumn("Emotion", lit(0))
    dt= df4_1
    dt.show(5)
    print("type dt", type(dt))
    dtset = pipelineFit.transform(dt)
    dtset.show(10)

    # model = LogisticRegression.load("C:/Truc/sentiment/Sentiment/lr_model")
    pred = lrModel.transform(dtset)
    out = pred.select("Sentence","probability","prediction")
    print("OUTPUT ==============================")
    print(out.show())



predic('C:/Truc/sentiment/Sentiment/data/data.csv')

