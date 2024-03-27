import math
import pandas as pd
import numpy as np
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SQLContext
from pyspark import SparkContext

from pyspark.sql.types import *

sc =SparkContext()
sqlContext = SQLContext(sc)
customSchema = StructType([
    StructField("clean_text", StringType()),
    StructField("category", StringType())])
#modi_data.csv file contains 10000 tweets with seach query modi
# filename1 = 'C:/Truc/sentiment/data/train_nor_811.csv'
filename2 = 'C:/Truc/sentiment/data/data.csv'

filename1 = 'C:/Truc/sentiment/Sentiment-Analysis-using-Pyspark-on-Multi-Social-Media-Data/redt_dataset.csv'
#filename2 = 'C:/Truc/sentiment/Sentiment-Analysis-using-Pyspark-on-Multi-Social-Media-Data/twtr_dataset.csv'
df1 = sqlContext.read.format("csv").option("header", "true").schema(customSchema).load(filename1)
df1.count()

df2 = sqlContext.read.format("csv").option("header", "true").schema(customSchema).load(filename2)
df2.count()
data2 = df2.na.drop(how="any")
df = df1.union(df2)#, emp_acc_LoadCsvDF("acc_id").equalTo(emp_info_LoadCsvDF("info_id")), "inner").selectExpr("acc_id", "name", "salary", "dept_id", "phone", "address", "email")
df.count()

data = df1.na.drop(how='any')
data.show(5)

df1.count()

data.printSchema()

from pyspark.sql.functions import col

data.groupBy("category").count().orderBy(col("count").desc()).show()

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression

# regular expression tokenizer
regexTokenizer = RegexTokenizer(inputCol="clean_text", outputCol="words", pattern="\\W")

# stop words
add_stopwords = ["http","https","amp","rt","t","c","the"]

stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)

# bag of words count
countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=30000, minDF=5)

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
label_stringIdx = StringIndexer(inputCol = "category", outputCol = "label")

pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])

# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(data)
dataset = pipelineFit.transform(data)
dataset2 = pipelineFit.transform(data2)
dataset.show(50)

# set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.8, 0.2], seed = 100)
testdata2 = dataset2
print("Training Dataset Count: " + str(trainingData.count()))
print("Test Dataset Count: " + str(testData.count()))

# NAVIE BAYES

from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes(smoothing=1)
nbModel = nb.fit(trainingData)
predictions = nbModel.transform(testData)
# predictions.filter(predictions['prediction'] != 0) \
#     .select("clean_text","category","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 500, truncate = 30)
predictions.show()


evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
nbAccuracy = evaluator.evaluate(predictions)
print(nbAccuracy)

pred = nbModel.transform(testdata2)
pred.show()
#MODEL Logistic Regression using TF-IDF Features¶

# from pyspark.ml.feature import HashingTF, IDF
#
# hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=30000)
# idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
# pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, hashingTF, idf, label_stringIdx])
#
# pipelineFit = pipeline.fit(data)
# dataset = pipelineFit.transform(data)
#
# (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
# lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
# lrModel = lr.fit(trainingData)
#
# predictions = lrModel.transform(testData)
#
# predictions.filter(predictions['prediction'] == 0) \
#     .select("Sentence","Emotion","probability","label","prediction") \
#     .orderBy("probability", ascending=False) \
#     .show(n = 50, truncate = 30)
#
# evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
# evaluator.evaluate(predictions)
#
#
#
# #Cross-Validation
#
# pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])
#
# pipelineFit = pipeline.fit(data)
# dataset = pipelineFit.transform(data)
# (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
#
# lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
#
# from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
#
# # Create ParamGrid for Cross Validation
# paramGrid = (ParamGridBuilder()
#              .addGrid(lr.regParam, [0.1, 0.3, 0.5]) # regularization parameter
#              .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
# #            .addGrid(model.maxIter, [10, 20, 50]) #Number of iterations
# #            .addGrid(idf.numFeatures, [10, 100, 1000]) # Number of features
#              .build())
#
# # Create 5-fold CrossValidator
# cv = CrossValidator(estimator=lr, \
#                     estimatorParamMaps=paramGrid, \
#                     evaluator=evaluator, \
#                     numFolds=5)
#
# cvModel = cv.fit(trainingData)
#
# predictions = cvModel.transform(testData)
# # Evaluate best model
# evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
# evaluator.evaluate(predictions)
# #print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))
#
#-----------------------------------------------------------------------------------------------------------

