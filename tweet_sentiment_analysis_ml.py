# -*- coding: UTF-8 -*-
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import col
from pyspark.sql.functions import count
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
# import findspark
# findspark.init()

def start_spark():
    sc = SparkContext(appName="PySparkShell")
    spark = SparkSession(sc)
    return spark

spark=start_spark()
dataFrame = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("encoding", "utf-8")\
    .load("tweet/tweet_labelled.csv")


(trainingD, testD) = dataFrame.randomSplit([0.9, 0.1])
trainingData = trainingD.select("tweetP","Category")
testData = testD.select("tweetP","Category")

label_string_idx = StringIndexer()\
                  .setInputCol("Category")\
                  .setOutputCol("label")

tokenizer = Tokenizer(inputCol="tweetP", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures")
idf = IDF(minDocFreq=3, inputCol="rawFeatures", outputCol="features")


##############################################    LOGISTIC REGRESSION    ##############################################

lr = LogisticRegression(featuresCol="features", labelCol="label",predictionCol="prediction",maxIter=20, regParam=0.3, elasticNetParam=0)
pipeline_lr = Pipeline(stages=[tokenizer, hashingTF, idf, label_string_idx, lr])
model_lr = pipeline_lr.fit(trainingData)
predictions_lr=model_lr.transform(testData)
pipeline_lr.write().overwrite().save('model/LogisticRegression')
predictions_lr.select('tweetP','Category',"probability","label","prediction").show(30)


evaluator_cv_lr = MulticlassClassificationEvaluator().setPredictionCol("prediction").evaluate(predictions_lr)
print('------------------------------Accuracy----------------------------------')
print(' ')
print('               Logistic Regression accuracy:{}:'.format(evaluator_cv_lr))


predictions_lr_metrics=predictions_lr.select("label", "prediction").rdd
lr_metrics = MulticlassMetrics(predictions_lr_metrics)
print(lr_metrics.confusionMatrix().toArray())


##############################################    NAIVE BAYES    ##############################################


nb = NaiveBayes(featuresCol="features", labelCol="label",predictionCol="prediction",smoothing=1)
pipeline_nb = Pipeline(stages=[tokenizer, hashingTF, idf, label_string_idx, nb])
model_nb = pipeline_nb.fit(trainingData)
predictions_nb=model_nb.transform(testData)

pipeline_nb.write().overwrite().save('model/NaiveBayes')
predictions_nb.select('tweetP','Category',"probability","label","prediction").show(30)


evaluator_cv_nb = MulticlassClassificationEvaluator().setPredictionCol("prediction").evaluate(predictions_nb)
print('------------------------------Accuracy----------------------------------')
print(' ')
print('                      Naive Bayes accuracy:{}:'.format(evaluator_cv_nb))

predictions_nb_metrics=predictions_nb.select("label", "prediction").rdd
nb_metrics = MulticlassMetrics(predictions_nb_metrics)
print(nb_metrics.confusionMatrix().toArray())

