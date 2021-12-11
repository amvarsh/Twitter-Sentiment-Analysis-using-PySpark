# import findspark
# findspark.init()
from pyspark.sql.functions import udf
import nltk
from pyspark.sql.types import StringType
import re

def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def realtime_process(spark):
    dataFrame = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("encoding", "utf-8")\
        .load("realtime/realtime_tweets.csv")

    dataFrame=dataFrame.dropna()
    
    convertUDF = udf(lambda z: clean_tweet(z),StringType())
    dataFrame = dataFrame.withColumn('tweetP',convertUDF('tweet'))

    dataFrame.toPandas().to_csv('realtime/realtime_preprocessed.csv', mode = 'w')

