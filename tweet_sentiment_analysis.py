import pandas as pd
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_replace,udf,col
from pyspark.sql.types import IntegerType

sc = SparkContext(appName="PySparkShell")
spark = SparkSession(sc)
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
dataFrame = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("encoding", "utf-8")\
    .load("tweet/tweet_preprocessed.csv")

neg_words= tuple(open('negative-words.txt').read().splitlines())
pos_words= tuple(open('positive-words.txt').read().splitlines())
excludedWords=("really", "truly", "actually" ,"indeed", "real", "downright", "forsooth", "now", "name", "nickname", "none", "nor")

def textSentiment(string):
    val = 0
    list = string.split()
    for word in list:
        if not word.startswith(excludedWords):
            if (word.startswith(pos_words)):
                val = val + 1

            if (word.startswith(neg_words)):
                if word not in ("not", "despite", "notwithstanding"):
                    val = val - 1

            # Il 'non' cambia dinamicamente la polarita' del tweet
            if word == "not":
                val = val * -1

            # Tutte le parole prima di 'ma' e 'però' non vengono considerate
            if word in ("but", "however", "yet"):
                val = 0

    if (val > 0):
        label = 2
    elif(val < 0):
        label = 0
    else:
        label = 1
    return label

#dfTweet[['label']] = dfTweet['tweet'].apply(lambda tweet: pd.Series(textSentiment(tweet)))
udf_sentiment = udf(lambda x:textSentiment(x),IntegerType())
dataFrame=dataFrame.withColumn("label",udf_sentiment(col("tweet")))
# Ordino il dataframe per la successiva parte di Machine Learning
dataFrame = dataFrame.drop("_c0").drop("username")
dataFrame.toPandas().to_csv('tweet/tweet_labelled.csv')

