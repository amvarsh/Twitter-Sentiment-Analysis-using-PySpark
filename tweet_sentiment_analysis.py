from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType
from textblob import TextBlob

def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None

def getsentiment(polarity):
    if isinstance(polarity, float):
        if(polarity<0):
            return 'negative'
        elif(polarity>0):
            return 'positive'
        else:
            return 'neutral'

def label(spark):
    dataFrame = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("encoding", "utf-8")\
        .load("realtime/realtime_preprocessed.csv")

    convertUDF = udf(lambda z: sentiment_calc(z),FloatType())
    dataFrame = dataFrame.withColumn('polarity',convertUDF('tweetP'))

    convertUDF = udf(lambda z: getsentiment(z),StringType())
    dataFrame = dataFrame.withColumn('Category',convertUDF('polarity'))
    dataFrame = dataFrame.na.drop()
    dataFrame = dataFrame.drop("polarity").drop("_c0").drop("username")
    dataFrame.toPandas().to_csv('realtime/realtime_labelled.csv',mode='w')
