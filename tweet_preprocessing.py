import pandas as pd
from nltk.corpus import stopwords
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lower, col, trim

sc = SparkContext(appName="PySparkShell")
spark = SparkSession(sc)
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
dataFrame = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("encoding", "utf-8")\
    .load("tweet/tweet_collection.csv")

dataFrame=dataFrame.dropDuplicates(['tweet'])
dataFrame=dataFrame.withColumn("tweet_original", dataFrame["tweet"])

# Adding a space before and after each stopword so as not to consider 
# the case in which the stopword is contained in a word
words = set(stopwords.words('english'))
stopwords = [' ' + x + ' ' for x in words]

emoticons =  ('😇','😊','❤️','😘','💞','💖','🤗','💕','👏','🎉','👍',
              '😂','😡','😠','😭','🤦‍','🤷🏼‍','😞','👎','😱','😓','🔝')
dataFrame= dataFrame.withColumn('tweet', regexp_replace('tweet', '@[\w]*[_-]*[\w]*', ' ')) 
dataFrame=dataFrame.withColumn('tweet', regexp_replace('tweet', 'https?://[\w/%-.]*', ' ')) 
dataFrame=dataFrame.withColumn('tweet', regexp_replace('tweet', '[^ a-zA-Zà-ú'
                            '\😇\😊\❤️\😘\💞\💖\🤗\💕\👏\🎉\👍'
                            '\😂\😡\😠\😭\🤦‍\🤷🏼‍\😞\😱\👎\😓\🔝]', ' ')) 
dataFrame=dataFrame.withColumn('tweet', regexp_replace('tweet',"""[^ 'a-zA-Z0-9,.?!]""", ' '))

for word in emoticons:
    dataFrame= dataFrame.withColumn('tweet', regexp_replace('tweet',word, " "+word+" ")) 

dataFrame = dataFrame.withColumn('tweet', regexp_replace('tweet', '\s+', ' '))            # Removing excess spaces
dataFrame = dataFrame.withColumn('tweet', regexp_replace('tweet', '^ ', ''))                # Removing spaces at the beginnning
dataFrame = dataFrame.withColumn('tweet', regexp_replace('tweet', ' $', ''))                # Removing spaces at the end
dataFrame = dataFrame.withColumn('tweet', lower(col('tweet')))
dataFrame = dataFrame.withColumn('tweet', regexp_replace('tweet', '^', ' ')) 
dataFrame = dataFrame.withColumn('tweet', regexp_replace('tweet', '$', ' '))

for word in stopwords:
    dataFrame = dataFrame.withColumn('tweet', regexp_replace('tweet', word, ' '))

    dataFrame = dataFrame.withColumn('tweet', trim(col('tweet')))
dataFrame.toPandas().to_csv('tweet/tweet_preprocessed.csv')

