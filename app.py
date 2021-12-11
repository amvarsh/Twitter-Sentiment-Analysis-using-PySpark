
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from tweet_collection import realtime_data
from tweet_preprocessing import realtime_process
from tweet_sentiment_analysis import label
import numpy as np
import streamlit as st
import atexit
import matplotlib.pyplot as plt
from EDA import all_wordcloud, negative_wordcloud, positive_wordcloud
import findspark
findspark.init()
from tweet_sentiment_predict import realtime_predict
from PIL import Image
import sys

sys.setrecursionlimit(3000)
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

st.set_page_config(layout='wide')

STYLE = """
<style>
img {
    max-width: 100%;
}
</style> """
st.markdown(
    """
    <style>
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
        width: 700px;
    }
    [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
        width: 70px;
        margin-left: -70px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

col1, col2 = st.columns(2)
html_temp = """<div style="text-align: center;background-color:powderblue;"><p style="color:black;font-size:40px;padding:9px">Twitter Sentiment Analysis</p></div>"""
st.sidebar.markdown(html_temp, unsafe_allow_html=True)

st.set_option('deprecation.showPyplotGlobalUse', False)
st.sidebar.subheader("A Twitter Sentiment analysis project which will scrape twitter for the topic selected ")

image = Image.open('logo.jpg')
st.sidebar.image(image,use_column_width=True)

with st.sidebar.form(key='my_form'):
    Topic = str()
    st.write('Enter the topic you are interested in (Click Go! once done)')
    Topic = str(st.text_input("Search"))
    submit = st.form_submit_button(label='Go!')

if submit:
    with st.sidebar:
        with st.spinner("Collecting tweets"):
            realtime_data(Topic)
        with st.spinner("Processing tweets"):
            realtime_process(spark)
        with st.spinner("Analysing tweets"):
            label(spark)
            realtime_predict(sc,spark)
        st.success('Sentiment Analysis Completed!!!') 

    dataFrame = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("encoding", "utf-8")\
        .load("realtime/realtime_lr_predictions.csv")

    st.sidebar.write(dataFrame.select("tweetP","words","prediction").toPandas().head(10))
      
    tot_count=dataFrame.count()
    pos_count=dataFrame.filter(dataFrame.prediction == 0).count()
    neg_count=dataFrame.filter(dataFrame.prediction == 1).count()
    neutral_count=dataFrame.filter(dataFrame.prediction == 2).count()
    pos_cent=(pos_count/tot_count)*100
    neg_cent=(neg_count/tot_count)*100
    neutral_cent=(neutral_count/tot_count)*100
    
    col1.header("Total Tweets Extracted are : {}".format(tot_count))
    
    with col2:
        st.metric("Positive Tweets", pos_count, "{}%".format(pos_cent))
    with col2:
        st.metric("Neutral Tweets", neutral_count, "{}%".format(neutral_cent))
    with col2:
        st.metric("Negative Tweets", neg_count, "-{}%".format(neg_cent))
    # st.sidebar.write("Total Positive Tweets are : {}".format(pos_count))
    # st.sidebar.write("Total Negative Tweets are : {}".format(neg_count))
    # st.sidebar.write("Total Neutral Tweets are : {}".format(neutral_count))
    
    with col1:
        st.subheader("Pie Chart for Different Sentiments of Tweets")
        d=np.array([pos_count,neg_count,neutral_count])
        explode = (0.1, 0.0, 0.1)
        plt.pie(d,shadow=True,explode=explode,labels=["Positive","Negative","Neutral"],autopct='%1.2f%%')
        plt.savefig("wordclouds/pie.jpeg")
        plt.clf()
        img = Image.open("wordclouds/pie.jpeg")
        st.image(img)


    with col1:
        st.subheader("WordCloud for all tweets said about {}".format(Topic))
        wordcloud_all = all_wordcloud(dataFrame.toPandas(),Topic)
        st.write(plt.imshow(wordcloud_all, interpolation='bilinear'))
        st.pyplot()

    with col2:
        st.subheader("WordCloud for positive tweets said about {}".format(Topic))
        wordcloud_positive = positive_wordcloud(dataFrame.toPandas(),Topic)
        st.write(plt.imshow(wordcloud_positive, interpolation='bilinear'))
        st.pyplot()

    with col2:
        st.subheader("WordCloud for negative tweets said about {}".format(Topic))
        wordcloud_negative = negative_wordcloud(dataFrame.toPandas(),Topic)
        st.write(plt.imshow(wordcloud_negative, interpolation='bilinear'))
        st.pyplot()

   

    # st.image('wordclouds/all.png')
atexit.register(sc.stop,"Successfully stopped SparkContext!!!")