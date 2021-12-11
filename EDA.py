import pandas as pd
from wordcloud import WordCloud, STOPWORDS
import re
import matplotlib.pyplot as plt

def cleanText(Topic_text,Topic):
        Topic = str(Topic).lower()
        Topic=' '.join(re.sub('([^0-9A-Za-z \t])', ' ', Topic).split())
        Topic = re.split("\s+",str(Topic))
        stopwords = set(STOPWORDS)
        stopwords.update(Topic)
        stopwords.update('amp')
        text_new = " ".join([txt for txt in Topic_text.split() if txt not in stopwords])
        return text_new

def all_wordcloud(data,Topic):
        text = []
        for w in data['words']:
            w=w.replace("'","").replace('[',"").replace(']',"").replace(',',"").split(" ")
            text.extend(w)
        text=' '.join(text)
        stopwords = ["https", "co", "amp"] + list(STOPWORDS)
        text_all = cleanText(text,Topic)
        wordcloud = WordCloud(stopwords=stopwords,max_words=800,max_font_size=70).generate(text_all)
        plt.imsave('wordclouds/all.png', wordcloud)
        return wordcloud


def positive_wordcloud(data,Topic):
        text = []
        df=data.loc[data['prediction']==0]
        for w in df['words']:
            w=w.replace("'","").replace('[',"").replace(']',"").replace(',',"").split(" ")
            text.extend(w)
        text=' '.join(text)
        stopwords = ["https", "co", "amp"] + list(STOPWORDS)
        text_pos = cleanText(text,Topic)
        wordcloud = WordCloud(stopwords=stopwords,max_words=800,max_font_size=70).generate(text_pos)
        plt.imsave('wordclouds/pos.png', wordcloud)
        return wordcloud

def negative_wordcloud(data,Topic):
        text = []
        df=data.loc[data['prediction']==1]
        for w in df['words']:
            w=w.replace("'","").replace('[',"").replace(']',"").replace(',',"").split(" ")
            text.extend(w)
        text=' '.join(text)
        stopwords = ["https", "co", "amp"] + list(STOPWORDS)
        text_neg = cleanText(text,Topic)
        wordcloud = WordCloud(stopwords=stopwords,max_words=800,max_font_size=70).generate(text_neg)
        plt.imsave('wordclouds/neg.png', wordcloud)
        plt.imshow(wordcloud, interpolation='bilinear')
        return wordcloud
