import tweepy
import csv

def authorize():
        auth = tweepy.OAuthHandler("g0FEzemjOJlnMubVL4ubP00XN", "XNKXE4uv45k8ERMPbo71BpEIeVWjZRti8bmtgcumj7uf971hxr")
        auth.set_access_token("1365764613481005056-EWRUoWuJWzHDTFV6Npj1gwp7iV2noZ","FDwy3cCRXbUYkk54g4yx9Om07dBk5mSYxzcOH1s6Ej4qz")

        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api

def realtime_data(keyword):
        api=authorize()
        csvFile = open('realtime/realtime_tweets.csv', 'w', encoding="utf-8")
        csvWriter = csv.writer(csvFile)
        csvWriter.writerow(["username","tweet"])
        # Collecting tweets which has the keyword mentioned
        for tweet in tweepy.Cursor(api.search, lang="en",  q=keyword, tweet_mode="extended").items(300):
                
                if not (hasattr(tweet, 'retweeted_status')):
                        csvWriter.writerow([tweet.user.screen_name,
                                        tweet.full_text.replace("\n", "")])

                if (hasattr(tweet, 'retweeted_status')): # taking retweets
                        csvWriter.writerow([tweet.user.screen_name,
                                        tweet.retweeted_status.full_text.replace("\n", "")])

