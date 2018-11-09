#!/usr/bin/env python

import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
import simplejson as json

from config import *

es = Elasticsearch(['sdl16377.labs.teradata.com:9200'])

class TweetStreamListener(StreamListener):
    
    def on_data(self, data):
        dict_data = json.loads(data)

        try:
            text = dict_data['text']
        except KeyError as e:
            return True


        if not text.startswith("RT @"):
            try:
                text = dict_data['extended_tweet']['full_text']
            except KeyError:
                pass
        else:
            try:
                text = dict_data['retweeted_status']['extended_tweet']['full_text']
            except KeyError:
                try:
                    text = dict_data['retweeted_status']['text']
                except KeyError:
                    pass

            text = "RT: {}".format(text)


        tweet = TextBlob(text)

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # add text and sentiment info to elasticsearch
        es.index(index="sentiment",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": text,
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       "sentiment": sentiment})
        return True


    def on_error(self, status):
        print(status)



if __name__ == '__main__':

    listener = TweetStreamListener()

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for "xxx" keyword
    stream.filter(track=['Thousand Oaks'])
