#!/usr/bin/env python

import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
import simplejson as json

from config import *

# es = Elasticsearch()

class TweetStreamListener(StreamListener):
    
    def on_data(self, data):
        dict_data = json.loads(data)

        text = ''

        if not dict_data['text'].startswith("RT @"):
            try:
                text = dict_data['extended_tweet']['full_text']
            except KeyError:
                text = dict_data['text']
        else:
            try:
                text = dict_data['retweeted_status']['extended_tweet']['full_text']
            except KeyError:
                text = dict_data['retweeted_status']['text']

            text = "RT: {}".format(text)

        print(text)
        print('')


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
    stream.filter(track=['tlry'])
    
