# coding=utf-8

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from os import listdir, remove
from os.path import isfile, join

import os
import sys
import unicodedata

from absl import app as absl_app
import pandas
import tensorflow as tf

import string
import collections
import tweepy

data="/home/maggie/twitter_data/German_Twitter_sentiment.csv"

def download_tweets(data_file=data):

    CONSUMER_KEY = "xq79TaJUVGQ1jAY2w37SFHJUU"
    CONSUMER_SECRET = "3WJmUaInU7jgabzVOpkAsaACPzZyZAAO3bk5D2D9pAAvfJINry"
    OAUTH_TOKEN = "2519101985-kAMW8WNwIwgQyk3B49qeSkuemNtybiMKmMrpcgy"
    OAUTH_TOKEN_SECRET = "kWxV58lHEZMq8dmQYsvh19c5c1STD0ShwepYYvVPl4vKM"

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    out = "/home/maggie/tweets"
    tweets = []
    index=0
    total=0
    file_num = 1

    with open(data_file, 'r') as f:
        line = f.readline()
        while True:
            line = f.readline()
            if not line:
                break
            total+=1
            tweet_id = line.split(',')[0]
            label = line.split(',')[1]
            annotator = line.split(',')[2]
            try:
                tweet = api.get_status(tweet_id)
            except:
                continue
            tweet = tweet.text
            tweets.append( (index, total, tweet_id, label, annotator[:-1] ,tweet) )
            index+=1
            if index % 100 == 0 :
                df = pandas.DataFrame(
                    data=tweets, columns=["Index", "Total","ID", "Label", "Annotator" ,"Tweet"]
                )
                output_file = out+str(file_num)+".tsv"
                file_num+=1
                df.to_csv(output_file, index=False, sep="\t")

            print(index, total, tweet_id)

            
    


def main(_):
    download_tweets()
   

if __name__ == "__main__":
    tf.logging.set_verbosity(tf.logging.INFO)
    absl_app.run(main)
