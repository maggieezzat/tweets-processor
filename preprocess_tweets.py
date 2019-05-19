# coding=utf-8

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from os import listdir, remove, mkdir
from os.path import join, dirname, abspath, isdir
import sys
from absl import app as absl_app
import pandas
import tweepy



data = join(dirname(abspath(__file__)), "German_Twitter_sentiment.csv")
current_path = dirname(abspath(__file__))


def download_tweets(data_file=data):

    CONSUMER_KEY = "xq79TaJUVGQ1jAY2w37SFHJUU"
    CONSUMER_SECRET = "3WJmUaInU7jgabzVOpkAsaACPzZyZAAO3bk5D2D9pAAvfJINry"
    OAUTH_TOKEN = "2519101985-kAMW8WNwIwgQyk3B49qeSkuemNtybiMKmMrpcgy"
    OAUTH_TOKEN_SECRET = "kWxV58lHEZMq8dmQYsvh19c5c1STD0ShwepYYvVPl4vKM"

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    tweets = []
    index=0
    total=0
    file_num = 1
    output_dir = join(current_path,"downloaded_tweets")
    exists = isdir(output_dir)
    if not exists:
        mkdir(output_dir)

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
            if index % 1000 == 0 :
                df = pandas.DataFrame(
                    data=tweets, columns=["Index", "Total","ID", "Label", "Annotator" ,"Tweet"]
                )
                file_name="tweets" + str(file_num) + ".tsv"
                output_file = join(output_dir, file_name)
                file_num+=1
                print(output_file)
                df.to_csv(output_file, index=False, sep="\t")

            print(index, total, tweet_id)



def remove_extra_annotations():
    print("keep only one annotation for each tweet")  



def split_dataset():
    print("split into train and test")  



def main(_):
    download_tweets()
   

if __name__ == "__main__":
    absl_app.run(main)
