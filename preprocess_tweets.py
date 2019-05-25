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
import os
import glob
import csv
from sklearn.model_selection import train_test_split



data = join(dirname(abspath(__file__)), "German_Twitter_sentiment.csv")
current_path = dirname(abspath(__file__))

CONSUMER_KEY = "xq79TaJUVGQ1jAY2w37SFHJUU"
CONSUMER_SECRET = "3WJmUaInU7jgabzVOpkAsaACPzZyZAAO3bk5D2D9pAAvfJINry"
OAUTH_TOKEN = "2519101985-kAMW8WNwIwgQyk3B49qeSkuemNtybiMKmMrpcgy"
OAUTH_TOKEN_SECRET = "kWxV58lHEZMq8dmQYsvh19c5c1STD0ShwepYYvVPl4vKM"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
api = tweepy.API(auth)




def download_tweets(data_file=data):
    
    tweets = []
    index=0
    total=0
    file_num = 1
    output_dir = join(current_path,"downloaded-tweets")
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
            try:
                tweet = api.get_status(tweet_id)
            except:
                continue
            tweet = tweet.text
            tweets.append( (tweet_id, label, tweet) )
            index+=1
            if index % 100 == 0 :
                df = pandas.DataFrame(
                    data=tweets, columns=["ID", "Label","Tweet"]
                )
                file_name="tweets" + str(file_num) + ".tsv"
                output_file = join(output_dir, file_name)
                file_num+=1
                df.to_csv(output_file, index=False, sep="\t")

            print(index, total, tweet_id)
    

    df = pandas.DataFrame(
            data=tweets, columns=["ID", "Label", "Tweet"]
        )
    file_name="tweets" + str(file_num) + ".tsv"
    output_file = join(output_dir, file_name)
    df.to_csv(output_file, index=False, sep="\t")


    last_file_name = "tweets" + str(file_num) + ".tsv"
    last_file = join(output_dir, last_file_name)
    new_file_name = join(output_dir, "tweets.tsv")
    files = glob.glob(output_dir+'/tweets*')
    for f in files:
        if f == last_file:
            continue
        os.remove(f)
    os.rename(last_file, new_file_name)

    return new_file_name



def get_missing_tweets(tweets_file, data_file=data):

    tweets = []
    total=0
    index = 0
    file_num = 1
    output_dir = join(current_path,"downloaded-tweets")
    exists = isdir(output_dir)
    if not exists:
        mkdir(output_dir)

    with open(data_file, 'r') as f:
        with open(tweets_file, 'r') as downloaded_f:
            
            f.readline()
            downloaded_f.readline()
            downloaded_ids = []

            while True:
                downloaded_tweet = downloaded_f.readline()
                if not downloaded_tweet:
                    break
                downloaded_tweet = downloaded_tweet.split('\t')
                if len(downloaded_tweet) < 3:
                    continue
                
                downloaded_tweet_id = downloaded_tweet[0]
                downloaded_ids.append(downloaded_tweet_id)

            while True:
                line = f.readline()
                if not line:
                    break
                total+=1
                tweet_id = line.split(',')[0]
                label = line.split(',')[1]

                if tweet_id not in downloaded_ids:
                    try:
                        tweet = api.get_status(tweet_id)
                    except:
                        print(tweet_id," can not be downloaded")
                        continue
                else:
                    print(tweet_id," exists")
                    continue
    
                tweet = tweet.text
                tweets.append( (tweet_id, label, tweet) )
                index+=1
                if index % 100 == 0 :
                    df = pandas.DataFrame(
                        data=tweets, columns=["ID", "Label", "Tweet"]
                    )
                    file_name="missing-tweets" + str(file_num) + ".tsv"
                    output_file = join(output_dir, file_name)
                    file_num+=1
                    df.to_csv(output_file, index=False, sep="\t")

                print(index, total, tweet_id)

    df = pandas.DataFrame(
            data=tweets, columns=["ID", "Label", "Tweet"]
        )
    file_name="missing-tweets" + str(file_num) + ".tsv"
    output_file = join(output_dir, file_name)
    df.to_csv(output_file, index=False, sep="\t")


    last_file_name = "missing-tweets" + str(file_num) + ".tsv"
    last_file = join(output_dir, last_file_name)
    new_file_name = join(output_dir, "missing-tweets.tsv")
    files = glob.glob(output_dir+'/missing-tweets*')
    for f in files:
        if f == last_file:
            continue
        os.remove(f)
    os.rename(last_file, new_file_name)

    return new_file_name



def concatenate_files(tweets_file, extra_tweets_file):
    
    output_dir = join(current_path,"downloaded-tweets")
    file_name = "all_tweets.tsv"
    output_file = join(output_dir, file_name)
    exists = isdir(output_dir)
    if not exists:
        mkdir(output_dir)
    
    with open(tweets_file, 'r') as tweets_f:
        with open(extra_tweets_file, 'r') as extra_f:
            with open(output_file, 'w') as output_f:
                tweets_f.readline()
                extra_f.readline()

                while True:
                    line = tweets_f.readline()
                    if not line:
                        break
                    output_f.write(line)
    
                while True:
                    line = extra_f.readline()
                    if not line:
                        break
                    output_f.write(line)
    
    
    os.remove(tweets_file)
    os.remove(extra_tweets_file)

    new_file_name = "tweets.tsv"
    new_output_file = join(output_dir, new_file_name)
    os.rename(output_file, new_output_file)
    return new_output_file
            


def handle_multiline_tweets(tweets_file):
    
    
    output_dir = join(current_path,"downloaded-tweets")
    file_name = "clean_tweets.tsv"
    output_file = join(output_dir, file_name)

    with open(tweets_file, 'r') as tweets_f:
        with open(output_file, 'w') as output_f:

            line = tweets_f.readline()
            tweet = line
            previous_tweet = tweet
            while True:
                line = tweets_f.readline()
                if not line:
                    break
                if len(line.split('\t')) == 3:
                    tweet = line
                    output_f.write(previous_tweet)
                    previous_tweet = tweet
                else:
                    if line.split('\t')[0] == '\n':
                        continue
                    else:
                        previous_tweet = previous_tweet[:-1] + " " + line
                        
            output_f.write(previous_tweet)

    os.remove(tweets_file)
    new_file_name = "tweets.tsv"
    new_output_file = join(output_dir, new_file_name)
    os.rename(output_file, new_output_file)
    return new_output_file




def remove_extra_annotations(tweets_file):
    
    output_dir = join(current_path,"downloaded-tweets")
    file_name = "single_ann_tweets.tsv"
    output_file = join(output_dir, file_name)
    
    with open(tweets_file, 'r') as tweets_f:
        with open(output_file, 'w') as output_f:
            lines = []
            while True:
                line = tweets_f.readline()
                if not line:
                    break
                lines.append(line)

            ids = []
            for line in lines:
                id = line.split('\t')[0]
                if id in ids:
                    continue
                else:
                    ids.append(id)
                    output_f.write(line)

    os.remove(tweets_file)
    new_file_name = "tweets.tsv"
    new_output_file = join(output_dir, new_file_name)
    os.rename(output_file, new_output_file)
    return new_output_file




def split_dataset(tweets_file):

    output_dir = join(current_path,"downloaded-tweets")
    test_file_name = "tweets-test.tsv"
    train_file_name = "tweets-train.tsv"
    test_file = join(output_dir, test_file_name)
    train_file = join(output_dir, train_file_name)

    labels = []
    texts = []

    # read full dataset file
    with open(tweets_file, "r") as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            labels.append(row[1])
            texts.append(row[2])

    # split dataset
    trn_texts, tst_texts, trn_labels, tst_labels = train_test_split(texts, labels, test_size=.15, random_state=42, stratify=labels)

    # write train and test datasets
    train = []
    test = []
    
    for i in range(len(trn_labels)):
        train.append([trn_labels[i], trn_texts[i]])
    
    for i in range(len(tst_labels)):
        test.append([tst_labels[i], tst_texts[i]])

    
    with open(train_file, "w") as file_write:
        writer = csv.writer(file_write, delimiter='\t')
        for row in train:
            writer.writerow(row)

    with open(test_file, "w") as file_write:
        writer = csv.writer(file_write, delimiter='\t')
        for row in test:
            writer.writerow(row)
                



def basic_clean(tweets_file):

    output_dir = join(current_path,"downloaded-tweets")
    file_name = "correct_format.tsv"
    output_file = join(output_dir, file_name)
    
    with open(tweets_file, 'r') as tweets_f:
        with open(output_file, 'w') as output_f:
            while True:
                line = tweets_f.readline()
                if not line:
                    break
                line = line.split('\t')
                output_f.write('\t'.join(line[2:]))

    os.remove(tweets_file)
    new_file_name = "tweets.tsv"
    new_output_file = join(output_dir, new_file_name)
    os.rename(output_file, new_output_file)
    return new_output_file




def remove_annotation_column(tweets_file):

    output_dir = join(current_path,"downloaded-tweets")
    file_name = "correct_format.tsv"
    output_file = join(output_dir, file_name)
    
    with open(tweets_file, 'r') as tweets_f:
        with open(output_file, 'w') as output_f:
            while True:
                line = tweets_f.readline()
                if not line:
                    break
                line = line.split('\t')
                new_list = []
                new_list.append(line[0])
                new_list.append(line[1])
                new_list.append(line[3])
                
                output_f.write('\t'.join(new_list))

    os.remove(tweets_file)
    new_file_name = "tweets.tsv"
    new_output_file = join(output_dir, new_file_name)
    os.rename(output_file, new_output_file)
    return new_output_file
                    



def main(_):
    
    tweets_file= "/home/maggie/tweets-processor/downloaded-tweets/tweets.tsv"
    #extra_tweets= "/home/maggie/tweets_processor/downloaded-tweets/missing-tweets.tsv"
    #tweets_file = download_tweets()
    #extra_tweets = get_missing_tweets(tweets_file)
    #all_tweets = concatenate_files(tweets_file, extra_tweets)
    #clean_tweets = handle_multiline_tweets(tweets_file)
    #clean_tweets_single_ann = remove_extra_annotations(clean_tweets)
    #split_dataset(clean_tweets_single_ann)
    split_dataset(tweets_file)



if __name__ == "__main__":
    absl_app.run(main)
