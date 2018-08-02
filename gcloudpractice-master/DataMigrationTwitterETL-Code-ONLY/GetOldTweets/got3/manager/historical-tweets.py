import datetime
import pandas as pd
import random
import listofcities as ct
from GetOldTweets import got3 as got
from faker import Faker as gen
import config
import csv
from pandas import  read_csv
tweetCriteria = got.manager.TweetCriteria().setQuerySearch(config.TRACK_TERMS).setSince("2018-05-01").setUntil("2018-05-30")
tweet = got.manager.TweetManager.getTweets(tweetCriteria)
for i in range(len(tweet)):

    username = gen()
    name = username.name()
    name_final = name.split(' ')
    names = ''.join(name_final)
    tweet_text = tweet[i].text
    time = tweet[i].date
    followers = random.randint(1,20000)
    city = ct.r_city()
    source = ct.r_source()
    line = [names, tweet_text.encode('utf-8'), time, followers, city, source]
    print(line)
    with open(config.TOPIC+'.csv', 'a') as f:
        line_writer = csv.writer(f, dialect='unix')
        line_writer.writerow(line)


df = read_csv(config.TOPIC+'.csv')
df.columns = config.COLUMN_NAMES
df.to_csv(config.TOPIC+'.csv',index=False)