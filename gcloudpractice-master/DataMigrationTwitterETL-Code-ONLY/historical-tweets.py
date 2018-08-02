import datetime
import pandas as pd
import random
import listofcities as ct
from GetOldTweets import got3 as got
from faker import Faker as gen
import config
import csv
from pandas import  read_csv
from google.cloud import bigquery, storage

'''Creating the client to use with GCS - json key required if running from external application'''

storage_client = storage.Client()
#.from_service_account_json(json_credentials_path=config.STORAGE_KEY_PATH,project=config.PROJECT_ID)
bucket = storage_client.get_bucket(config.BUCKET_NAME)


with open(config.TOPIC+'.csv', 'a') as f:
    line_writer = csv.DictWriter(f, fieldnames=config.COLUMN_NAMES,lineterminator='\n')
    line_writer.writeheader()



tweetCriteria = got.manager.TweetCriteria().setQuerySearch(config.TRACK_TERMS).setSince("2018-06-01").setUntil("2018-06-02").setMaxTweets(1000)
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
        
os.system('gsutil cp' +config.TOPIC+'.csv' 'gs://'+config.BUCKET_NAME+'/Source/')


