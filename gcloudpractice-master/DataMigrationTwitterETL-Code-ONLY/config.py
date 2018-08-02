import time
from time import gmtime, strftime

# Giving access to use client library
#for the purpose of migration user needs to be granted the following access levels
# bigquery.admin and storage.objectViewer

'''gs-bg-migration i.e dynamic load'''
PROJECT_ID ='warm-airline-207713'

KEY_PATH = 'authkey1.json'

#google cloud storage info

BUCKET_NAME = 'dod-mwja-project1'

STORAGE_KEY_PATH = 'storage_key.json'

#logging

LOGGING_FILENAME = 'DoD-Project1-logs1.log'

'''static-migrator  i.e batch load'''

time = strftime("%Y_%m_%d_%H_%M_%S", gmtime())

CSV_NAME = 'Tweets'+time+'.csv'
COLUMN_NAMES = ['Username', 'Tweet', 'Time', 'Followers', 'Location', 'Source']



'''Twitter ETL start historical-tweets'''

TOPIC = 'Donald_Trump_tweets'
TRACK_TERMS = 'Donald_Trump'
COLUMN_NAMES = ['Username', 'Tweet', 'Time', 'Followers', 'Location', 'Source']
