import time
from time import gmtime, strftime

# Giving access to use client library
#for the purpose of migration user needs to be granted the following access levels
# bigquery.admin and storage.objectViewer

'''gs-bg-migration i.e dynamic load'''
PROJECT_ID ='warm-airline-207713'

KEY_PATH = 'authkey1.json'

#creating dataset

DATASET = 'Tweets_raw'

#google cloud storage info

BUCKET_NAME = 'dod-mwja-project1'

STORAGE_KEY_PATH = 'storage_key.json'

#loading from gcs to big q

#can create function to auto genrate list of files directly from bucket using list blobs method
#ALL_files =
#LIST_OF_FILES = ['SampleCSVFile_119kb.csv', 'SampleCSVFile_556kb.csv', 'sample_with_headers.csv', 'blaaah',]

#header available = yes/no

#logging

LOGGING_FILENAME = 'DoD-Project1-logs1.log'

'''static-migrator  i.e batch load'''

MASTER_TABLE = 'Donald_Trump_Tweets'


