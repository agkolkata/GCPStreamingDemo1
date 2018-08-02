import config
from google.cloud import pubsub
import os
import time
from google.cloud import bigquery
from textblob import TextBlob


import ast
import datetime
client =  bigquery.Client()
#.from_service_account_json(json_credentials_path=config.KEY_PATH,project=config.PROJECT_ID)
dataset_ref = client.dataset(dataset_id=config.DATASET,project=config.PROJECT_ID)
table_ref = dataset_ref.table(config.STREAM_TABLE)
table = client.get_table(table_ref)

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/709231/PycharmProjects/DataMigrationProjectGCP/pubsub-with-storage.json"
publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(config.PROJECT_ID, 'twitter-stream')

'''Create sub client'''
subscriber = pubsub.SubscriberClient()

'''Creating subscription '''
subscription_path = subscriber.subscription_path(config.PROJECT_ID, 'twitter_sub_DS')
#subscriber.create_subscription(subscription_path,topic_path)

'''Subscribe to subscription'''
def callback(message):

    templist = message.data.decode().split('-=-')
    for j, item in enumerate(templist):
        templist[j] = item.replace(',','')

    tweet = templist[1]
    sent = TextBlob(tweet).sentiment.polarity
    templist.append(sent)

    row = [tuple(map(str,templist))]
    print(row)
    client.insert_rows(table, rows=row)

    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(60)
