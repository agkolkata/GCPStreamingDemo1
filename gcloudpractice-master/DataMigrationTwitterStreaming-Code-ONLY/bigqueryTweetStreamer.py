import config
from google.cloud import pubsub
import os
import time
from google.cloud import bigquery
import ast
import datetime
client =  bigquery.Client()
#.from_service_account_json(json_credentials_path=config.KEY_PATH,project=config.PROJECT_ID)
dataset_ref = client.dataset(dataset_id=config.DATASET,project=config.PROJECT_ID)
table_ref = dataset_ref.table(config.MASTER_TABLE)
table = client.get_table(table_ref)

#row = [('Munir', 'testweet', datetime.datetime.today(), 23, 'london', 'iphone')]
#client.insert_rows(table,row)


#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/709231/PycharmProjects/DataMigrationProjectGCP/pubsub-with-storage.json"
publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(config.PROJECT_ID, 'twitter-stream')

'''Create sub client'''
subscriber = pubsub.SubscriberClient()

'''Creating subscription '''
subscription_path = subscriber.subscription_path(config.PROJECT_ID, 'twitter_sub')
#subscriber.create_subscription(subscription_path,topic_path)



'''Subscribe to subscription'''
def callback(message):
    #print('Received message: {}'.format(message.data))
    templist = message.data.decode().split('-=-')
    for j, item in enumerate(templist):
        templist[j] = item.replace(',','')
    #templist[2] = datetime.datetime.strptime(templist[2],'%Y-%m-%d %H:%M:%S')
    #templist[3] = int(templist[3])
    #print(tuple(templist))
    row = [tuple(map(str,templist))]
    print(row)
    client.insert_rows(table, rows=row)
    #assert errors == []
    #print(message.data)
   # client.insert_rows(table,tuple(templist))
    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking, so we must keep the main thread from
# exiting to allow it to process messages in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(60)
