import apache_beam as beam

import config
import argparse
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from textblob import TextBlob

options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = config.PROJECT_ID
google_cloud_options.staging_location = 'gs://dod-mwja-project11/staging'
google_cloud_options.temp_location = 'gs://dod-mwja-project11/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(StandardOptions).streaming = True
options.view_as(SetupOptions)



def compute_sentiment(line):
    #import os
    #os.system('sudo pip install textblob')
    import textblob
    from textblob import TextBlob
    templist = line.split('-=-')
    for j, item in enumerate(templist):
        templist[j] = item.replace(',', '')
    tweet = templist[1]
    sent = TextBlob(tweet).sentiment.polarity
    templist.append(str(sent))

    diction = dict(zip(['uName', 'uTweet', 'uTime', 'nFollowers', 'uLoc', 'dSource', 'uSentiment'], templist))

    return diction

'''class sentimentDoFn(beam.DoFn):
    def process(self, element):
        #import os
        #os.system('sudo pip install textblob')
        from textblob import TextBlob
        templist = element.split('-=-')
        for j, item in enumerate(templist):
            templist[j] = item.replace(',', '')
        tweet = templist[1]
        sent = TextBlob(tweet).sentiment.polarity
        templist.append(sent) 
        diction = dict(zip(['uName', 'uTweet', 'uTime', 'nFollowers', 'uLoc', 'dSource', 'uSentiment'], templist))
        return diction'''
        
        
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--requirements_file', required=True)
    pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(options=options, argv=pipeline_args) as p:
        # Read the pubsub topic into a PCollection.
        lines = (p | beam.io.ReadStringsFromPubSub(topic='projects/stream4gcp1/topics/twitter-stream')
                   | beam.Map(compute_sentiment)
                   | beam.io.WriteToBigQuery('stream4gcp1:Tweets_raw.Tweets_data1',
                    schema='uName:STRING, uTweet:STRING, uTime:TIMESTAMP, nFollowers:INTEGER, uLoc:STRING, dSource:STRING, uSentiment:FLOAT',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

if __name__ == '__main__':
    run()


