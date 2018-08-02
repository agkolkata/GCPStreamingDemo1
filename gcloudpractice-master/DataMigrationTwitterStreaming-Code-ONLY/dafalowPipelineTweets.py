import apache_beam as beam
import apache_beam.transforms.window as window
import config
from google.cloud import pubsub
import os
import argparse
def parse_pubsub(line):
    import json
    record = json.loads(line)
    return (record['Name']), (record['Tweet']), (record['Time']), (record['Followers']), (record['Location']), (record['Device'])


def run(argv=None):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True,help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    parser.add_argument('--output_table', required=True,help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
        # Read the pubsub topic into a PCollection.
        lines = (p | beam.io.ReadStringsFromPubSub(known_args.input_topic)
                   | beam.Map(parse_pubsub)
                   | beam.Map(lambda Name_bq, Tweet_bq, Time_bq, Followers_bq, Location_bq, Device_bq: {'Name':Name_bq , 'Tweet':Tweet_bq, 'Time': Time_bq, 'Followers':Followers_bq, 'Location':Location_bq, 'Device':Device_bq})
                   | beam.io.WriteToBigQuery(
                    known_args.output_table,
                    schema='Username:STRING, Tweet:STRING, Time:TIMESTAMP, Followers:INTEGER, Location:STRING, Source:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

if __name__ == '__main__':
    run()


