from google.cloud import bigquery, storage
import config as config

'''Setting up Big Query Client'''
client = bigquery.Client()
dataset_id = config.DATASET
dataset_ref = client.dataset(dataset_id, project=config.PROJECT_ID)

'''Setting up GCS Client'''
storage_client = storage.Client()
bucket = storage_client.get_bucket(config.BUCKET_NAME)

def load_csv(table_name, uri):
    ## data load ###
    #load csv into table and creates table in dataset
    job_config = bigquery.LoadJobConfig()

    job_config.autodetect = True

    #job_config.skip_leading_rows = 1

    load_job = client.load_table_from_uri(
        source_uris=uri,destination=dataset_ref.table(table_name),job_config=job_config
    )

    assert load_job.job_type == 'load'
    print('Starting job {} to load {} into dataset {} '.format(load_job.job_id, uri, dataset_ref.dataset_id ))
    #logging.info('Starting job {} to load {} into dataset {} '.format(load_job.job_id, file, dataset_ref.dataset_id ))
    load_job.result()  # Waits for table load to complete.
    print('Job finished.')
    #logging.info('Job Complete')
    assert load_job.state == 'DONE'
    destination_table = client.get_table(dataset_ref.table(table_name))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    #logging.info('Loaded {} rows.'.format(destination_table.num_rows))

def move_blob(sourceLocation, destination):

    import os

    os.system('gsutil mv gs://'+sourceLocation+' gs://'+destination+'')

    #logging.info('The file has been loaded from {} to {}'.format(sourceLocation,destination))




if __name__ == '__main__':

    blobs = bucket.list_blobs(prefix='Source/', delimiter='Source/')
    blob_list = []
    for blob in blobs:
        blob_list.append(blob.name)
    for file in blob_list[1:len(blob_list)]:
        load_csv(config.MASTER_TABLE, 'gs://' + config.BUCKET_NAME+'/'+file)
        source_location = config.BUCKET_NAME+'/'+file
        destination = config.BUCKET_NAME+'/Completed/'
        move_blob(source_location,destination)

