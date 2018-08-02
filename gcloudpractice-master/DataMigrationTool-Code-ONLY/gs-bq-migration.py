from google.cloud import bigquery, storage
import config as config
import os
import logging
import time

'''first we need to set up authentication so we can access bigQ and GCS from external apps
go into gc and set create a role with rights to bigquery.admin and storage.objectviewer.
Next download the .json key and store this in the config.py folder'''

'''NOTE TO SELF, TWO SEPERATE JSON KEYS SET UP, BIGQ CLIENT DOES NOT HAVE GCS STORAGE RIGHTS TO NEED TO MAKE FILES IN BUCKET PUBLIC'''

'''Create IAM Service Account'''
#os.system('gcloud iam service-accounts create config.ACCOUNT_NAME')
'''Add Role to Account'''
#os.system('gcloud projects add-iam-policy-binding config.PROJECT_ID --member "serviceAccount:"+config.ACCOUNT_NAME+"@"+config.PROJECT_ID+".iam.gserviceaccount.com" --role "roles/bigquery.admin"')
'''Generate .json key'''
#gcloud iam service-accounts keys create [FILE_NAME].json --iam-account [NAME]@[PROJECT_ID].iam.gserviceaccount.com

'''Setting up Big Query Client'''
client = bigquery.Client()
dataset_id = config.DATASET
dataset_ref = client.dataset(dataset_id, project=config.PROJECT_ID)

'''Setting up GCS Client'''
storage_client = storage.Client()
bucket = storage_client.get_bucket(config.BUCKET_NAME)

logging.basicConfig(filename=config.LOGGING_FILENAME, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

'''Creating a new dataset'''

def create_dataset():

    datasets = client.list_datasets()
    list_of_datasets = []

    for dataset in datasets:
        list_of_datasets.append(dataset.dataset_id)

    dataset_id = config.DATASET

    if dataset_id not in list_of_datasets:

        dataset_ref = client.dataset(dataset_id,project=config.PROJECT_ID)

        dataset = bigquery.Dataset(dataset_ref)

        dataset = client.create_dataset(dataset)
        #confirm creation
        print('Dataset {} created.'.format(dataset.dataset_id))
        logging.info('Dataset {} created.'.format(dataset.dataset_id))
    else:
        print('The dataset:', dataset_id, 'already exists in Big Query.')


''' Dataset properties function for later use...metadata'''

def dataset_properties():
    dataset_id = config.DATASET

    dataset_ref = client.dataset(dataset_id, project=config.PROJECT_ID)

    dataset = bigquery.Dataset(dataset_ref)

    # view dataset properties
    print('Dataset ID'.format(dataset_id))
    print('Description: '.format(dataset.description))
    print('Labels:')
    for label, value in dataset.labels.items():
        print('\t{}: {}'.format(label, value))


def load_csv(file, uri):
    ## data load ###
    #load csv into table and creates table in dataset
    job_config = bigquery.LoadJobConfig()

    job_config.autodetect = True

    #job_config.skip_leading_rows = 1
    remove_source = file.split('/')
    file_with_extension = remove_source[1]

    remove_extension = file_with_extension.split('.')
    table_name = remove_extension[0]

    load_job = client.load_table_from_uri(
        source_uris=uri,destination=dataset_ref.table(table_name),job_config=job_config
    )

    assert load_job.job_type == 'load'
    print('Starting job {} to load {} into dataset {} '.format(load_job.job_id, file, dataset_ref.dataset_id ))
    logging.info('Starting job {} to load {} into dataset {} '.format(load_job.job_id, file, dataset_ref.dataset_id ))
    load_job.result()  # Waits for table load to complete.
    print('Job finished.')
    logging.info('Job Complete')
    assert load_job.state == 'DONE'
    destination_table = client.get_table(dataset_ref.table(table_name))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    logging.info('Loaded {} rows.'.format(destination_table.num_rows))
'''example for loading files directly into GCS bucket'''
#blob = bucket.blob('testUpLoadWebLogs')
#blob.upload_from_filename('C://Users/709231/Desktop/weblogs1.csv')

'''example for getting table metadata'''
#gets info about table
#dataset_ref = client.dataset(dataset_id)
#table_ref = dataset_ref.table(config.table_name)
#table = client.get_table(table_ref)  # API Request

# View table properties
#print(table.schema) #schema
#print(table.description) #metadata
#print(table.num_rows) #number of rows
#print(table.created)

'''Moving blob to either a completed or a failed folder within bucket'''

def move_blob(sourceLocation, destination):

    import os

    os.system('gsutil mv gs://'+sourceLocation+' gs://'+destination+'')

    logging.info('The file has been loaded from {} to {}'.format(sourceLocation,destination))



if __name__ == '__main__':

    while True:
        create_dataset()

        '''Creating a list of files (blobs) currently inside the GCS bucket'''
        blobs = bucket.list_blobs(prefix='Source/', delimiter='Source/')
        blob_list = []
        for blob in blobs:
            blob_list.append(blob.name)
        print(blob_list)

        #print(blob_list)
        #print(config.LIST_OF_FILES)

        '''Creating a list of tables currently inside the dataset in Big Query'''
        tables = list(client.list_tables(dataset_ref))  # API request(s)
        list_of_tables = []
        if tables:
            for table in tables:
                list_of_tables.append(table.table_id)
        else:
            print('This dataset does not contain any tables')
        #print('Tables currently in ')
        #print(list_of_tables)

        '''Start of logic for migration'''

        for file in blob_list[1:len(blob_list)]:
            print(file, 'is in the bucket', config.BUCKET_NAME)
            remove_source = file.split('/')
            file_with_extension = remove_source[1]
            remove_extension = file_with_extension.split('.')
            table_name = remove_extension[0]
            if table_name in list_of_tables:
                print('There is already a table with the name:', file, 'in this dataset')
                print('Moving', file, 'to Failed folder. Please see logs')
                uri=config.BUCKET_NAME+'/'+file
                destination = config.BUCKET_NAME+'/Failed/'
                move_blob(uri,destination)
                print('The file:', file, 'has been moved to', destination)
                logging.warning('{} already in the dataset now moved to {} in your bucket'.format(file, '/Failed'))
                logging.info('Please delete when you have confirmed this file exists')
            else:
                uri = 'gs://' + config.BUCKET_NAME+'/'+file
                load_csv(file, uri)
                destination = config.BUCKET_NAME+'/Completed/'
                uri = config.BUCKET_NAME+'/'+file
                move_blob(uri,destination)
                print('The file', file, 'has successfully been migrated and is now in the folder', destination )
                logging.info('{} migration successful'.format(file))


        os.system('gsutil cp '+config.LOGGING_FILENAME+' gs://'+config.BUCKET_NAME)

        time.sleep(60)

