import config
def move_blob(sourceLocation, destination):
    import os

    os.system('gsutil mv gs://' + sourceLocation + ' gs://' + destination + '')


move_blob(config.BUCKET_NAME+'/Failed/*', config.BUCKET_NAME+'/Source/')
