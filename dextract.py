import boto3
import botocore
import os
import json

from botocore.handlers import disable_signing

BUCKET_NAME = 'data-engineering-interns.macpaw.io'
KEY = 'files_list.data'

LOCAL_DIR = 'local/'

s3 = boto3.resource('s3')
s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

def download_json_files(files_list):
    for f in files_list:
        try:
            s3.Bucket(BUCKET_NAME).download_file(f, f)
            print ('Downloaded ' + f)
        except botocore.exceptions.ClientError as e:
            print (e.response['Error'])

def get_json_file_local(filename):
    json_file = open(filename)
    res = json.load(json_file)
    json_file.close()
    os.remove(filename)
    return res

def get_new_files():

    old_files_list = []
    files_list = []

    #check if previously downloaded files_list exists, if yes, parse into a list
    if os.path.isfile(KEY):
        print('Reading old files_list...')
        old_files_list = open(KEY).read().splitlines()

    #download new files_list
    try:
        print('Connecting to S3...')
        s3.Bucket(BUCKET_NAME).download_file(KEY, KEY)
        print('Downloaded '+KEY)
    except botocore.exceptions.ClientError as e:
        print (e.response['Error'])
        quit()

    #parse new files_list into a list
    with open(KEY) as f:
        files_list = f.read().splitlines()
        print(KEY + ' contains ' + str(len(files_list)) + ' filenames')

    #return new files
    res = set(files_list) - set(old_files_list)
    print(str(len(res)) + ' new filenames found')

    deleted = set(old_files_list) - set(files_list)
    print(str(len(deleted)) + ' filenames deleted')

    download_json_files(res)
    
    return res
