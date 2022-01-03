'''
File        :   get_s3_csv_file_script
Description :   to fetch csv file from aws s3 bucket
                return csv data as data frame 
Creator     :   Imanpreet Singh 


Version     Date        Author              Description
1           01-01-2022  Imanpreet Singh

'''


import pandas as pd 

#sdk to interact with AWS services. In our case it is S3 bucket
import boto3

#config.py created by me, get config function from it
from config_script import config

def get_s3_csv_file (fileName, bucketName, configFile, configSection):

    ####################### AWS S3 Connection and Load  ###################### 

    print ('Fetching Csv file from S3')

    #using self created config function to get paramerters from initialization (config) file
    params_s3 = config(file=configFile,section=configSection)
    #print(params_s3['region']) 
    #params_s3 is a dictionary

    #   one can pass dictionary as an input parameter using **dictionary
    #   however this will work only when keys of dictionary will match exactly with expected input paramerters
    #   e.g. another way to do without user defined config function which provide config values as dictionary is as below: 
    """
        from configparser import ConfigParser
        parser = ConfigParser()
        parser.read(config.ini')

        aws_access_key_param = parser['aws_s3']['aws_access_key_id']
        aws_secret_access_key_param = parser['aws_s3']['aws_secret_access_key']
        region_param = parser['aws_s3']['region_name']

        s3_client = boto3.client('s3', aws_access_key_id = aws_access_key_param, aws_secret_access_key = aws_secret_access_key_param, region_name = region_param )
    """
    #   we can use dictionary only when keys matches with aws_access_key_id,aws_secret_access_key and region_name exactly.



    #make connection to AWS S3 service, here we can define the service on which we want to work
    s3_client = boto3.client('s3',**params_s3) 

    '''
    #to get list of buckets wtih other details in json format
    response = s3_client.list_buckets()

    #respone is json
    print (response)

    #Buckets object is list 
    print (response['Buckets'])

    #Further each element of Buckets list is again a JSON - corresponding to each bucket
    print (response['Buckets'][0]['Name'])

    #getting buckets name
    for bucket in response['Buckets']:
        print (bucket['Name'])

    '''

    print ('Loading Csv file to data frame')

    #load data from bucket = 's3-bucket-file-load-to-pgdb' to data frame
    s3_file = s3_client.get_object(Bucket = bucketName, Key = fileName)
    df_csv = pd.read_csv(s3_file['Body'])


    return df_csv
