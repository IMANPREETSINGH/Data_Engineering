'''
File        :   getfiledatas3
Description :   to get file date from s3
Creator     :   Imanpreet Singh 

Assumptions:    Bucket Name = env + filename

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''


from logging import exception
from datetime import datetime
import pandas as pd
import boto3 
import os 
import sys
import time

from pathy import set_client_params

from awsS3.createbucketS3 import CreateBucketS3

def GetFileDatas3(bucketname, key, logfile, **s3params):

    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)
        
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")
 
    msg = "GetFileDatas3 process started"
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print(msg)
    
    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------

    msg = ''
    if len(s3params) == 0:
        msg = "s3 params are not provided, exiting"
    elif bucketname == '' :
        msg = "bucketname is mandatory, exiting"
    elif key == '' :
        msg = "key is mandatory, exiting"  

    if len(msg) != 0:
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    #-----------------------------------------------------------
    #S3 Client
    #-----------------------------------------------------------
    s3client = boto3.client('s3',**s3params) 
    try:
        s3client = boto3.client('s3',**s3params) 
    except:
        msg = "Issue in creating S3 client"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)


    #-----------------------------------------------------------
    #S3 Bucket check
    #-----------------------------------------------------------
    try:
        s3client.head_bucket(Bucket =bucketname)
        msg = "{} bucket exist, checking file".format(bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} bucket doesn't exists, exiting".format(bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    #-----------------------------------------------------------
    #S3 key check
    #-----------------------------------------------------------
    try:
        s3client.head_object(Bucket =bucketname, Key = key)
        msg = "{} key/file exist, getting data".format(key)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} key/file doesn't exists, exiting".format(key)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    s3file = s3client.get_object(Bucket =bucketname, Key = key)
    time.sleep(3)
    
    dfdata = pd.read_csv(s3file['Body'])
    return (dfdata)