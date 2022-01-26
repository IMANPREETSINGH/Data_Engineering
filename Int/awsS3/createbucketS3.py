'''
File        :   createbucketS3
Description :   to create bucket in s3
Creator     :   Imanpreet Singh 

Assumptions:    

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''

from logging import exception 
from datetime import datetime

import sys
import boto3
from datetime import datetime

def CreateBucketS3(bucketname,logfile, **s3params):
    
    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)
        
    logfileobj = open(logfile, "a")
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')

    msg = "CreateBucketS3 process started"
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print(msg)

    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------

    if len(s3params) == 0:
        msg = "s3 params are not provided, exiting"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)
  
    #check region of bucket
    if 'region_name' in s3params.keys():
        s3bucketregion = s3params['region_name']
    else:
        msg = "{} doesn''t exists in params".format('region_name')
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    #-----------------------------------------------------------
    #S3 Client
    #-----------------------------------------------------------

    s3client = boto3.client('s3',**s3params) 

    #-----------------------------------------------------------
    #check bucket existence
    #-----------------------------------------------------------

    try:
        s3client.head_bucket(Bucket =bucketname)
        msg = "{} bucket exist, loading file".format(bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} bucket doesn't exists, creating bucket".format(bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
        s3client.create_bucket(Bucket =bucketname, CreateBucketConfiguration ={'LocationConstraint': s3bucketregion} )
        try:
            s3client.head_bucket(Bucket =bucketname)
            msg = "{} bucket created, loading file".format(bucketname)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            #print (msg)
        except:
            msg = "{} bucket creation failed, exiting".format(bucketname)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
            sys.exit(1)

    