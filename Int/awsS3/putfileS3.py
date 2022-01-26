'''
File        :   putfileS3
Description :   to load files from local to s3
Creator     :   Imanpreet Singh 

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''

import boto3 
import os 
import sys
from logging import exception
from datetime import datetime

from awsS3.createbucketS3 import CreateBucketS3
from awsS3.archivefileS3 import ArchiveFileS3

def PutFileS3(bucketname, filepath, filename, logfile, **s3params):

    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)
        
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")

    msg = "PutFileS3 process started"
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
  
    #check file path existence
    if not os.path.isdir(filepath):
        msg = "{} doesn't exist".format(filepath)
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        print (msg)
        sys.exit(1)
    
    fullfilename = os.path.join(filepath,filename)

    #-----------------------------------------------------------
    #Bucket creation
    #-----------------------------------------------------------

    CreateBucketS3(bucketname,logfile,**s3params)

    #-----------------------------------------------------------
    #S3 Client
    #-----------------------------------------------------------

    s3client = boto3.client('s3',**s3params) 

    try:
        s3client.head_object(Bucket= bucketname, Key = filename)
        msg = "{} file exists in bucket {}, archiving the file".format(filename, bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
       
        ArchiveFileS3(bucketname, filename, logfile, **s3params)

        s3client.upload_file(Bucket = bucketname, Key = filename, Filename = fullfilename)

        try:
            s3client.head_object(Bucket= bucketname, Key = filename) 
            msg = "{} file loaded in bucket {}".format(filename, bucketname)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
    
        except:
            msg = "{} file load to bucket {} failed, exiting".format(filename, bucketname)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
            sys.exit(1)

    except:
        s3client.upload_file(Bucket = bucketname, Key = filename, Filename = fullfilename)
        try:
            s3client.head_object(Bucket= bucketname, Key = filename) 
            msg = "{} file loaded in bucket {}".format(filename, bucketname)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
    
        except:
            msg = "{} file load to bucket {} failed, exiting".format(filename, bucketname)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
            sys.exit(1)
    
