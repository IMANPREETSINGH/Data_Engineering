'''
File        :   deletefileS3
Description :   to delete file from S3 bucket
Creator     :   Imanpreet Singh 

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''

import boto3 
import os 
import sys
from logging import exception
from datetime import datetime

def DeleteFileS3(bucketname, filename, logfile, **s3params):

    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)
        
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")

    msg = "DeleteFileS3 process started"
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
    elif filename == '' :
        msg = "filename is mandatory, exiting"  

    if len(msg) != 0:
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
        msg = "{} bucket exist, checking file existence".format(bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} bucket doesn't exists, nothing to delete".format(bucketname)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)

    #-----------------------------------------------------------
    #check file existence
    #-----------------------------------------------------------
    
    try:
        s3client.head_object(Bucket= bucketname, Key = filename)
        msg = "{} file exists in bucket {}, initiating deletion".format(bucketname, filename)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)

        try:
            s3client.delete_object(Bucket= bucketname, Key = filename)
            msg = "{} file delete from bucket {}".format(bucketname, filename)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            #print (msg)
        except:
            msg = "Error in file deletion"
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
            sys.exit(1)

    except:
        msg = "{} file doesn't exist in bucket {}, nothing to delete".format(bucketname, filename)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)

   