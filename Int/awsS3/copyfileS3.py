'''
File        :   copyfileS3
Description :   to copy files from one s3 bucket to another s3 bucket
Creator     :   Imanpreet Singh 

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''

import boto3 
import os 
import sys
from logging import exception
from datetime import datetime

def CopyFileS3(sourcebucket, targetbucket, sourcefilename, targetfilename, logfile, **s3params):

    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)
        
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")
 
    msg = "CopyFileS3 process started"
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print(msg)


    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------
 
    msg = ''
    if len(s3params) == 0:
        msg = "s3 params are not provided, exiting"
    elif sourcebucket == '' :
        msg = "sourcebucket is mandatory, exiting"
    elif targetbucket == '' :
        msg = "targetbucket is mandatory, exiting"  
    elif sourcefilename == '' :
        msg = "sourcefilename is mandatory, exiting" 
    elif targetfilename == '' :
        msg = "targetfilename is mandatory, exiting"

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
        s3client.head_bucket(Bucket =sourcebucket)
        msg = "{} bucket exist".format(sourcebucket)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} bucket doesn't exists, exiting".format(sourcebucket)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    try:
        s3client.head_bucket(Bucket =targetbucket)
        msg = "{} bucket exist".format(targetbucket)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} bucket doesn't exists, exiting".format(targetbucket)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    #-----------------------------------------------------------
    #check file existence
    #-----------------------------------------------------------
    
    try:
        s3client.head_object(Bucket= sourcebucket, Key = sourcefilename)
        msg = "{} file exists in bucket {}".format(sourcebucket, sourcefilename)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        #print (msg)
    except:
        msg = "{} file doesn't exist in bucket {}, nothing to copy, exiting".format(sourcefilename,sourcebucket)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        return(0)

    try:
        s3client.head_object(Bucket= targetbucket, Key = targetfilename)
        msg = "{} file already exists in bucket {}, nothing to copy, exiting".format(targetfilename,targetbucket)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        return(0)

    except:
        msg = "{} file doesn't exist in bucket {}, copying file".format(targetbucket, targetfilename)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)

        #-----------------------------------------------------------
        #S3 resoruce
        #-----------------------------------------------------------
    
        awssession = boto3.session.Session(**s3params)
        s3resource = awssession.resource('s3')
        bucket = s3resource.Bucket(targetbucket)


        #-----------------------------------------------------------
        #S3 copy
        #-----------------------------------------------------------

        try:
            copysource = {'Bucket': sourcebucket, 'Key': sourcefilename}
            bucket.copy(copysource,targetfilename)
            msg = "File {} copied from bucket - {} to bucket - {} as {}".format(sourcefilename,sourcebucket,targetbucket, targetfilename)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            #print (msg)
            return(1)
        except:
            msg = "File {} copy from bucket - {} to bucket - {} as {} failed".format(sourcefilename,sourcebucket,targetbucket, targetfilename)
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            print (msg)
            sys.exit(1)
    