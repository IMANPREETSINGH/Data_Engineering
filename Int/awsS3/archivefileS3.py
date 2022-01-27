'''
File        :   archivefileS3
Description :   to archive files from one s3 bucket to another
Creator     :   Imanpreet Singh 

Assumptions:    Archive Bucket Name = env + filename + archive
                Archive file name = filename_datetime 

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''

from fileinput import filename
import boto3 
import os 
import sys
import time
from logging import exception
from datetime import datetime

from awsS3.createbucketS3 import CreateBucketS3
from awsS3.copyfileS3 import CopyFileS3
from awsS3.deletefileS3 import DeleteFileS3
from awsS3.getfiledatas3 import GetFileDatas3

def ArchiveFileS3(bucketname, filename, logfile, **s3params):

    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)
        
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")
    
    msg = "ArchiveFileS3 process started"
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print(msg)

    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------

    msg = ''
    if len(s3params) == 0:
        msg = "s3 params are not provided, exiting"
    elif bucketname == '' :
        msg = "sourcebucket is mandatory, exiting"
    elif filename == '' :
        msg = "targetbucket is mandatory, exiting"  

    if len(msg) != 0:
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    df = GetFileDatas3 (bucketname,filename,logfile,**s3params)
    
    if 'date' in df.columns:
        datevalue = df['date'].max()
        datevalue = datetime.strftime(datetime.strptime(datevalue,'%d/%m/%Y'), '%d%m%Y')
    else:
        datevalue = datetime.now().strftime('%d%m%Y')

    #print(datevalue)
    archivebucket= bucketname+'archive'
    archivefilename = filename.split('.')[0]
    archivefilextension = filename.split('.')[1]
    archivefile = archivefilename + '_' + datevalue + '.' + archivefilextension
    #print (archivefile)

    #-----------------------------------------------------------
    #archive bucket creation
    #-----------------------------------------------------------

    CreateBucketS3(archivebucket,logfile,**s3params)
    time.sleep(3)
    #-----------------------------------------------------------
    #file copy
    #-----------------------------------------------------------

    tobedeleted = CopyFileS3(bucketname, archivebucket, filename, archivefile, logfile, **s3params)
    
    #-----------------------------------------------------------
    #delete file
    #-----------------------------------------------------------
    

    if tobedeleted ==1:
        DeleteFileS3(bucketname,filename, logfile, **s3params)