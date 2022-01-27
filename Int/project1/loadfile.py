#-----------------------------------------------------------
#common with all scripts
#-----------------------------------------------------------
import os,sys
projectroot = os.path.abspath(os.path.join(os.path.dirname(__file__),os.pardir))
sys.path.append(projectroot)
configpath = os.path.join(projectroot,'config')
currpath = os.path.dirname(os.path.realpath(__file__))

#-----------------------------------------------------------
#-----------------------------------------------------------
#-----------------------------------------------------------
'''
File        :   loadfilel
Description :   to load files from landing to s3 bucket and form s3 bucket to achive bucket
Creator     :   Imanpreet Singh 

Version     Date        Author              Description
1           27-01-2022  Imanpreet Singh

'''
#-----------------------------------------------------------
#-----------------------------------------------------------
#-----------------------------------------------------------

from config import ConfigParams
from awsS3 import PutFileS3


#-----------------------------------------------------------
#input parameters check and variable declration
#-----------------------------------------------------------
envconfigfile = os.path.join(configpath,'envconfig.ini')
s3configfile = os.path.join(configpath,'s3config.ini')

projectconfigfile = os.path.join(currpath,'projectconfig.ini')

env = sys.argv[1]
project = sys.argv[2]


#-----------------------------------------------------------
#get parameters
#-----------------------------------------------------------
envparams = ConfigParams(envconfigfile,env)
s3params = ConfigParams(s3configfile,env)
projectparams = ConfigParams(projectconfigfile,project)

bucketname = projectparams['bucket_name'] + env.lower()
filename = projectparams['file_name']
filepath = envparams['file_source_path']

logfilepath = envparams['log_file_path']
logfilename = 'loadfile.log'
logfile = os.path.join(logfilepath,logfilename)


#-----------------------------------------------------------
#load files to s3
#-----------------------------------------------------------
PutFileS3(bucketname, filepath, filename,logfile, **s3params)
