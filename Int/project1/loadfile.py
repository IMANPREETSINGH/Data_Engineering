import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__),os.pardir))
sys.path.append(PROJECT_ROOT)


from config import ConfigParams
from awsS3 import PutFileS3

#-----------------------------------------------------------
#input parameters check and variable declration
#-----------------------------------------------------------
envconfigfile = 'envconfig.ini'
s3configfile = 's3config.ini'
projectconfigfile = 'projectconfig.ini'

env = sys.argv[1]
project = sys.argv[2]

#env = 'DEV'
#project = 'cart'

#-----------------------------------------------------------
#get parameters
#-----------------------------------------------------------
envparams = ConfigParams(envconfigfile,env)
s3params = ConfigParams(s3configfile,env)
projectparams = ConfigParams(projectconfigfile,project)

bucketname = projectparams['bucket_name'] + env.lower()
filename = projectparams['file_name']
filepath = envparams['file_path']

logfilepath = envparams['log_file_path']
logfilename = 'loadfile.log'
logfile = os.path.join(logfilepath,logfilename)

#-----------------------------------------------------------
#load files to s3
#-----------------------------------------------------------
PutFileS3(bucketname, filepath, filename,logfile, **s3params)
