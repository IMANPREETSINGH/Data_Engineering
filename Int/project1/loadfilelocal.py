import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__),os.pardir))
sys.path.append(PROJECT_ROOT)

from config import ConfigParams
from localfile import PutFile

#-----------------------------------------------------------
#input parameters check and variable declration
#-----------------------------------------------------------
envconfigfile = 'envconfig.ini'
projectconfigfile = 'projectconfig.ini'

env = sys.argv[1]
project = sys.argv[2]

#env = 'DEV'
#project = 'cart'

#-----------------------------------------------------------
#get parameters
#-----------------------------------------------------------
envparams = ConfigParams(envconfigfile,env)
projectparams = ConfigParams(projectconfigfile,project)

filesourcepath = envparams['file_source_path']
filetargetpath = envparams['file_path']
filename = projectparams['file_name']

filearchivepath = envparams['file_archive_path']


logfilepath = envparams['log_file_path']
logfilename = 'loadfile.log'
logfile = os.path.join(logfilepath,logfilename)

#-----------------------------------------------------------
#load files from local to path provided
#-----------------------------------------------------------
PutFile(filesourcepath,filetargetpath,filearchivepath,filename,logfile)



