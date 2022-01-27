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
File        :   loadfilelocal
Description :   to load files from landing to processign and form processing to archive
Creator     :   Imanpreet Singh 

Version     Date        Author              Description
1           27-01-2022  Imanpreet Singh

'''
#-----------------------------------------------------------
#-----------------------------------------------------------
#-----------------------------------------------------------

from config import ConfigParams
from localfile import PutFile

#-----------------------------------------------------------
#input parameters check and variable declration
#-----------------------------------------------------------
envconfigfile = os.path.join(configpath,'envconfig.ini')
projectconfigfile = os.path.join(currpath,'projectconfig.ini')

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



