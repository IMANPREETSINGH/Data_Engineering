import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__),os.pardir))
sys.path.append(PROJECT_ROOT)

import pandas as pd
from datetime import datetime, timedelta 

from config import ConfigParams
from database import *


#-----------------------------------------------------------
#input parameters check and variable declration
#-----------------------------------------------------------
envconfigfile = 'envconfig.ini'
dbconfigfile = 'dbconfig.ini'
projectconfigfile = 'projectconfig.ini'

env = sys.argv[1]
project = sys.argv[2]

dbsourceenv = env + '_' + 'SOURCE'
dbtargetenv = env + '_' + 'TARGET'
#env = 'DEV'
#project = 'cart'

#-----------------------------------------------------------
#get parameters
#-----------------------------------------------------------
envparams = ConfigParams(envconfigfile,env)
dbsourceparams = ConfigParams(dbconfigfile,dbsourceenv)
dbtargetparams = ConfigParams(dbconfigfile,dbtargetenv)
projectparams = ConfigParams(projectconfigfile,project)

sourceschema = projectparams['source_schema']
sourcetable = projectparams['source_table']
sourcecolumnlist = projectparams['source_column_list']
targetschema = projectparams['target_schema']
targettable = projectparams['target_table'] 
targetcolumnlist = projectparams['target_column_list']

filename = projectparams['file_name']
filepath = envparams['file_path']


logfilepath = envparams['log_file_path']
logfilename = 'etlprocess.log'
logfile = os.path.join(logfilepath,logfilename)


#-----------------------------------------------------------
#logging
#-----------------------------------------------------------

currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
logfileobj = open(logfile, "a")
logfileobj.write("\n{}: ETL process started".format(currenttime))


#-----------------------------------------------------------
#source where condition
#-----------------------------------------------------------

maxdatequery = 'select max(date) as maxdate from {}.{}'.format(targetschema,targettable)
#print(maxdatequery)

dfdbmaxdate = GetData(query = maxdatequery, logfile= logfile, **dbsourceparams)
dfdbmaxdate.dropna(inplace=True)

if dfdbmaxdate.empty:
    msg = "No data in table {}.{}, initiating insert with current date".format(targetschema,targettable)
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    #print (msg)
    maxdate = datetime.now()  #+ timedelta(days=1)
    maxdate = maxdate.strftime('%Y-%m-%d') 
else:   
    maxdate = pd.to_datetime(dfdbmaxdate['maxdate'], format='%d-%m-%Y')
    maxdate = maxdate + timedelta(days=1)
    maxdate = maxdate.apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))[0]

#print(maxdate)

sourcewherecondition = "where date = '{}'".format(maxdate)
    
#print (sourcewherecondition)


#-----------------------------------------------------------
#source select query
#-----------------------------------------------------------
sourcequery = 'select {} from {}.{} {}'.format(sourcecolumnlist,sourceschema,sourcetable,sourcewherecondition)
dfsourcedb = GetData(query = sourcequery, logfile= logfile, **dbsourceparams)
#print (dfsourcedb)


#-----------------------------------------------------------
#get csv file data from s3 bucket
#-----------------------------------------------------------

dffiledata = pd.read_csv(os.path.join(filepath, filename))
#print(dffiledata)

#-----------------------------------------------------------
#column validation
#-----------------------------------------------------------

filecolumnlist =  dffiledata.columns

for sourcecolumn in sourcecolumnlist.split(','):
    if sourcecolumn not in filecolumnlist:
        msg = "mandatory column {} is not present in the file, exiting".format(sourcecolumn)
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

#-----------------------------------------------------------
#dataframe empty validation
#-----------------------------------------------------------
dfopenprice = dfsourcedb
dfcloseprice = dffiledata

if dfopenprice.empty and dfcloseprice.empty:
    msg = "Open and close price data for {} is not present in database and file, nothing to process, exiting".format(maxdate)
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print (msg)
    sys.exit(1)
elif dfopenprice.empty:
    msg = "Open price data for {} is not present in database, can't process data, exiting".format(maxdate)
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print (msg)
    sys.exit(1)
elif dfcloseprice.empty:
    msg = "Close price data for {} is not present in file, can't process data, exiting".format(maxdate)
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print (msg)
    sys.exit(1)

#maxdate clause on file data
maxfiledate = dffiledata['date'].max()
maxfiledate = datetime.strftime(datetime.strptime(maxfiledate,'%d/%m/%Y'),'%Y-%m-%d')

if maxdate != maxfiledate: 
    msg = "Close price data for {} is not present in file, can't process data, exiting".format(maxdate)
    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print (msg)
    sys.exit(1)
    
#-----------------------------------------------------------
#transformation process
#-----------------------------------------------------------
#print (dfopenprice)
#print (dfcloseprice)

#date column standardization    
dfopenprice['date'] = pd.to_datetime(dfopenprice['date'], format='%Y-%m-%d')
dfcloseprice['date'] = pd.to_datetime(dfcloseprice['date'], format='%d/%m/%Y')

#pricecolumn standardization
dfopenprice['price'] = dfopenprice.price.astype('float64')
dfcloseprice['price'] = dfcloseprice.price.astype('float64')

dftarget = pd.merge(dfopenprice,dfcloseprice, how = 'left', on = ['isin','date'],suffixes=('_open', '_close'),)
#print (dftarget)

#adding day, month, year 
dftarget['day'] = dftarget['date'].apply(lambda x: datetime.strftime(x, '%d'))
dftarget['month'] = dftarget['date'].apply(lambda x: datetime.strftime(x, '%m'))
dftarget['year'] = dftarget['date'].apply(lambda x: datetime.strftime(x, '%Y'))
#print (dftarget)

#ordering column based on target table column list
listtargetcolumn =  list(col for col in targetcolumnlist.split(','))
dftarget = dftarget[listtargetcolumn]
dftarget['date'] = dftarget['date'].apply(lambda x: datetime.strftime(x, '%d-%m-%Y'))
print(dftarget) 


#-----------------------------------------------------------
#target database load
#-----------------------------------------------------------

insertquery = GetInsertQuery(targetschema,targettable,listtargetcolumn,logfile)
#print (insertquery)

PutData(insertquery,dftarget, logfile, **dbtargetparams)
