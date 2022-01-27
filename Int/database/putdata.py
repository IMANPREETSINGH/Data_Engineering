'''
File        :   putdata
Description :   to put data into database 
Return      :   
Creator     :   Imanpreet Singh 


Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

Assumptions:    

'''
from logging import exception
import pandas as pd
import psycopg2 as pg
import sys
import os
import numpy as np
from datetime import datetime

def PutData (query, dataframe, logfile, **dbparams):
 
    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)

    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")
    logfileobj.write("\n{}: PutData process started".format(currenttime))
    logfileobj.write("\n{}: {}".format(currenttime,query))

    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------

    if query == '' :
        msg = "query is mandatory, not able to get data, exiting"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)


    #-----------------------------------------------------------
    #Database Connection Setup 
    #-----------------------------------------------------------
    try:
        conn = pg.connect(**dbparams)  
    except:
        msg = "error in database connection setup"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)


    #-----------------------------------------------------------
    #putting data
    #-----------------------------------------------------------
    try:
        cur= conn.cursor()

        for index, row in dataframe.iterrows():
            #print (tuple(row))
            cur.execute(query,tuple(row))
        
        msg = "Data Load complete"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)

        cur.close()
        conn.commit()
        conn.close()

    except:
        msg = "error in writing data"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)
