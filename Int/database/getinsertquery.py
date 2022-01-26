'''
File        :   getinsertquery
Description :   to generate insert query
Return      :   string
Creator     :   Imanpreet Singh 


Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

Assumptions:    

'''
from logging import exception
from datetime import datetime
import sys

def GetInsertQuery (schemaname, tablename, columnlist, logfile):
    
    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)

    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")
    logfileobj.write("\n{}: GetInsertQuery process started".format(currenttime))

    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------

    if tablename == '' :
        msg = "tablename is mandatory, not able to create insert script, exiting"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    if schemaname == '' :
        msg = "schemaname is mandatory, not able to create insert script, exiting"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    if columnlist == '' :
        msg = "columnlist is mandatory, not able to create insert script, exiting"
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    #print (schemaname, tablename, columnlist,logfile)

    #-----------------------------------------------------------
    #Insert query creation
    #-----------------------------------------------------------

    insert_query = "Insert into {}.{} (".format(schemaname,tablename)
    value_query = ''

    #concatenate columns to insert query
    for index, col in enumerate(columnlist):
        if index == 0 :
            insert_query = insert_query + col
            value_query = value_query + '%s'
        else:
            insert_query = insert_query + ',' + col
            value_query = value_query + ',%s'

    #suffix to insert query
    insert_query = insert_query + ' ) values (' + value_query + ')'

    return insert_query


