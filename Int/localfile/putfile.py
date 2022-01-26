'''
File        :   putfile
Description :   to load files from one local to another
Creator     :   Imanpreet Singh 

Version     Date        Author              Description
1           26-01-2022  Imanpreet Singh

'''
from logging import exception
from datetime import datetime

import os 
import sys
import shutil
import time
import pandas as pd

#from localfile.archviefile import ArchvieFile

def PutFile(filesourcepath, filetargetpath, filearchivepath, filename, logfile):

    #-----------------------------------------------------------
    #logging
    #-----------------------------------------------------------
    if logfile == '' :
        msg = "logfile is mandatory, exiting"
        print (msg)
        sys.exit(1)

    currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
    logfileobj = open(logfile, "a")

    msg = "PutFile process started"
    logfileobj.write("\n{}: {}".format(currenttime,msg))
    print(msg)

    #-----------------------------------------------------------
    #input parameters check and variable declration
    #-----------------------------------------------------------
 
    msg = ''
    if filesourcepath == '' :
        msg = "filesourcepath is mandatory, exiting"
    elif filetargetpath == '' :
        msg = "filetargetpath is mandatory, exiting" 
    elif filename == '' :
        msg = "filename is mandatory, exiting"  

    if len(msg) != 0:
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        print (msg)
        sys.exit(1)

    #check file path existence
    if not os.path.isdir(filesourcepath):
        msg = "{} doesn't exist".format(filesourcepath)
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        print (msg)
        sys.exit(1)

    if not os.path.isdir(filetargetpath):
        msg = "{} doesn't exist".format(filetargetpath)
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        print (msg)
        sys.exit(1)  


    sourcefilefullname = os.path.join(filesourcepath,filename)
    targetfilefullname = os.path.join(filetargetpath,filename)

    #print (sourcefilefullname)
    #print (targetfilefullname)

    #-----------------------------------------------------------
    #checking file
    #-----------------------------------------------------------
    
    if filearchivepath == '': 
        msg = "Archiving Process started"
        logfileobj.write("\n{}: {}".format(currenttime,msg))
        currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
        print (msg)
        
        if not os.path.isfile(sourcefilefullname): 
            msg = "{} doesn't exist , nothing to archive".format(sourcefilefullname)
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            print (msg)
            sys.exit(1)  

        df = pd.read_csv(sourcefilefullname)
        
        if 'date' in df.columns:
            datevalue = df['date'].max()
            datevalue = datetime.strftime(datetime.strptime(datevalue,'%d/%m/%Y'), '%d%m%Y')
        else:
            datevalue = datetime.now().strftime('%d%m%Y')

        #print(datevalue)
        archivefilename = filename.split('.')[0]
        archivefilextension = filename.split('.')[1]
        archivefile = archivefilename + '_' + datevalue + '.' + archivefilextension
        archivefilefullname = os.path.join(filetargetpath,archivefile)

        if os.path.isfile(archivefilefullname): 
            msg = "{} file already exist at {}".format(archivefile,filetargetpath)
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            print (msg)
        else:
            shutil.move(sourcefilefullname, archivefilefullname)
            if os.path.isfile(archivefilefullname): 
                msg = "{} file moved to {} as {}, proceed to deletion".format(filename,filetargetpath,archivefile)
                logfileobj.write("\n{}: {}".format(currenttime,msg))
                currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
                print (msg)

    else:     

        if not os.path.isfile(sourcefilefullname): 
            msg = "{} doesn't exist in landing area".format(sourcefilefullname)
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            print (msg)
            sys.exit(1)  

        if os.path.isfile(targetfilefullname): 
            msg = "{} file already exist at {}, archiving the file".format(filename,filetargetpath)
            logfileobj.write("\n{}: {}".format(currenttime,msg))
            currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
            print (msg)

            PutFile(filetargetpath,filearchivepath,'',filename,logfile)
            time.sleep(3)
            
            shutil.move(sourcefilefullname, targetfilefullname)

            if not os.path.isfile(sourcefilefullname): 
                msg = "{} moved from landing area".format(sourcefilefullname)
                logfileobj.write("\n{}: {}".format(currenttime,msg))
                currenttime  = datetime.now().strftime('%d%m%Y_%H%M%S')
                print (msg)
                return(0)