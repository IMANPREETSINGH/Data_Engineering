'''
File        :   load_df_to_db_script
Description :   load any data frame to database tables
Creator     :   Imanpreet Singh 


Version     Date        Author              Description
1           01-01-2022  Imanpreet Singh

'''

#postgres related modules
import psycopg2 as ps

#postgres related modules
import pandas as pd 

#config.py created by me, get config function from it
from config_script import config
from list_to_insert_query_script import list_to_insert_query



def load_df_to_db (schemaName, tableName, configFile, configSection, dataFrame):

    ####################### Dynamic Insert query   ######################

    #get columns' name from data frame as list
    column_list = dataFrame.columns

    #call udf to get insert query
    insert_query = list_to_insert_query(column_list,schemaName,tableName)
    print (insert_query)


    ####################### Database Connection Setup   ######################

    print ('Establishing connection to database')

    #establish  connection 

    #get config params
    params_db = config(file=configFile,section=configSection)

    #connect to db
    conn = ps.connect(**params_db)

    #create cursor
    cur = conn.cursor()

    ####################### Database Connection Setup   ######################




    ####################### Database Load Queries   ######################

    """ 
    Database Query to create schema and table 
            
        create schema if not exists aws_s3;

        drop table if exists aws_s3.metrics;

        create table if not exists aws_s3.metrics
        (time  timestamp,
        name text,
        value numeric(26,12),
        insertTS timestamp default current_timestamp
        );

    """
    #truncating and loading into table aws_s3.metrics

    print ('Truncating Table')

    truncate_query = 'TRUNCATE TABLE ' + schemaName + '.' + tableName
    cur.execute(truncate_query)

    print ('Loading data from data frame to Table')

    for index, row in dataFrame.iterrows(): 
        cur.execute(insert_query,tuple(row))

    print ('Data Load completed')

    ####################### Database Load Queries   ######################


    #close cursor and commit
    cur.close()
    conn.commit()



