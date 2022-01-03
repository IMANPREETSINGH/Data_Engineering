import pandas as pd 

#udfs 
from get_s3_csv_file_script import get_s3_csv_file
from load_df_to_db_script import load_df_to_db



fileName = 'metrics.csv'
bucketName = 's3-bucket-file-load-to-pgdb'
schemaName = 'aws_s3'
tableName = 'metrics'
configFile = 'config.ini'

#def loads3todb (fileName, bucketName, schemaName, tableName, configFile):

####################### AWS S3 Connection and Load  ###################### 

df_csv = get_s3_csv_file(fileName, bucketName, configFile, 'aws_s3')

####################### AWS S3 Connection and Load  ###################### 


####################### Tranformation can be done here - specific to files   ######################

print ('data cleanup/transformation started')

#data cleanup - converting all name to lower 
df_csv['name'] = df_csv['name'].str.lower()

print ('data cleanup/transformation completed')

####################### Tranformation can be done here  - specific to files    ######################


####################### Database Load Queries   ######################

load_df_to_db(schemaName, tableName, configFile, 'postgresql', df_csv)

####################### Database Load Queries   ######################


 


