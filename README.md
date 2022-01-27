Please read this file before starting. 

##Files

	This folder contains all files and folders needed for testing.
	This is as per the default config files and not needed if someone using this over its own setup.
	However, below number of folder are needed in order to correctly run the script.

		DEV

			Landing --> Files from the source will be put here
			Files	--> File which will be processed and loaded into database. It is fetched from Landing area.
			Archive --> Before adding any new file to Files folder, old file will be archived in this folder with suffix as date of records it is containing.
			Logs 	--> Contains log files


##Int

	This folder contains all the internally created packages and modules needed for the processing.

		awsS3		--> This package contains all the modules related S3 processing		
		config		--> This package contains module to process config files and return configuration values as dictionary.
				    This folder also contains environment, s3 and database config files	
		database	--> This package contains all the modules related to database processing
		localfile	--> This package contains all the modules related to local file processing
		project1	--> This folder is project/user specific.
					It contains config file specific to project 
					This folder will contain script to load files and for etl processing using above mentioned modules. 

	