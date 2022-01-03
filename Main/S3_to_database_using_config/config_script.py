#module to get configuration from config/ini files 
from configparser import ConfigParser

#function to read config files and return configuration keys as dictionary
#Dictionary can be used directly to pass parameters while making connection by **dictionary_name

#file = name of the config/ini file
#section = name of the section to be used within config/ini file
def config (file,section):

    #print (file + section)
    
    #create parser
    parser= ConfigParser()
   
    #read config_file
    parser.read(file)

    #print (parser.sections())

    #get params details in this dictionary
    params_dict={}
    
    #check if seciton menitoned exists or not
    if parser.has_section(section):

        #print (parser.items(section)) 
        #this will return list of tuples where each tuple is each config key value  
        #[('aws_access_key_id', '***keyid****'), ('aws_secret_access_key', '*******SecretKey**********'), ('region', '***us/eu/east/west***')]
        params = parser.items(section)

        for param in params:
            
            #print(parm)     #('aws_access_key_id', '***keyid****')
            #print(param[0]) #aws_access_key_id
            #print(param[1]) #***keyid****

            params_dict[param[0]] = param[1]
   
    #return error if section menitoned doesn't exist
    else:
        raise Exception('Section{0} doesn''t exist in file {1}'.format(section,file))

    return params_dict