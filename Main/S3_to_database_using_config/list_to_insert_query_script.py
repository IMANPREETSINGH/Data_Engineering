'''
File        :   list_to_insert_query_script
Description :   return insert query based on the tableName, columnName and columnList
Creator     :   Imanpreet Singh 


Version     Date        Author              Description
1           01-01-2022  Imanpreet Singh

'''

# to create Insert query based on input parameters
def list_to_insert_query (columnList, schemaName, tableName): 
    
    #initial Insert query
    insert_query = 'Insert into ' + schemaName + '.' + tableName + '( '
    value_query = ''

    #concatenate columns to insert query
    for index, col in enumerate(columnList):
        if index == 0 :
            insert_query = insert_query + col
            value_query = value_query + '%s'
        else:
            insert_query = insert_query + ',' + col
            value_query = value_query + ',%s'

    #suffix to insert query
    insert_query = insert_query + ' ) values (' + value_query + ')'

    return insert_query