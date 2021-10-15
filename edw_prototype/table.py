from os import path as p
from pypostgres.util import SubbableStr, get_formattedNames
from pypostgres.pypostgres import dbexecute, psql_command
from pypostgres.table import cast_to_new_table


def castTable(tableName, schemaName, properties, logger=None):
    ''' Cast columns to new table '''
    
    if logger: logger.info(f'Casting {schemaName}.{tableName}')

    colNames = getattr(properties, tableName).colNames_orig
    oldColTypes = ['varchar'] * len(colNames)
    newColTypes = [properties.colTypeMap[colName] for colName in colNames]
    newTableName = f'{tableName}__temp'
    cast_to_new_table(tableName, newTableName, schemaName=schemaName, cols=list(zip(colNames, oldColTypes, newColTypes)))

    dbexecute(f'drop table {schemaName}.{tableName};')
    dbexecute(f'alter table {schemaName}.{newTableName} rename to {tableName};')
    
    if logger: logger.info(f'Casting {schemaName}.{tableName}: DONE')
