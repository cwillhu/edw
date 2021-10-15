import csv, re, subprocess, time, logging
from os import path as p
from pathlib import Path
from multiprocessing import Process
from pypostgres.pypostgres import dbexecute
from pypostgres.table import create_table, append_csv, create_spatial_index, create_btree_index
from edw_prototype.util.filesys import list_dir, extract_zip
from edw_prototype.config.config import epa_downloadDir, epa_extractDir, logDir
from edw_prototype.epa.prop import prop
from edw_prototype.logger import formatter, getRotatingFileHandler

# Set up logging 
logger = logging.getLogger(__name__)
epaLogDir = Path(logDir, 'epa')
epaLogFile = Path(epaLogDir, 'epa.log')
epaLogFile.parent.mkdir(parents=True, exist_ok=True)
logger.addHandler( getRotatingFileHandler(epaLogFile) )


#get dir prefix that corresponds to table name
def dir_prefix(tableName):
    if re.search('^(eight|8)', tableName):
        return '8hour'
    elif re.search('^daily', tableName):
        return 'daily'
    elif re.search('^annual', tableName):
        return 'annual'
    else:
        raise ValueError(f'Unknown value: {tableName}')

    
def create_airquality_table(tableName, schemaName='epa', logger=logger):
    ''' Create and load an EPA air quality table (annual, daily or eight_hour) '''

    colNames = getattr(prop, tableName).colNames_orig
    colTypes = [prop.colTypeMap[colName] for colName in colNames]
    create_table(tableName, schemaName, cols=list(zip(colNames, colTypes)), logger=logger)


def create_airquality_table_filelist(tableName):    
    ''' Extract zips and create list of files to be loaded '''

    fileList = []
    
    # Directories containing zip files to be extracted:
    dirPrefix = dir_prefix(tableName)
    zdirs = list_dir(epa_downloadDir, pattern=f'{dirPrefix}_*',  returnDirectories=True, returnFiles=False)

    for zdir in zdirs:
        zdir_basename = p.basename(zdir)
        zip_extractDir = p.join(epa_extractDir, zdir_basename)

        zips = list_dir(zdir, pattern='*.zip')
        for zipfile in zips:
            csvfile_name = re.sub(r'\.zip$', '.csv', p.basename(zipfile), flags=re.IGNORECASE)
            csvfile = p.join(zip_extractDir, csvfile_name)

            if not p.isfile(csvfile):
                logger.info(f'Extracting {zipfile}')
                extract_zip(zipfile, destinationDir=zip_extractDir)

            # Append file to file list
            fileList.append(csvfile)

    return(fileList)


def append_airquality_table(my_spid, fileDict, tableName, schemaName='epa'):
    ''' Load files with a given subprocess ID into table '''

    # Get list of files for this subprocess
    my_files = [f for f, spid in fileDict.items() if spid == my_spid]

    # Append data in each file to table
    for csvfile in my_files:
        logger.info(f'Table {schemaName}.{tableName}: spid {my_spid}: Loading file: {csvfile}')
        append_csv(tableName, schemaName, csvfile, header=True)

        
def parallel_load_airquality_table(fileList, num_subprocs, tableName, schemaName='epa', logger=logger):
    ''' Launch parallel loads of epa data files into table '''

    logger.info(f"Begin parallel load of {schemaName}.{tableName}")

    # Assign subprocess ids to files in file list
    fileDict = {epaFile: i % num_subprocs + 1 for i, epaFile in enumerate(fileList)}
    with open(Path(epaLogDir, 'fileList'), 'w') as f:  #log fileList
        print(fileDict, file=f)
    
    # Create subprocesses
    spids = range(1, num_subprocs + 1)
    procs = [Process(target=append_airquality_table, args=(spid, fileDict, tableName, schemaName)) for spid in spids]

    t0 = time.time() 

    # Begin execution
    for proc in procs:
        proc.start()

    # Wait for all procs to complete
    for proc in procs:
        proc.join()        
    
    logger.info(f"Elapsed time for parallel load of {schemaName}.{tableName}: {((time.time() - t0)/60):0.1f} minutes ")

            
def create_and_load_sites_table(tableName='sites', schemaName='epa', logger=logger):
    ''' Create and load EPA sites table (contains zipcodes for sites) '''
    
    colNames = prop.sites.colNames_orig
    colTypes = [prop.colTypeMap[colName] for colName in colNames]
    create_table(tableName, schemaName, cols=list(zip(colNames, colTypes)), logger=logger)

    csvfile = Path(epa_extractDir, 'site/aqs_sites/aqs_sites.csv')
    append_csv(tableName, schemaName, csvfile, header=True, logger=logger)    

    
##
# Postprocessing
##

        
def create_geoid_and_geom_cols(tableName, schemaName='epa', logger=logger):
    ''' Create geoid and geom columns using CREATE TABLE...AS SELECT '''
    
    logger.info(f'{schemaName}.{tableName}: Create geoid and geom columns')

    origColNames = getattr(prop, tableName).colNames_orig
    
    if tableName == 'sites':
        site_col = 'site_number'
    else:
        site_col = 'site_num'

    # select columns into staging table, then rename table
    dbexecute(f'drop table if exists {schemaName}.{tableName}_temp;')
    dbexecute(f'''
               create table {schemaName}.{tableName}_temp as
                   select *, 
                       concat(state_code, '_', county_code, '_', {site_col}) as geoid,
                       st_setsrid(st_makepoint(longitude, latitude), 4326) as geom       
                   from {schemaName}.{tableName}
               ''')
                       
    dbexecute(f"drop table {schemaName}.{tableName};")
    dbexecute(f"alter table {schemaName}.{tableName}_temp rename to {tableName};")

    logger.info(f'{schemaName}.{tableName}: Create geoid and geom columns: DONE')    
    
    
def etl(schemaName='epa'):
    ''' All steps in EPA ETL '''

    ##
    # Load and postprocess sites table
    ##

    sites_tableName = 'sites'
    create_and_load_sites_table(sites_tableName, schemaName)
    create_geoid_and_geom_cols(sites_tableName, schemaName)

    # Set primary key on sites table
    dbexecute(f'alter table {schemaName}.{sites_tableName} add primary key (geoid)')

    # Create indexes on sites table
    create_spatial_index(sites_tableName, schemaName, logger=logger)
    create_btree_index('zip_code', sites_tableName, schemaName, logger=logger)        

    ##
    # Load and postprocess air quality tables
    ##

    # Create and load air quality tables
    for tableName in ['annual', 'daily', 'eight_hour']:
        create_airquality_table(tableName, schemaName)
        fileList = create_airquality_table_filelist(tableName)
        parallel_load_airquality_table(fileList, num_subprocs=32, tableName=tableName, schemaName=schemaName)
    
    # Postprocess air quality tables        
    for tableName in ['annual', 'daily', 'eight_hour']:
        create_geoid_and_geom_cols(tableName, schemaName)    
        create_spatial_index(tableName, schemaName, logger=logger)

        create_btree_index('geoid',         tableName, schemaName, logger=logger)        
        create_btree_index('parameter_name', tableName, schemaName, logger=logger)
    
        if tableName == 'annual':
            create_btree_index('year', tableName, schemaName, logger=logger)
        else:
            create_btree_index('date_local', tableName, schemaName, logger=logger)


def annual(schemaName='epa'):
    ''' All steps in ETL for annual table only '''

    tableName='annual'
    create_airquality_table(tableName, schemaName)
    fileList = create_airquality_table_filelist(tableName)
    parallel_load_airquality_table(fileList, num_subprocs=32, tableName=tableName, schemaName=schemaName)

    # post-process table
    create_geoid_and_geom_cols(tableName, schemaName)    
    create_spatial_index(tableName, schemaName, logger=logger)
    create_btree_index('geoid',          tableName, schemaName, logger=logger)        
    create_btree_index('parameter_name', tableName, schemaName, logger=logger)
    create_btree_index('year',           tableName, schemaName, logger=logger)

            
