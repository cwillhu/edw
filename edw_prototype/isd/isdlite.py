''' Load and postprocess isd.isd_lite and isd.stations tables '''

import re, logging, sys, time
from os import path as p
from pathlib import Path
import pandas as pd
from multiprocessing import Process
from pypostgres.config.config import Config
from pypostgres.pypostgres import dbexecute, Connection
from pypostgres.table import create_table, create_spatial_index, create_btree_index
from pypostgres.util import subproc
from edw_prototype.util.filesys import list_dir
from edw_prototype.config.config import isd_extractDir, logDir
from edw_prototype.isd.prop import prop
from edw_prototype.logger import formatter, getRotatingFileHandler

# Set up logging 
logger = logging.getLogger(__name__)
isdLogFile = Path(logDir, 'isd/isdlite.log')
isdLogFile.parent.mkdir(parents=True, exist_ok=True)
logger.addHandler( getRotatingFileHandler(isdLogFile) )

# Create schema for isd if it doesn't exist
dbexecute('create schema if not exists isd;')

# Module vars
_psql_client = Config.get('psql_client_string')
_isd_stations_csvfile = p.join(isd_extractDir, 'isd-history.csv')


def get_IDs_from_filepath(filepath): #station IDs (usaf and wban) are the first and second numbers in the filename
    ''' Parse filename and return USAF and WBAN station IDs.'''

    filepath = str(filepath)  #in case filepath is type Path
    filename = p.basename(filepath)
    pat = re.compile("([A-Z0-9]+)-([A-Z0-9]+)-") 
    res = pat.match(filename) #match() matches from beginning of string

    class ids: pass
    ids.usaf_id = res.group(1)
    ids.wban_id = res.group(2)

    return ids


def append_isd_data(txtfile, usaf_id, wban_id, tableName='isd_lite', schemaName='isd'):
    ''' Append table with data in isd format '''
    
    # Convert isd format (space-separated/fixed width) to csv and load into table via psql
    copy_command = f"cat {txtfile} | sed --regexp-extended -e 's/$/ {usaf_id} {wban_id}/' -e 's/ +/,/g' | " \
                  + _psql_client \
                  + rf'--no-psqlrc --echo-all -c "\copy {schemaName}.{tableName} from stdin with (format csv, null -9999)"'

    res = subproc(copy_command)
    if res.returnVal != 0:
        logger.error(res.text, stack_info=True)
        sys.exit(1)

        
##
#  Stations table
##
    

def create_and_load_stations_table(tableName= 'stations', schemaName='isd', csvfile=_isd_stations_csvfile):
    ''' Create and load table isd.stations (contains mapping from station id to lat/lon) '''

    logger.info(f'Create and load {schemaName}.{tableName}')    
    colNames = prop.stations.colNames_orig
    colTypes = [prop.colTypeMap[colName] for colName in colNames]
    create_table(tableName, schemaName, cols=list(zip(colNames, colTypes)))

    # skip header line when piping file to psql, replace missing values ("") with 'null'
    copy_command = f"cat {csvfile} | tail +2 | sed --regexp-extended -e 's/\\\"\\\"/null/g' | " + _psql_client \
                  + rf''' --no-psqlrc --echo-all -c "\copy {schemaName}.{tableName} from stdin with (format csv, null 'null')" '''

    res = subproc(copy_command)
    if res.returnVal != 0:
        logger.error(res.text, stack_info=True)
        sys.exit(1)

        
def postprocess_stations(tableName='stations', schemaName='isd'):
    ''' Create geoid and geom columns, and select rows from stations in U.S. '''

    logger.info(f'Postprocess {schemaName}.{tableName}')

    dbexecute(f'drop table if exists {schemaName}.{tableName}_temp;')

    # select rows and columns of stations table into a staging table
    dbexecute(f'''
               create table {schemaName}.{tableName}_temp as
                   select *, 
                       concat(usaf_station_id, '_', wban_station_id) as geoid,
                       st_setsrid(st_makepoint(longitude, latitude), 4326) as geom
                   from {schemaName}.{tableName} 
                   where end_date >= '1980-01-01'::date 
                       and country = 'US';
               ''')  #active stations have an end date of 2021-08-03

    dbexecute(f'drop table {schemaName}.{tableName};')  
    dbexecute(f"alter table {schemaName}.{tableName}_temp rename to {tableName};")

    logger.info(f'Postprocess {schemaName}.{tableName}: DONE')
        
    
##
#  Isd-lite table
##


def create_isdlite_table(tableName='isd_lite', schemaName='isd'):
    ''' Create empty table isd_lite '''

    colNames = prop.isd_lite.colNames_orig
    colTypes = [prop.colTypeMap[colName] for colName in colNames]
    create_table(tableName, schemaName, cols=list(zip(colNames, colTypes)))


def make_isdlite_file_list(rootDir=isd_extractDir, stations_tableName='stations', schemaName='isd'):

    # Get set of US stations which have data after 1979  (6023 stations)
    with Connection() as c:
        us_stations = pd.read_sql(f'''
                                select geoid from {schemaName}.{stations_tableName} 
                                    where end_date > '1980-01-01'::date and country = 'US'; 
                                ''', c.conn)
    us_stations = set(us_stations['geoid'].values)  #'in' operation faster for set than list, numpy array or dataframe

    # Get list of files to be loaded
    fileList = []
    for year in list(range(1980, 2021)):

        txtDir = p.join( rootDir, str(year) ) #directory contains .txt data files to be loaded
        txtfiles = list_dir(txtDir, pattern='*.txt')
        for txtfile in txtfiles:
            ids = get_IDs_from_filepath(txtfile)
            station = f'{ids.usaf_id}_{ids.wban_id}'
            if station in us_stations:
                fileList.append(txtfile) 

    return(fileList)  # 43940 post-1979 files from stations in US

                
def append_isdlite_table(my_spid, fileDict, tableName='isd_lite', schemaName='isd'):
    ''' Load files with a given subprocess ID into table '''

    # Get list of files for this subprocess
    my_files = [f for f, spid in fileDict.items() if spid == my_spid]

    # Append data in each file to table
    for txtfile in my_files:
        logger.info(f'spid {my_spid}: Loading file: {txtfile}')
        ids = get_IDs_from_filepath(txtfile)
        append_isd_data(txtfile, ids.usaf_id, ids.wban_id, tableName, schemaName)


def parallel_load_isdlite(fileList, num_subprocs, tableName='isd_lite', schemaName='isd'):
    ''' Launch parallel loads of isd data files into table '''

    logger.info(f"Begin parallel load of {schemaName}.{tableName}")

    # Assign subprocess ids to files in file list
    fileDict = {isdFile: i % num_subprocs + 1 for i, isdFile in enumerate(fileList)}

    # Create subprocesses
    spids = range(1, num_subprocs + 1)
    procs = [Process(target=append_isdlite_table, args=(spid, fileDict, tableName, schemaName)) for spid in spids]

    t0 = time.time() 

    # Begin execution
    for proc in procs:
        proc.start()

    # Wait for all procs to complete
    for proc in procs:
        proc.join()        
    
    logger.info(f"Elapsed time for parallel load of {schemaName}.{tableName}: {((time.time() - t0)/60):0.1f} minutes ")
    
    
def postprocess_isdlite(isdlite_tableName='isd_lite', stations_tableName='stations', schemaName='isd'):
    ''' Select colummns; set date_local, time_local, geoid, geom; select rows from stations in U.S. '''

    logger.info(f'Postprocess {schemaName}.{isdlite_tableName}')

    dbexecute(f'drop table if exists {schemaName}.{isdlite_tableName}_temp;')

    # select rows and columns of isdlite table into a staging table
    dbexecute(f'''
               create table {schemaName}.{isdlite_tableName}_temp as
                   select
                       d.temperature,
                       d.dew_point,
                       d.sea_level_pressure,
                       d.wind_direction,
                       d.wind_speed,
                       d.cloud_coverage_code,
                       d.precipitation_one_hour,
                       d.precipitation_six_hour,
                       make_date(d.year, d.month, d.day) as date_local,
                       (interval '01:00' * d.hour) as time_local,
                       concat(d.usaf_station_id, '_', d.wban_station_id) as geoid,
                       st_setsrid(st_makepoint(s.longitude, s.latitude), 4326) as geom
                   from {schemaName}.{isdlite_tableName} as d 
                   join {schemaName}.{stations_tableName} as s 
                   on d.usaf_station_id = s.usaf_station_id and d.wban_station_id = s.wban_station_id
                   where s.country = 'US' 
               ''')

    dbexecute(f'drop table {schemaName}.{isdlite_tableName};')  #drop table before assigning name to staging table
    dbexecute(f"alter table {schemaName}.{isdlite_tableName}_temp rename to {isdlite_tableName};")  #constant schema name is implied

    logger.info(f'Postprocess {schemaName}.{isdlite_tableName}: DONE')

    
def etl(schemaName='isd'):
    ''' All steps in ETL proces for ISD tables'''

    ##
    # Load and postprocess stations table
    ##
    
    stations_tableName = 'stations'
    create_and_load_stations_table(stations_tableName, schemaName)

    # Add geoid and geom columns
    postprocess_stations(stations_tableName, schemaName)  

    # Set primary key to geoid
    dbexecute(f'alter table {schemaName}.{stations_tableName} add primary key (geoid)')    

    # Create index on geom column
    create_spatial_index(stations_tableName, schemaName)

    ##
    # Load and postprocess isd_lite table
    ##

    isdlite_tableName = 'isd_lite'
    create_isdlite_table(isdlite_tableName)

    # Parallel load of isd_lite table
    isdFileList = make_isdlite_file_list(rootDir=isd_extractDir, stations_tableName=stations_tableName, schemaName=schemaName)
    parallel_load_isdlite(isdFileList, num_subprocs=32, tableName=isdlite_tableName, schemaName=schemaName)

    # Create date_local, time_local, geoid and geom cols
    postprocess_isdlite(isdlite_tableName, stations_tableName, schemaName)

    # Create btree indexes on geoid and date_local
    create_btree_index('geoid',      isdlite_tableName, schemaName, logger=logger)        
    create_btree_index('date_local', isdlite_tableName, schemaName, logger=logger)

