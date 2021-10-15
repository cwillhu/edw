from importlib_resources import files, as_file
import datetime
import pandas as pd
from edw_prototype.isd.isdlite import append_isd_data, create_and_load_stations_table, get_IDs_from_filepath, postprocess_isdlite
from edw_prototype.isd.prop import prop
from pypostgres.pypostgres import dbexecute, Connection
from pypostgres.table import create_table
import edw_prototype.test.data.isd


def test_04__postprocess_isdlite():

    isdlite_tableName = 'test__postprocess_isdlite__isdlite'
    stations_tableName = 'test__postprocess_isdlite__stations'
    schemaName = 'test'
    
    # create test stations table
    with as_file(files(edw_prototype.test.data.isd).joinpath('isd-history.test.csv')) as stations_csvfile:
        create_and_load_stations_table(stations_tableName, schemaName, csvfile=stations_csvfile)
    
    # create test isdlite table
    colNames = prop.isd_lite.colNames_orig
    colTypes = [prop.colTypeMap[colName] for colName in colNames]
    create_table(isdlite_tableName, schemaName, cols=list(zip(colNames, colTypes)))
    
    # populate isdlite table
    filename_prefixes = ['010650-99999','680700-99999','725059-14702','725066-94724']
    for fnp in filename_prefixes:
        data_resource = files(edw_prototype.test.data.isd).joinpath(fnp + '-2015.txt')
        with as_file(data_resource) as txtfile:
            print(txtfile)
            ids = get_IDs_from_filepath(txtfile)
            append_isd_data(txtfile, ids.usaf_id, ids.wban_id, tableName=isdlite_tableName, schemaName='test')

    # select cols and rows of isdlite
    postprocess_isdlite(isdlite_tableName, stations_tableName, schemaName) #currently only selects into <tablename>_temp

    # read in the table just modified
    with Connection() as c:
        df = pd.read_sql(f"select * from test.{isdlite_tableName};", c.conn)

    
    # assert that number of data rows corresponding to stations inside US is the expected value
    assert( sum(df.geoid.str.startswith('725059')) + sum(df.geoid.str.startswith('725066')) == 17490 )

    # assert that no geoids are from stations outside US
    assert( sum(df.geoid.str.startswith('010650')) + sum(df.geoid.str.startswith('680700')) == 0 )

    # check col names
    assert( list(df.columns.values) == ['temperature','dew_point','sea_level_pressure','wind_direction','wind_speed',
                                        'cloud_coverage_code','precipitation_one_hour','precipitation_six_hour',
                                        'date_local','time_local','geoid','geom'] )

    # check a data value is as expected
    assert( df.iloc[12345]['date_local'] == datetime.date(2015,5,30) )

    dbexecute(f'drop table test.{isdlite_tableName};')
    dbexecute(f'drop table test.{stations_tableName};')    
