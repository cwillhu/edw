from importlib_resources import files, as_file
from os import path as p
import pandas as pd
from edw_prototype.isd.isdlite import create_and_load_stations_table
from edw_prototype.isd.prop import isd_lite
from pypostgres.pypostgres import dbexecute, Connection
import edw_prototype.test.data.isd


def test_03__create_and_load_stations_table():

    tableName = 'test__create_and_load_stations_table__stations'

    # create test stations table
    with as_file(files(edw_prototype.test.data.isd).joinpath('isd-history.csv')) as stations_csvfile:
        create_and_load_stations_table(tableName, schemaName='test', csvfile=stations_csvfile)

    # read in the table just loaded
    with Connection() as c:
        df = pd.read_sql(f"select * from test.{tableName};", c.conn)

    assert( df.iloc[29577]['usaf_station_id'] == 'A51256' )
    assert( df.iloc[29577]['wban_station_id'] == '00451' )
    assert( df.iloc[29577]['latitude'] == 36.699 )
    assert( df.iloc[29577]['longitude'] == -93.402 )    

    dbexecute(f'drop table test.{tableName};')
