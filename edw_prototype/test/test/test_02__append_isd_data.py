import tempfile
from os import path as p
import pandas as pd
from edw_prototype.isd.isdlite import append_isd_data
from pypostgres.pypostgres import dbexecute, Connection


def test_02__append_isd_data():

    tableName = 'test__append_isd_data'
    colNames = ['c1', 'c2', 'c3', 'usaf_id', 'wban_id']

    #create table for test
    colDefs = [f'{x} integer' for x in colNames]
    colDefs_str = ' ,'.join(colDefs)

    dbexecute(f'drop table if exists test.{tableName};')
    dbexecute(f'create table test.{tableName} ( {colDefs_str} );')
    
    with tempfile.TemporaryDirectory() as tmpDir:

        tmpfile = p.join(tmpDir, 'temp__copy_from_isd_txtfile.txt')
        with open(tmpfile, 'w') as t:
            t.write('92 100 3\n')
            t.write('292  0 100\n')
            t.write('10 24 613\n') 

        append_isd_data(tmpfile, usaf_id='10000', wban_id='99999', tableName=tableName, schemaName='test')
        
        # Read table just loaded from txt file
        with Connection() as c:
            df1 = pd.read_sql(f"select * from test.{tableName};", c.conn)

        rows = []
        rows.append([92, 100, 3, 10000, 99999])
        rows.append([292, 0, 100, 10000, 99999])
        rows.append([10, 24, 613, 10000, 99999])
        df1_expected = pd.DataFrame(rows)
        df1_expected.columns = colNames

        assert df1.equals(df1_expected)        

        #test that a second function call appends and does not overwrite
        append_isd_data(tmpfile, usaf_id='10002', wban_id='123456', tableName=tableName, schemaName='test')

        with Connection() as c:
            df2 = pd.read_sql(f"select * from test.{tableName};", c.conn)
        
        rows.append(rows[0][:3] + [10002, 123456])
        rows.append(rows[1][:3] + [10002, 123456])
        rows.append(rows[2][:3] + [10002, 123456])
        df2_expected = pd.DataFrame(rows)
        df2_expected.columns = colNames
         
        assert df2.equals(df2_expected)

    dbexecute(f'drop table test.{tableName};')
