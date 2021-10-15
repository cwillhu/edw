from pypostgres.pypostgres import psql_command
import re

# test db connection via psql_command()
def test_01__psql_command():
    command='select * from etl.edw_download limit 3;'
    res = psql_command(command)

    assert re.search(r'\(3 rows\)', res.text)
    assert res.returnVal == 0
