import unittest, logging
from dynamodb_dataframes import dynamodb_sql_api2

class TestMethods(unittest.TestCase):
    dynamodb_sql_api2.setup()

    def test_1_runSqlAPI_selectwhere(self):
        try:
            print (dynamodb_sql_api2.sql("   select      *   from    table1ss where pk='1'   and   sk =  '1'"))
            print(dynamodb_sql_api2.sql("select * from table1ns where pk=   1 and sk='1' "))
            print(dynamodb_sql_api2.sql("select * from table1nn where pk=1 and sk=     1     "))
            print(dynamodb_sql_api2.sql("select * from table1ns where pk=1 and sk='1'", logging.WARN))
            print(dynamodb_sql_api2.sql("select * from table1ns where pk =  22 and sk = '  22  '"))
            print(dynamodb_sql_api2.sql("select   * from   table1_sn where p_k     =  '  2 2  '     and    s_k    = 22.2"))
        except:
            self.fail("exception occured")