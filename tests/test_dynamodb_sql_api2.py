import unittest, logging
from dynamodb_dataframes import dynamodb_sql_api2

class TestMethods(unittest.TestCase):
    dynamodb_sql_api2.setup()

    def test_1_runSqlAPI_selectwhere(self):
        try:
            print (dynamodb_sql_api2.sql("   select      *   from    table1ss where pk='1'   and   sk =  '1'"))
            print(dynamodb_sql_api2.sql("select * from table1ns where pk=   1 and sk='1' "))
            print(dynamodb_sql_api2.sql("select * from table1nn where pk=1 and sk=     1     "))
            print(dynamodb_sql_api2.sql("select * from table1ns where pk=1 and sk='     1     '", logging.WARN))
        except:
            self.fail("exception occured")