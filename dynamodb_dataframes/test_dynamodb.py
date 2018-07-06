import unittest, time
from dynamodb_dataframes import dynamodb_sql_api

class TestMethods(unittest.TestCase):


    dynamodb_sql_api.dynamodb_base_api.dyanamoOps.setup()

    def test_1_runDynamoAPI_dropTables(self):
        try:
            l_tables = dynamodb_sql_api.dynamodb_base_api.run(['list'])
            if (l_tables != None ):
                l_tables = [dynamodb_sql_api.dynamodb_base_api.run([x, 'drop']) for x in l_tables]
            print (str(l_tables) + ' dropped')
        except:
            self.fail("exception occured")

    def test_2_runDynamoAPI_listTables(self):
        try:
            print( 'list of table = \n' + str(dynamodb_sql_api.dynamodb_base_api.run(['list'])))
        except:
            self.fail("exception occured")

    def test_3_runDynamoAPI_createTable(self):
        try:
            print (str(dynamodb_sql_api.dynamodb_base_api.run(['table1ss', 'create', '2,pk,S,sk,S'])) + ' created')
            print(str(dynamodb_sql_api.dynamodb_base_api.run(['table1ns', 'create', '2,pk,N,sk,S'])) + ' created')
            print(str(dynamodb_sql_api.dynamodb_base_api.run(['table1nn', 'create', '2,pk,N,sk,N'])) + ' created')
        except:
            self.fail("exception occured")

    def test_4_runDynamoAPI_insertTables(self):
        try:
            print(str(
                dynamodb_sql_api.dynamodb_base_api.run(
                    ['table1ss', 'insert', "pk,'1',sk,'1',col1,'val1'"])) + ' inserted values 1 1 val1')
            print(str(
                dynamodb_sql_api.dynamodb_base_api.run(
                    ['table1ss', 'insert', "pk,'2',sk,'2',col1,'val2'"])) + ' inserted values 1 1 val1')
            print(str(
                dynamodb_sql_api.dynamodb_base_api.run(['table1ns', 'insert', "pk,1,sk,'1',col1,'val1'"])) + ' inserted values 1 1 val1')
            print(str(
                dynamodb_sql_api.dynamodb_base_api.run(['table1nn', 'insert', "pk,1,sk,1,col1,1000"])) + ' inserted values 1 1 1000')
        except:
            self.fail("exception occured")

    def test_5_runSqlAPI_help(self):
        try:
            dynamodb_sql_api.sql('help')
        except:
            self.fail("exception occured")

    def test_6_runSqlAPI_showtables(self):
        try:
            print (dynamodb_sql_api.sql('show tables'))
        except:
            self.fail("exception occured")

    def test_7_runSqlAPI_select(self):
        try:
            print (dynamodb_sql_api.sql('select * from table1ss'))
            print(dynamodb_sql_api.sql('select * from table1ns'))
            print(dynamodb_sql_api.sql('select * from table1nn'))
        except:
            self.fail("exception occured")

    def test_8_runSqlAPI_selectwhere(self):
        try:
            print (dynamodb_sql_api.sql("select * from table1ss where pk='1' and sk='1'"))
            print(dynamodb_sql_api.sql("select * from table1ns where pk=1 and sk='1'"))
            print(dynamodb_sql_api.sql("select * from table1nn where pk=1 and sk=1"))
        except:
            self.fail("exception occured")

    def test_90_runSqlAPI_list(self):
        try:
            print(dynamodb_sql_api.sql('show tables'))
        except:
            self.fail("exception occured")



    def test_91_runDynamoAPI_describe(self):
        try:
            print (dynamodb_sql_api.dynamodb_base_api.run(['table1ss' , 'describe']))
            print(dynamodb_sql_api.dynamodb_base_api.run(['table1ns', 'describe']))
            print(dynamodb_sql_api.dynamodb_base_api.run(['table1nn', 'describe']))
        except:
            self.fail("exception occured")

    def test_92_runSqlAPI_insert_with_keys(self):
        try:
            print (str(dynamodb_sql_api.sql("insert into table1ss (pk,sk) values ('2','22')")) + ' inserted data')
            print(str(dynamodb_sql_api.sql("insert into table1ns (pk,sk) values (2,'22')")) + ' inserted data')
        except:
            self.fail("exception occured")

    def test_93_runSqlAPI_insert_with_keys_and_columns(self):
        try:
            print(str(
                dynamodb_sql_api.sql("insert into table1ss (pk,sk,col1) values ('2','22',222)")) + ' inserted data')
            print(str(
                dynamodb_sql_api.sql("insert into table1nn (pk,sk,col1) values (2,22,222)")) + ' inserted data')
        except:
            self.fail("exception occured")

    def test_94_runSqlAPI_SQLwithDoubleQuotes(self):
        with self.assertRaises(Exception):
            dynamodb_sql_api.sql('select * from table1ss where pk = "1"')

    def test_94_runSqlAPI_describeTable(self):
        try:
            print(dynamodb_sql_api.sql('describe table1ss'))
        except:
            self.fail("exception occured")

