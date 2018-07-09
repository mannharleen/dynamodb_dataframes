from dynamodb_dataframes import dynamodb_sql_api2

dynamodb_sql_api2.setup()

print(dynamodb_sql_api2.sql("select * from table1ss"), end='\n\n')
print(dynamodb_sql_api2.sql("select * from table1ss where pk='1' and sk='1' "), end='\n\n')
