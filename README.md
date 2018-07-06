# dynamodb_dataframes

## Objective

To create a SQL parses for AWS dynamodb that will enable:
1. SQL command line for SQL/DML/DDL operations 
2. SQL API for all those operations

## Motivation

SQL is the mostly widely used and know language across the board. So why not empower the users and developers!

## Short term object

1. To parse simple SQL statements to create, insert and select data (without predicates, aggregations, sorting etc)

## Installation

### From github

```sh
pip install git+https://github.com/mannharleen/dynamodb_dataframes.git
```

## Usage

### Using the SQL API:

```python
from dynamodb_dataframes import dynamodb_sql_api

dynamodb_sql_api.dynamodb_base_api.dyanamoOps.setup()
print (dynamodb_sql_api.sql("show tables"))
                                        # return a string of the result, which can be printed
                                        # In the future, this will return a pandas dataframe
```

### Using the SQL prompt:

```sh
$ python dynamodb_sql_api.py
sql> show tables;
sql> describe table1ss;
```