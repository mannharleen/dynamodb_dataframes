# dynamodb_dataframes

## Objective

To create an easy to use API for AWS dynamodb that will enable:
1. SQL command line for SQL/DML/DDL operations  
2. SQL API for all those operations, that will return a dataframe (pandas)

## Motivation

Two simple motivations:
1. Make the dynamodb API simpler by using SQL, DML and DDL statements
2. dynamodb doesn't allow joins and other complex transformations (sometimes this is required)

## Objective

1. Creating an API that returns the data in dynamodb as a pandas dataframe. Data transformation made easy!

## Installation

### From github

This is the recommended way (unless I decided to publish this package on pypi)
Simply run the following in the command/shell prompt
```sh
pip install git+https://github.com/mannharleen/dynamodb_dataframes.git
```

## From local filesystem

This should be only used if you are not able to connect to github from the machine where you need to pip install

Download the required version file from https://github.com/mannharleen/dynamodb_dataframes/tree/master/dist and copy to the machine lets say on into the folder C:\dist\
Then run the following in the command/shell prompt
```sh
pip install dynamodb_dataframes --no-index --find-links file://C:\dist
```

## Usage

### Using the SQL API:

```python
from dynamodb_dataframes import dynamodb_sql_api

dynamodb_sql_api.dynamodb_base_api.dyanamoOps.setup()
print (dynamodb_sql_api.sql("show tables"))
                                        # return the result as object.__str()__, which can be printed
                                        # In the future, this will return a pandas dataframe
```

### Using the SQL prompt:

```sh
$ python dynamodb_sql_api.py
sql> show tables;
sql> describe table1ss;
```