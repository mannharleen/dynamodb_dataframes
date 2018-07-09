from dynamodb_dataframes import dynamodb_base_api
import sys, os, logging, re
import pandas as pd


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s'))
logger.addHandler(ch)
logger.setLevel(logging.INFO)


def setup(**kwargs):
    dynamodb_base_api.dyanamoOps.setup(**kwargs)


def setupREPL():
    print('## Enter values or simply press enter to use default values ##')
    region_name = {'region_name': input('Enter region_name ')}
    aws_access_key_id = {'aws_access_key_id': input('Enter aws_access_key_id ')}
    aws_secret_access_key = {'aws_secret_access_key': input('Enter aws_secret_access_key ')}
    endpoint_url = {'endpoint_url': input('Enter endpoint_url ')}
    dict_kwargs = {**region_name, **aws_access_key_id, **aws_access_key_id, **endpoint_url}
    dict_nonEmpty_kwargs = {k:v for k,v in dict_kwargs.items() if v != ''}
    dynamodb_base_api.dyanamoOps.setup(**dict_nonEmpty_kwargs)
    os.system('cls')
    #os.system('clear')


def runCommand():
    user_input = input('Type command [or help or exit] here \n')
    if user_input != 'exit':
        dynamodb_base_api.run(user_input.split())
    return user_input


def runSqlHelp():
    print("""
        - to list tables
                'show tables' to list tables
        - to select all items from tables
                'select * from <table>'
                'select * from <table> where k1=1 and k2 = 2'
        - to insert values
                'insert into <table> (k1,k2,k3) values ('v1', v2, v3) 
        - to exit
                'exit' 
        - to view help
                'help'
    """)


def runSqlREPL():
    """
    Used to take input from the user on the sql> prompt and call runSql
    """
    sql_user_input = input('sql> ')
    print(sql(sql_user_input))
    return (sql_user_input)


def sql(sql_api_input):
    """
    Used to take input from the API and call runSql
    """
    # singleq_replaced_by = "'!@#"
    # space_replaced_by = '@#$'
    # l_sql_api_input_splitsq = sql_api_input.replace("'", singleq_replaced_by).split("'")
    # sql_api_input_catered = ''.join(list(map(lambda x: x.replace(' ', space_replaced_by).replace(singleq_replaced_by[1:], "'") if x.__contains__(singleq_replaced_by[1:]) else x, l_sql_api_input_splitsq)))
    # print (sql_api_input)
    # print(sql_api_input_catered)
    # l_sql_api_input = sql_api_input_catered.split()
    l_sql_api_input = sql_api_input.split()        #original works
    return runSql_API(l_sql_api_input)


def runSql_API(l_sql_user_input=[]):
    l_sql_user_input = [x.lower().strip() for x in l_sql_user_input]

    if any(x.__contains__('"') for x in l_sql_user_input) :
        logger.exception(' SQL must not contain double quotes ". Type help if unsure.')
        raise Exception(' SQL must not contain double quotes ". Type help if unsure.')
    else:
        if l_sql_user_input.__contains__('help'):   # help
            runSqlHelp()
        elif l_sql_user_input[0] == 'show' and l_sql_user_input[1] == 'tables': # show tables
            return dynamodb_base_api.run(['list'])
        elif l_sql_user_input[0] == 'describe' and len(l_sql_user_input) == 2: # describe table1ss
            return dynamodb_base_api.run([l_sql_user_input[1], 'describe'])
        elif l_sql_user_input[0] == 'select' and l_sql_user_input[2] == 'from': # select
            l_parsed_text = []
            if l_sql_user_input[1] == '*':
                if len(l_sql_user_input) == 4:   # select * from table1
                    l_parsed_text = [l_sql_user_input[3], 'select']
                else:                                                           # select * from table1 where a='x' and b=2
                    predicates = ''.join(l_sql_user_input[5:]).replace(' ','')               # a='x'andb=2
                    parsed_predicates = predicates.replace('=', ',').replace('and', ',')            # a,1,b,2
                    l_parsed_text = [l_sql_user_input[3], 'select', parsed_predicates]
            # todo -- else: !!! cater for select <col1> from...
            try:
                returned_rows = dynamodb_base_api.run(l_parsed_text)
                if isinstance(returned_rows, dict):
                    returned_rows = [returned_rows]     # convert dict to list of dict for easy pandas conversion
                return pd.DataFrame( returned_rows)
            except:
                logger.exception(" Unable to understand input. Type help if unsure.")
        elif l_sql_user_input[0] == 'insert' and l_sql_user_input[1] == 'into' and l_sql_user_input[4] == 'values': # insert into table1 (pk,sk,col1) values ('a','b','c')
            l_parsed_text = []
            l_cols = [x.strip() for x in l_sql_user_input[3].replace('(','').replace(')','').split(',')]
            l_vals = [x.strip() for x in l_sql_user_input[5].replace('(','').replace(')','').split(',')]
            l_parsed_col_vol = []
            for x in range(0,len(l_cols)):
                l_parsed_col_vol.append(l_cols[x])
                l_parsed_col_vol.append(l_vals[x])
            parsed_col_vol = ','.join(l_parsed_col_vol)
            l_parsed_text = [l_sql_user_input[2], 'insert', parsed_col_vol]
            try:
                return dynamodb_base_api.run(l_parsed_text)
            except:
                logger.exception(" Unable to understand input. Type help if unsure.")
        else:
            logger.warning('Unable to understand the input. Type help if unsure.')


def run(argv):
    setupREPL()
    #dynamodb_base_api.dyanamoOps.setup()
    user_input = ''
    while user_input != 'exit':
        user_input = runSqlREPL()

if __name__ == '__main__':
    run(sys.argv[1:])