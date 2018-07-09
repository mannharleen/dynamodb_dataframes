from dynamodb_dataframes import dynamodb_base_api
import sys, os, logging
import pandas as pd
import regex


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s'))
logger.addHandler(ch)
logger.setLevel(logging.WARN)

def setup(**kwargs):
    dynamodb_base_api.dyanamoOps.setup(**kwargs)


def sql(sql_api_input, level=logging.WARN):
    """
    Used to take input from the API and call runSql
    """
    return runSql_API(sql_api_input, level)


def runSql_API(sql_user_input='', level=logging.WARN):
    """
    Making the sql parser more robust by using regex to parse sql statements
    """
    global logger
    logger.setLevel(level)
    inp = sql_user_input.lower()     # select..... full sql

    if sql_user_input.__contains__('"'):
        logger.exception(' SQL must not contain double quotes ". Type help if unsure.')
        raise Exception(' SQL must not contain double quotes ". Type help if unsure.')
    else:
        if inp.split()[0] == 'select':
            inp_left_from = inp.split('from')[0]  # select...
            inp_right_from = inp.split('from')[1]
            if inp.__contains__('where'):
                inp_left_where = inp_right_from.split('where')[0]  # table1
                inp_right_where = inp_right_from.split('where')[1]
            else:
                inp_left_where = inp_right_from
                inp_right_where = ''
            #
            logger.info(" Table {} selecting using sql_api2".format(inp_left_where.strip()))
            #
            rx_cols = regex.match(r'\s*(select)\s+(\s*(\*|\w+)\s*,?)+', inp_left_from)
            l_cols = rx_cols.captures(3)
            #
            rx_table = regex.match(r'\s*(\w+)\s*', inp_left_where)
            l_tables = rx_table.captures(1)
            if inp_right_where != '':
                rx_where = regex.findall(r"""\s*(([\w-_]*)\s*=\s*(('?).+?(\4))\s*)""", inp_right_where)
                l_predicate_key = [x[1] for x in rx_where]
                l_predicate_value = [x[2] for x in rx_where]
                l_predicates = []
                for i in range(0, len(l_predicate_key)):
                    l_predicates.append(l_predicate_key[i])
                    l_predicates.append(l_predicate_value[i])
            else:
                l_predicates = {}
                   # todo -- else: !!! cater for select <col1> from...
            l_parsed_text = [inp_left_where.strip(), 'select', ','.join(l_predicates)]
            try:
                returned_rows = dynamodb_base_api.run(l_parsed_text, level)
                if isinstance(returned_rows, dict):
                    returned_rows = [returned_rows]     # convert dict to list of dict for easy pandas conversion
                return pd.DataFrame( returned_rows)
            except:
                logger.exception(" Unable to understand input. Type help if unsure.")
        else:
            logger.warning('Use v2 api for select only')

