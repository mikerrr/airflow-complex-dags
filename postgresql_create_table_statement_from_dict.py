# -*- coding: utf-8 -*-

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

pgsql_source_connid = 'posgresql_connection'
pgsql_target_tablename = 'target_pgsql_table'
postfix = '_queryname'

#for example, fill with your values
primary_key = ['primary_key_column']
indexes = [
    ['index_column_1', 'index_column_2'],
    ]

filename = 'tamp_csv_file_from_query.csv'

# make CREATE TABLE statement from column_types dict obtained from  get_column_types_from_query() function

def get_create_table_statement_from_dict(column_types):
    
    global primary_key
    global indexes
    global postfix
    res = f'CREATE TABLE {pgsql_target_tablename}\n'
    res += '(\n'
    lines = [f'\t{col} {column_types[col]}' for col in column_types]
    # primary key
    res += ',\n'.join(lines)
    if len(primary_key)>0: 
        res += ',\n\tPRIMARY KEY (' + ', '.join(primary_key) + ')'

    res += '\n);'
    res += '\nGRANT SELECT ON TABLE ' + pgsql_target_tablename + ' TO analytic;'
    if len(indexes)>0: 
        ind_lines = [f'CREATE INDEX IF NOT EXISTS ' + ''.join(ind) +\
                     f'{postfix}_idx ON '+ pgsql_target_tablename +\
                     ' (' + ', '.join(ind) + ');' for ind in indexes]
        for l in ind_lines:
            res += f'\n{l}'
    return res 

'''
USAGE under Airflow:
'''

query = ... #SQL query

pgsql = PostgresHook(postgres_conn_id=pgsql_source_connid)

column_types = get_column_types_from_query(pgsql, query) # see other .py file in repository

# process timestamp with timezone 
param_dict = {col:{'utc':False} for col in column_types if 'timestamp with time zone' in column_types[col].lower()}
 
df = pgsql.get_pandas_df(query, parse_dates = param_dict)
    
# process int types
need_to_convert_types = ['bigint', 'integer', 'smallint', 'smallserial', 'serial', 'bigserial']
convert_to_int_columns = [col for col in column_types if column_types[col] in need_to_convert_types]
for col in convert_to_int_columns:
    df[col] = df[col].apply(lambda x: '' if pd.isna(x) else str(int(x))) 

df.to_csv(filename, index = False)

sql_create_table = get_create_table_statement_from_dict(column_types)

# create table
pgsql.run(sql=sql_create_table)

# upload file to db table
sql_copy = "COPY "+ pgsql_target_tablename +" FROM STDIN WITH CSV HEADER DELIMITER as ','"
pgsql.copy_expert(sql = sql_copy, filename = filename)      #filename is a CSV file
    
# remove temp file
os.remove(filename)