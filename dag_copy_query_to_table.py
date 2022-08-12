# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import datetime
import pendulum
import os

# List of email address to send email alerts to if this job fails
monitoring_recipients_emails = Variable.get("monitoring_recipients_emails")
ALERT_EMAIL_ADDRESSES = [x.strip() for x in monitoring_recipients_emails.split(',')]

pgsql_source_connid = 'source_postgreql_connection'
pgsql_source_sqlfile = 'text_file_with_sql_query_path'

postfix = '_queryname'

pgsql_target_connid = 'target_postgreql_connection'
pgsql_target_tablename = 'target_pgsql_table_name'

#for example, fill with your values
primary_key = ['primary_key_column']
indexes = [
    ['index_column_1', 'index_column_2'],
    ]

default_args = {
    'start_date': pendulum.datetime(2022, 4, 8, 10, 0, tz="Europe/Moscow"),
    'owner': 'Airflow',
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
    'depends_on_past': False,
}

def get_column_types_from_query(postgres_hook, sql_query):
    
    # get column types: create temp view -> get types from it -> delete temp view
    query = f'CREATE OR REPLACE VIEW tmp_view_python{postfix} AS(\n' + sql_query
    query = query.replace(';', ');')
    postgres_hook.run(query)
    
    CUR_DIR = os.path.abspath(os.path.dirname(__file__)) 
    with open(CUR_DIR+'/sql/get_column_types.sql', "r") as file:
        get_column_types_sql = file.read()
        get_column_types_sql = get_column_types_sql.replace('tablename', f'tmp_view_python{postfix}')
    column_types = dict(postgres_hook.get_records(get_column_types_sql))    
    
    postgres_hook.run(f'DROP VIEW tmp_view_python{postfix};')
    return column_types

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
        ind_lines = ['CREATE INDEX IF NOT EXISTS ' + ''.join(ind) +\
                     f'{postfix}_idx ON '+ pgsql_target_tablename +\
                     ' (' + ', '.join(ind) + ');' for ind in indexes]
        for l in ind_lines:
            res += f'\n{l}'
    return res 

def get_data(**kwargs):
    
    import os
    import pandas as pd
    
    CUR_DIR = os.path.abspath(os.path.dirname(__file__))  
    
    with open(CUR_DIR+pgsql_source_sqlfile, "r") as file:
        query = file.read()
    
    filename = f'temp_df_{pgsql_target_tablename}.csv'
    
    #get postgres hook
    pgsql = PostgresHook(postgres_conn_id=pgsql_source_connid)
    
    column_types = get_column_types_from_query(pgsql, query)
    ti = kwargs['ti']
    ti.xcom_push(key='column_types', value=column_types)
    
    # process timestamp with timezone 
    param_dict = {col:{'utc':False} for col in column_types if 'timestamp with time zone' in column_types[col].lower()}
 
    df = pgsql.get_pandas_df(query, parse_dates = param_dict)
        
    # process int types
    need_to_convert_types = ['bigint', 'integer', 'smallint', 'smallserial', 'serial', 'bigserial']
    convert_to_int_columns = [col for col in column_types if column_types[col] in need_to_convert_types]
    for col in convert_to_int_columns:
        df[col] = df[col].apply(lambda x: '' if pd.isna(x) else str(int(x))) 
    
    print (f'Read {len(df)} records.')
    df.to_csv(filename, index = False)
   
    return filename 

def upload_data_to_posgresql(**kwargs):

    import os
      
    #get file name via XCom
    ti = kwargs['ti']
    filename = ti.xcom_pull(key=None, task_ids='get_data')
    column_types = ti.xcom_pull(key='column_types', task_ids='get_data')
    print (f'Loading file: {filename}')
               
    sql_create_table = get_create_table_statement_from_dict(column_types)

    #get postgres hook
    pgsql = PostgresHook(postgres_conn_id=pgsql_target_connid)
       
    # drop table if exists
    sql_table = 'DROP TABLE IF EXISTS ' + pgsql_target_tablename + ';'
    pgsql.run(sql=sql_table)
    
    # create table
    pgsql.run(sql=sql_create_table)
    
    # upload file to db table
    sql_copy = "COPY "+ pgsql_target_tablename +" FROM STDIN WITH CSV HEADER DELIMITER as ','"
    pgsql.copy_expert(sql = sql_copy, filename = filename)    
        
    # remove temp file
    os.remove(filename)
    return


copy_query_to_table_dag = DAG(
    'DAG_name',
    default_args = default_args,
    description = 'DAG description',
    schedule_interval = "@daily", 
    catchup=False, # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=catchup#catchup
    tags=["production", 'table'])

get_data_operator = PythonOperator(
    task_id='get_data', 
    python_callable=get_data,
    dag=copy_query_to_table_dag)

upload_data_to_posgresql_operator = PythonOperator(
    task_id='upload_data_to_posgresql', 
    python_callable=upload_data_to_posgresql,
    dag=copy_query_to_table_dag)

get_data_operator >> upload_data_to_posgresql_operator