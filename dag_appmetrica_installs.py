# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import datetime
import pendulum

# List of email address to send email alerts to if this job fails
monitoring_recipients_emails = Variable.get("monitoring_recipients_emails")
ALERT_EMAIL_ADDRESSES = [x.strip() for x in monitoring_recipients_emails.split(',')]

appmetrica_authorization_key = Variable.get("appmetrica_authorization")
appmetrica_application_id = 12345678

# this variable must exists in Airflow
variable_last_download = 'appmetrica_installs_last_download'
last_download = Variable.get(variable_last_download)

filename_prefix = 'appmetrica_installs'

pgsql_connid = 'postgresql_connection'
pgsql_tablename = 'target_table_name'

appmetrica_host = 'api.appmetrica.yandex.ru'
appmetrica_retries = 14
fields_to_get = ['publisher_name', 'tracker_name', 'install_datetime', 
                 'is_reinstallation', 'appmetrica_device_id', 
                 'app_version_name']
appmetrica_refresh_period = 90 #in minuties

default_args = {
    'start_date': pendulum.datetime(2022, 4, 5, 10, 0, tz="Europe/Moscow"),
    'owner': 'Airflow',
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=4),
    'depends_on_past': False,
}

def select_full_or_increment(ti):
    
    import pandas as pd
    
    last_download_dtm = pd.to_datetime(last_download, errors='coerce')
    if pd.isna(last_download_dtm):
        print ('Variable is not datetime - full download.')
        return 'get_full_data'
       
    check_table_exists_sql = f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{pgsql_tablename}' );"
    
    pgsql = PostgresHook(postgres_conn_id=pgsql_connid)
    res = pgsql.get_first(check_table_exists_sql)[0]
    print (res)
    if not res:
        print ('Table not exists in database - full download.')
        return 'get_full_data'

    print ('Increment download.')
    return 'get_increment_data'

def get_full_data(**kwargs):
    
    import requests
    import pandas as pd
    import datetime
    import time
    
    execution_dtm = datetime.datetime.now()   
    execution_date = execution_dtm.strftime("%Y_%m_%d_%H_%M")
    execution_date_url = execution_dtm.strftime("%Y-%m-%d %H:%M:00")
    
    headers = {'Host': appmetrica_host, 
               'Authorization': 'OAuth ' + appmetrica_authorization_key, 
               'Cache-Control': 'max-age=600'}
    
    needed_fields = ','.join(fields_to_get)
    url = f'https://{appmetrica_host}/logs/v1/export/installations.json?application_id={appmetrica_application_id}&date_since=2019-01-01&date_until={execution_date_url}&fields={needed_fields}'
    
    print('URL:', url) 
    
    for i in range(appmetrica_retries):
        r = requests.get(url,headers = headers)
        if r.status_code != 202:
            break
        print ('Retrying...')
        time.sleep(25)
    
    if r.status_code == 202:
        raise Exception(f'HTTP Get unsuccesful. Number of retries is {appmetrica_retries}')
   
    jsondata = r.json()['data']
    installs = pd.json_normalize(jsondata)
    installs_len = len(installs)
    print (f'Data loaded. There is {installs_len} records in dataset.')

    filename = filename_prefix + '_first_full_' + execution_date + '.csv'
    installs.to_csv(filename, index = False)
       
    Variable.set(variable_last_download, execution_date_url)
    
    return filename

def get_increment_data(**kwargs):

    import requests
    import pandas as pd
    import datetime
    import time
    
    execution_dtm = datetime.datetime.now()   
    execution_date = execution_dtm.strftime("%Y_%m_%d_%H_%M")
    execution_date_url = execution_dtm.strftime("%Y-%m-%d %H:%M:00")
    
    last_download_dtm = pd.to_datetime(last_download, errors='coerce')
    last_download_date = last_download_dtm.strftime("%Y_%m_%d_%H_%M")
    last_download_url = last_download #last_download_dtm.strftime("%Y-%m-%d %H:%M:00")
    
    headers = {'Host': appmetrica_host, 
               'Authorization': 'OAuth ' + appmetrica_authorization_key, 
               'Cache-Control': 'max-age=600'}
    
    needed_fields = ','.join(fields_to_get)
    url = f'https://{appmetrica_host}/logs/v1/export/installations.json?application_id={appmetrica_application_id}&date_since={last_download_url}&date_until={execution_date_url}&fields={needed_fields}'
    
    print (f'URL: {url}')
    
    for i in range(appmetrica_retries):
        r = requests.get(url,headers = headers)
        if r.status_code != 202:
            break
        print ('Retrying...')
        time.sleep(25)
    
    if r.status_code == 202:
        raise Exception(f'HTTP Get unsuccesful. Number of retries is {appmetrica_retries}')
   
    jsondata = r.json()['data']
    installs = pd.json_normalize(jsondata)
    installs_len = len(installs)
    print (f'Data loaded. There is {installs_len} records in dataset.')
    
    # if installs_len!=0:

    filename = filename_prefix + '_increment_' + execution_date + '.csv'
    installs.to_csv(filename, index = False)
       
    Variable.set(variable_last_download, execution_date_url)
    
    return filename

def upload_full_data_to_posgresql(**kwargs):
    
    import os
    import pandas as pd
    
    #get file name via XCom
    ti = kwargs['ti']
    filename = ti.xcom_pull(key=None, task_ids='get_full_data')
    print (f'Loading file: {filename}')
    
    #Load file via pandas
    df = pd.read_csv(filename, index_col=0)
    sql_create_table = pd.io.sql.get_schema(df.reset_index(), pgsql_tablename)
    sql_create_table = sql_create_table.replace('"is_reinstallation" INTEGER', '"is_reinstallation" BOOLEAN')
    sql_create_table = sql_create_table.replace('"appmetrica_device_id" INTEGER', '"appmetrica_device_id" TEXT')
    
    #get postgres hook
    pgsql = PostgresHook(postgres_conn_id=pgsql_connid)
    
    # drop table if exists
    sql_table = 'DROP TABLE IF EXISTS ' + pgsql_tablename + ';'
    pgsql.run(sql=sql_table)
    
    # create table
    pgsql.run(sql=sql_create_table)
    
    # upload file to db table
    sql_copy = "COPY "+ pgsql_tablename +" FROM STDIN WITH CSV HEADER DELIMITER as ','"
    pgsql.copy_expert(sql = sql_copy, filename = filename)    
    
    # remove temp file
    os.remove(filename)
    
    return

def upload_increment_data_to_posgresql(**kwargs):

    import os
    
    #get file name via XCom
    ti = kwargs['ti']
    filename = ti.xcom_pull(key=None, task_ids='get_increment_data')
    print (f'Loading file: {filename}')
    
    if filename is not None:
        
        #get postgres hook
        pgsql = PostgresHook(postgres_conn_id=pgsql_connid)
        
        # upload file to db table
        sql_copy = "COPY "+ pgsql_tablename +" FROM STDIN WITH CSV HEADER DELIMITER as ','"
        pgsql.copy_expert(sql = sql_copy, filename = filename)    
    
        # remove temp file
        os.remove(filename)
    
    return

def delete_duplicates(**kwargs):
    
    #https://dba.stackexchange.com/questions/138320/duplicate-rows-how-to-remove-one
    sql_delete_duplicates = f'''
    with d as 
        ( select ctid, row_number() over (partition by t.*) as rn 
         from {pgsql_tablename} as t 
         ) 
    delete from {pgsql_tablename} as t 
    using d 
    where d.rn > 1 
    and d.ctid = t.ctid ;
    '''
    
    print ('Removing duplicates from DB...')
    pgsql = PostgresHook(postgres_conn_id=pgsql_connid)
    pgsql.run(sql_delete_duplicates)
    res = pgsql.conn.notices
    print ('SQL query result:', res)   
    
    return

get_appmetrica_installs_dag = DAG(
    'appmetrica_installs',
    default_args = default_args,
    description = 'Processing installs data from AppMetrica',
    schedule_interval = datetime.timedelta(minutes=appmetrica_refresh_period), #"*/15 * * * *"  = каждые 15 минут
    catchup=False, # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=catchup#catchup
    tags=["appmetrica", 'import', 'raw_data'])

    
select_full_or_increment_operator = BranchPythonOperator(
    task_id="select_full_or_increment",
    python_callable=select_full_or_increment,
    dag=get_appmetrica_installs_dag)

get_full_data_operator = PythonOperator(
    task_id='get_full_data', 
    python_callable=get_full_data,
    dag=get_appmetrica_installs_dag)

get_increment_data_operator = PythonOperator(
    task_id='get_increment_data', 
    python_callable=get_increment_data,
    dag=get_appmetrica_installs_dag)

upload_full_data_to_posgresql_operator = PythonOperator(
    task_id='upload_full_data_to_posgresql', 
    python_callable=upload_full_data_to_posgresql,
    dag=get_appmetrica_installs_dag)

upload_increment_data_to_posgresql_operator = PythonOperator(
    task_id='upload_increment_data_to_posgresql', 
    python_callable=upload_increment_data_to_posgresql,
    dag=get_appmetrica_installs_dag)

delete_duplicates_postgres_operator = PythonOperator(
    task_id='delete_duplicates_postgres', 
    python_callable=delete_duplicates,
    dag=get_appmetrica_installs_dag)

select_full_or_increment_operator >> get_full_data_operator >> upload_full_data_to_posgresql_operator
select_full_or_increment_operator >> get_increment_data_operator >> upload_increment_data_to_posgresql_operator >> delete_duplicates_postgres_operator