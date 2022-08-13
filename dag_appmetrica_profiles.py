# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import datetime
import pendulum


appmetrica_authorization_key = Variable.get("appmetrica_authorization")
appmetrica_application_id = 12345678


# List of email address to send email alerts to if this job fails
monitoring_recipients_emails = Variable.get("monitoring_recipients_emails")
ALERT_EMAIL_ADDRESSES = [x.strip() for x in monitoring_recipients_emails.split(',')]

pgsql_connid = 'postgresql_connection'
pgsql_tablename = 'target_table_name'

appmetrica_host = 'api.appmetrica.yandex.ru'
appmetrica_retries = 14
fields_to_get = ['profile_id', 'appmetrica_gender', 
                 'appmetrica_first_session_date', 'appmetrica_last_start_date',
                 'appmetrica_sessions',
                 'appmetrica_device_id', 'city',
                 'device_type', 'app_version_name', 'os_name']
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

def get_data(**kwargs):
    
    import requests
    import pandas as pd
    import time
    
    execution_date = time.strftime("%Y_%m_%d_%H_%M")
    
    headers = {'Host': appmetrica_host, 
               'Authorization': 'OAuth ' + appmetrica_authorization_key, 
               'Cache-Control': 'max-age=600'}
    
    needed_fields = ','.join(fields_to_get)
    url = f'https://{appmetrica_host}/logs/v1/export/profiles.json?application_id={appmetrica_application_id}&fields={needed_fields}'
    
    for i in range(appmetrica_retries):
        r = requests.get(url,headers = headers)
        if r.status_code != 202:
            break
        print ('Retrying...')
        time.sleep(25)
    
    if r.status_code == 202:
        raise Exception(f'HTTP Get unsuccesful. Number of retries is {appmetrica_retries}')
    
    print (r.status_code)
    jsondata = r.json()['data']
    profiles = pd.json_normalize(jsondata)
    profiles_len = len(profiles)
    print (f'Data loaded. There is {profiles_len} records in dataset.')

    filename = 'appmetrica_profiles_' + execution_date + '.csv'
    profiles.to_csv(filename, index_label='ind')
   
    return filename #execution_date

def upload_data_to_posgresql(**kwargs):

    import os
    import pandas as pd
    
    #get file name via XCom
    ti = kwargs['ti']
    filename = ti.xcom_pull(key=None, task_ids='get_data')
    print (f'Loading file: {filename}')
    
    #Load file via pandas
    df = pd.read_csv(filename, index_col=0)
    sql_create_table = pd.io.sql.get_schema(df.reset_index(), pgsql_tablename)
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

get_appmetrica_profiles_dag = DAG(
    'appmetrica_profiles',
    default_args = default_args,
    description = 'Processing profiles data from AppMetrica',
    schedule_interval = datetime.timedelta(minutes=appmetrica_refresh_period), #"*/15 * * * *"  = каждые 15 минут
    catchup=False, # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=catchup#catchup
    tags=["appmetrica", 'import', 'raw_data'])

get_data_operator = PythonOperator(
    task_id='get_data', 
    python_callable=get_data,
    dag=get_appmetrica_profiles_dag)

upload_data_to_posgresql_operator = PythonOperator(
    task_id='upload_data_to_posgresql', 
    python_callable=upload_data_to_posgresql,
    dag=get_appmetrica_profiles_dag)

get_data_operator >> upload_data_to_posgresql_operator