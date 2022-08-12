# -*- coding: utf-8 -*-

from airflow import DAG
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pendulum
import datetime
from datetime import timedelta
import os
import json

# List of email address to send email alerts to if this job fails
monitoring_recipients_emails = Variable.get("monitoring_recipients_emails")
ALERT_EMAIL_ADDRESSES = [x.strip() for x in monitoring_recipients_emails.split(',')]

kafka_config = json.loads(Variable.get("kafka_authorization"))
kafka_login = kafka_config['login']
kafka_pass = kafka_config['pass']

pgsql_connid_1 = 'postgresql connection name'

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

def process_message(message):
    
    slack = SlackHook(slack_conn_id='slack-alerts')
    
    key = json.loads(message.key())
    offset = message.offset()
    
    if message.value() is not None:
        value = json.loads(message.value())
    else:
        print (f'Message value is None! Offset = {offset}, Key = {key}')
        return
        
    # condition for alert
    if (value['op']=='c') and (value['after']['user_roles']=='CLIENT'):
        
        print (f'Offset: {offset}')
        print (f'New CLIENT user: {value}')
        
        # here you can get from db some additional information if needed
        pgsql_1 = PostgresHook(postgres_conn_id=pgsql_connid_1)
        user_id = value['after']['id']
        client_info = ... 
        
        if client_info is not None: 
            msg = f'''Зарегистрирован новый сотрудник клиента:
                ...'''
            channel = '...' #slack channel
            slack.client.chat_postMessage(channel=channel, text=msg)  
        else:
            raise ValueError('Some error message')

    return

default_args = {
    'start_date': pendulum.datetime(2022, 7, 5, 10, 0, tz="Europe/Moscow"),
    'owner': 'Airflow',
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
    'depends_on_past': False,
}

dag_alerts = DAG(
    'alerts',
    default_args = default_args,
    description = 'DAG for alerts',
    schedule_interval = "*/10 * * * *",
    catchup=False, # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=catchup#catchup
    tags=['alerts'],
    max_active_runs=1, # Only allow one run of this DAG to be running at any given time
    )

get_messages = ConsumeFromTopicOperator(
        task_id="get_messages",
        topics=["production.public.users"],
        apply_function="dag_alerts.process_message",
        consumer_config = {   #https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
            "client.id": 'airflow',
            'group.id':'airflow',
            'bootstrap.servers': 'kafka_server:9092',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': CUR_DIR+'/ca.crt', #private cert for SSL connection
            #'enable.ssl.certificate.verification': False,
            'sasl.mechanism': 'PLAIN',
            'sasl.username': kafka_login,
            'sasl.password': kafka_pass,
            "auto.offset.reset": "smallest",            
            },
        commit_cadence="end_of_batch",
        max_messages=10000,
        max_batch_size=500,
        dag=dag_alerts,
        execution_timeout=timedelta(seconds=180),
        poll_timeout = 1
    )