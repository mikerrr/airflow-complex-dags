# airflow-complex-dags

Just sharing some tricky DAGs made to work under Airflow

dag_alerts.py : Read messages from Kafka, analyze them, send alerts to Slack (sort of internal alerting...)

postgresql_get_column_types_from_query.py : Function of getting PostgreSQL column types automatically from any query (even very complex) via creation of VIEW and getting info from PosgreSQL internal tables. Also added some info why you may need this (some troubles with Pandas processing).

postgresql_create_table_statement_from_dict.py : Function of creating 'CREATE TABLE' statement using function in prevous file, and example of usage of such function under Airflow (download result of query to Pandas DataFrame, save it to CSV, upload to PostgreSQL new table) 

dag_copy_query_to_table.py: DAG example using two previous functions. Copying result of a complex query to separate table in PostgreSQL. Source and target can be two different PgSQL servers.

dag_appmetrica_profiles.py: DAG for downloading profiles information from Yandex Appmetrica. Profiles volume is not big, so you can download all of them every time

dag_appmetrica_installs.py: DAG for downloading installs (of monitored application) information from Yandex Appmetrica. There should exist special variable where last dowload time is stored. First time there is full download, next times - increment downloads. Also we delete duplicated rows in target table if any.