# airflow-complex-dags

Just sharing some tricky DAGs made to work under Airflow

dag_alerts.py : Read messages from Kafka, analyze them, send alerts to Slack (sort of internal alerting...)

postgresql_get_column_types_from_query.py : Function of getting PostgreSQL column types automatically from any query (even very complex) via creation of VIEW and getting info from PosgreSQL internal tables. Also added some info why you may need this (some troubles with Pandas processing).

