import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres_operator import PostgresOperator

def extract_data():
  """Extracts data from the CSV file."""
  with open('weather.csv', 'r') as f:
    data = f.readlines()

  return data

def transform_data(data):
  """Transforms the data."""
  for line in data:
    line = line.strip()
    line = line.split(',')

    # Convert the temperature from Celsius to Fahrenheit.
    temperature = int(line[1]) * 9 / 5 + 32

    # Round the humidity and wind speed to two decimal places.
    humidity = round(float(line[2]), 2)
    wind_speed = round(float(line[3]), 2)

    data.append((line[0], temperature, humidity, wind_speed))

  return data

def load_data(data):
  """Loads the data into the PostgreSQL database."""
  connection = airflow.models.Connection(
    conn_id='postgres_conn',
    conn_type='postgres',
    host='localhost',
    port='5432',
    username='postgres',
    password='postgres',
    schema='public',
  )

  postgres_operator = PostgresOperator(
    task_id='load_data',
    sql='INSERT INTO weather (date, temperature, humidity, wind_speed) VALUES (%s, %s, %s, %s)',
    params=data,
    connection_id=connection.conn_id,
  )

  postgres_operator.execute()

dag = airflow.DAG(dag_id='etl_pipeline', schedule_interval='@daily')

extract_task = PythonOperator(
  task_id='extract_data',
  python_callable=extract_data,
  dag=dag,
)

transform_task = PythonOperator(
  task_id='transform_data',
  python_callable=transform_data,
  dag=dag,
)

load_task = PythonOperator(
  task_id='load_data',
  python_callable=load_data,
  dag=dag,
)

extract_task >> transform_task >> load_task