from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import clickhouse_connect
import pandas as pd

import os

# Ваша функция для выполнения
def send_to_clickhouse():
    client = clickhouse_connect.get_client(
        host='j7l1ejvruf.europe-west4.gcp.clickhouse.cloud',
        user='default',
        password='ToW~Rk02PD8rk',
        secure=True
    )
    print("Result:", client.query("select * from \"fitness_stats\"").result_set)

    csv_file_path = '/root/airflow/activity-transform.csv'
    df = pd.read_csv(csv_file_path)
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    # df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    print(df['Date'])

    table_name = 'fitness_stats'

    values = df.values

    print(type(values[0][1]))
    columns = ['UserId','Date','Total_Distance','Tracker_Distance','Logged_Activities_Distance','Very_Active_Distance','Moderately_Active_Distance','Light_Active_Distance','Sedentary_Active_Distance','Very_Active_Minutes','Fairly_Active_Minutes','Lightly_Active_Minutes','Sedentary_Minutes','Steps','Calories_Burned']

    client.insert(table_name, values, columns)

    print("Data inserted successfully.")
    print("Result:", client.query("select * from \"fitness_stats\"").result_set)
    print("Running my Python function!")

# Настройки DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'send_to_clickhouse',
    default_args=default_args,
    description='Run job every hour',
    schedule_interval='@hourly'  # Указываем частоту выполнения
)

# Создание задачи для выполнения вашей функции
run_this_task = PythonOperator(
    task_id='run_send_to_clickhouse',
    python_callable=send_to_clickhouse,
    dag=dag,
)

# Устанавливаем зависимость задачи от предыдущей
run_this_task