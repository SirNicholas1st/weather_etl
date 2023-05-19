from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine
import requests
import pandas as pd

default_args = {
    "owner": "SirNicholas1st",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

@dag (
    dag_id = "weather_etl",
    start_date = datetime(2023, 5, 17),
    default_args = default_args,
    schedule = "@hourly",
    catchup = False
)
def pipeline():

    @task
    def get_weather_data(start_date = datetime.now()):
    
        # creating url from the start date and receiving the JSON from the API
        url = f"https://api.open-meteo.com/v1/forecast?latitude=62.24&longitude=25.72&hourly=temperature_2m,relativehumidity_2m&start_date={start_date.date()}&end_date={start_date.date() + timedelta(days=7)}"
        req = requests.get(url)
        api_data = req.json()
        
        # getting the data into a dataframe and dropping unnecessary info, converting the time column type to datetime.
        df = pd.json_normalize(api_data)
        df = df.explode(["hourly.time", "hourly.temperature_2m", "hourly.relativehumidity_2m"], ignore_index=1)
        df = df.drop(["generationtime_ms", "timezone", "utc_offset_seconds", "timezone_abbreviation", "elevation", "hourly_units.time", "hourly_units.temperature_2m", "hourly_units.relativehumidity_2m"], axis=1)
        df["hourly.time"] = pd.to_datetime(df["hourly.time"], format="%Y-%m-%dT%H:%M")
        df = df.rename(columns={"hourly.time": "date_time", "hourly.temperature_2m":"temperature", "hourly.relativehumidity_2m":"RH"})
        return df
    
    @task
    def check_data(df):

        if df.empty:
            raise Exception("Dataframe is empty")
        elif df.isna().sum().sum() > 0:
            raise Exception("Dataframe contains Null values")
        
    @task
    def df_to_snowflake(df):
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "snowflake_default")
        conn = snowflake_hook.get_uri()
        engine = create_engine(conn)
        df.to_sql("WEATHER_TABLE", con = engine, index = False, if_exists = "replace")


    task1 = get_weather_data()

    task2 = check_data(task1)

    task3 = df_to_snowflake(task1)

    task0 = SnowflakeOperator(
        task_id = "create_table",
        sql = "sql/create_table.sql",
        snowflake_conn_id = "snowflake_default"
    )


    task0 >> task1 >> task2 >> task3

pipeline()