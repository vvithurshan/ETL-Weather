from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime


LATITUDE = "43.70011"
LONGITUDE = "-79.4163"

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "weatherapi"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 7, 6),
}


with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    @task
    def extract_weather():

        http_hook = HttpHook(
            http_conn_id=API_CONN_ID,
            method="GET",
        )
        
        # Test
       #  https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()

        raise ValueError(
            f"Weather API failed. Status code: {response.status_code}"
        )


    @task
    def transform_weather(data):

        current_weather = data.get("current_weather", {})

        transformed = {
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "temperature": current_weather.get("temperature"),
            "windspeed": current_weather.get("windspeed"),
            "winddirection": current_weather.get("winddirection"),
            "weathercode": current_weather.get("weathercode"),
            "time": current_weather.get("time"),
            "is_day": current_weather.get("is_day"),
        }

        return transformed


    @task
    def load_weather(data):

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather (
                id SERIAL PRIMARY KEY,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode FLOAT,
                time TEXT,
                is_day FLOAT
            )
            """
        )

        cursor.execute(
            """
            INSERT INTO weather (
                latitude,
                longitude,
                temperature,
                windspeed,
                winddirection,
                weathercode,
                time,
                is_day
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data["latitude"],
                data["longitude"],
                data["temperature"],
                data["windspeed"],
                data["winddirection"],
                data["weathercode"],
                data["time"],
                data["is_day"],
            ),
        )

        conn.commit()

        cursor.close()
        conn.close()


    # ETL pipeline

    weather_data = extract_weather()

    transformed_weather = transform_weather(
        weather_data
    )

    load_weather(transformed_weather)