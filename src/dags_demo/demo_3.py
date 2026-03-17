from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"


def crear_tabla():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run("""
        CREATE TABLE IF NOT EXISTS demo_airflow_users (
            id INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL
        );
    """)
    print("Tabla creada o ya existente")


def insertar_datos():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run("""
        INSERT INTO demo_airflow_users (id, nombre)
        VALUES
            (1, 'Ana'),
            (2, 'Luis'),
            (3, 'Marta')
        ON CONFLICT (id) DO NOTHING;
    """)
    print("Datos insertados")


def consultar_datos():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = hook.get_records("SELECT * FROM demo_airflow_users ORDER BY id;")

    print("Contenido de demo_airflow_users:")
    for row in rows:
        print(row)


with DAG(
    dag_id="demo_04_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "postgres"],
) as dag:

    crear = PythonOperator(
        task_id="crear_tabla",
        python_callable=crear_tabla,
    )

    insertar = PythonOperator(
        task_id="insertar_datos",
        python_callable=insertar_datos,
    )

    consultar = PythonOperator(
        task_id="consultar_datos",
        python_callable=consultar_datos,
    )

    crear >> insertar >> consultar