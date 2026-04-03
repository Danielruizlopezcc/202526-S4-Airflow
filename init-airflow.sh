#!/bin/bash
set -e

echo "Waiting for postgres..."
sleep 5

echo "Initializing Airflow database..."
airflow db upgrade || airflow db init || { echo "Database initialization failed"; exit 1; }

echo "Database initialized successfully!"

echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || echo "User already exists"

echo "Adding Kafka connection..."
airflow connections add kafka_default \
    --conn-type kafka \
    --conn-host kafka \
    --conn-port 29092 \
    --conn-extra '{"bootstrap.servers": "kafka:29092", "group.id": "airflow-consumer"}' || echo "Connection already exists"

echo "Adding Postgres connection (Airflow metadata DB)..."
airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema airflow || echo "Connection already exists"

echo "Creating sensor_data schema inside airflow DB..."
PGPASSWORD=airflow psql -h postgres -U airflow -d airflow \
    -c "CREATE SCHEMA IF NOT EXISTS sensor_data;" || echo "Schema already exists"

echo "Starting Airflow scheduler in background..."
airflow scheduler &

echo "Starting Airflow webserver..."
exec airflow webserver --port 8080
