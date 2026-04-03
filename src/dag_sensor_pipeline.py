"""
DAG: sensor_data_pipeline
=========================
Pipeline de ingestión de datos de sensores IoT (temperatura, humedad y calidad
del aire) utilizando Kafka → HDFS → PostgreSQL orquestado con Apache Airflow.

Dataset: home_temperature_and_humidity_smoothed_filled.csv
         Datos reales de 4 salas (salon, chambre, bureau, exterieur) a intervalos
         de 15 min. Formato wide → se pivota a long antes de publicar en Kafka.

Flujo de tareas (9 del enunciado + 1 extra por split técnico):
  1.  read_csv                 – Lee y valida el CSV, empuja estadísticas a XCom
  2.  produce_to_kafka         – Pivota wide→long y publica cada lectura en Kafka
  3.  register_hdfs_connector  – Registra el HDFS Sink Connector (hdfs3) vía REST
  4.  wait_hdfs_flush          – Sensor: espera hasta que aparezcan archivos en HDFS
  5.  verify_hdfs_files        – Lista y registra los archivos en HDFS
  6.  transform_normalize      – Lee HDFS vía WebHDFS, normaliza tipos y fechas
  7.  create_postgres_table    – Crea sensor_data.sensor_readings si no existe
  8.  load_to_postgres         – Carga los datos transformados (idempotente)
  9.  run_analytical_queries   – Ejecuta 5 consultas analíticas sobre los datos
  10. save_results             – Persiste resultados en /opt/airflow/data/
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.python import PythonSensor
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER       = "kafka:29092"
KAFKA_TOPIC        = "sensor_data"
KAFKA_CONNECT_URL  = "http://kafka-connect:8083"
CONNECTOR_NAME     = "hdfs-sink-sensor-data"

NAMENODE_HOST      = "namenode"
NAMENODE_WEBHDFS   = 9870
HDFS_TOPICS_DIR    = "/topics"
HDFS_SENSOR_PATH   = f"{HDFS_TOPICS_DIR}/{KAFKA_TOPIC}/partition=0"

POSTGRES_CONN_ID   = "postgres_default"
PG_SCHEMA          = "sensor_data"
PG_TABLE           = f"{PG_SCHEMA}.sensor_readings"

# Columnas del CSV y su mapeo a sala + ubicación
# Formato wide: una fila = un timestamp con lecturas de todas las salas
CSV_PATH = "/opt/airflow/data/home_temperature_and_humidity_smoothed_filled.csv"

ROOMS = {
    "salon":     "In",
    "chambre":   "In",
    "bureau":    "In",
    "exterieur": "Out",
}

TRANSFORMED_PATH = "/opt/airflow/data/transformed_data.json"
RESULTS_PATH     = "/opt/airflow/data/analytics_results.json"

# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="sensor_data_pipeline",
    default_args=default_args,
    description="Pipeline Kafka → HDFS → PostgreSQL para datos de temperatura y humedad",
    schedule_interval=None,
    catchup=False,
    tags=["kafka", "hdfs", "postgres", "sensors", "entregable3"],
)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _webhdfs(op: str, path: str, **kwargs) -> requests.Response:
    url = f"http://{NAMENODE_HOST}:{NAMENODE_WEBHDFS}/webhdfs/v1{path}?op={op}"
    for k, v in kwargs.items():
        url += f"&{k}={v}"
    return requests.get(url, timeout=30)


def _webhdfs_open(path: str) -> str:
    url = (
        f"http://{NAMENODE_HOST}:{NAMENODE_WEBHDFS}/webhdfs/v1{path}"
        "?op=OPEN&noredirect=false"
    )
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    return resp.text


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 – read_csv
# ─────────────────────────────────────────────────────────────────────────────

def _read_csv(**context):
    """
    Lee el CSV en formato wide, valida su estructura y calcula estadísticas.
    El CSV tiene una fila por timestamp con columnas temperature_X, humidity_X
    y air_X para cada sala (salon, chambre, bureau, exterieur).
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV no encontrado: {CSV_PATH}")

    rows = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    total_timestamps = len(rows)
    total_messages   = total_timestamps * len(ROOMS)   # tras pivotar

    temps = []
    hums  = []
    for row in rows:
        for room in ROOMS:
            t = row.get(f"temperature_{room}", "")
            h = row.get(f"humidity_{room}", "")
            if t:
                temps.append(float(t))
            if h:
                hums.append(float(h))

    stats = {
        "total_timestamps":     total_timestamps,
        "rooms":                list(ROOMS.keys()),
        "total_messages_kafka": total_messages,
        "date_from":            rows[0]["timestamp"],
        "date_to":              rows[-1]["timestamp"],
        "temp_min":             round(min(temps), 2),
        "temp_max":             round(max(temps), 2),
        "temp_avg":             round(sum(temps) / len(temps), 2),
        "humidity_min":         round(min(hums), 2),
        "humidity_max":         round(max(hums), 2),
        "humidity_avg":         round(sum(hums) / len(hums), 2),
    }

    logger.info("CSV validado: %d timestamps × %d salas = %d mensajes Kafka",
                total_timestamps, len(ROOMS), total_messages)
    logger.info("Periodo: %s → %s", stats["date_from"], stats["date_to"])
    logger.info("Temperatura: min=%.1f  max=%.1f  avg=%.2f",
                stats["temp_min"], stats["temp_max"], stats["temp_avg"])
    logger.info("Humedad:     min=%.1f  max=%.1f  avg=%.2f",
                stats["humidity_min"], stats["humidity_max"], stats["humidity_avg"])

    context["ti"].xcom_push(key="csv_stats", value=stats)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 – produce_to_kafka
# ─────────────────────────────────────────────────────────────────────────────

def _produce_to_kafka(**context):
    """
    Lee el CSV en formato wide y lo pivota a long:
      una fila CSV → 4 mensajes Kafka (uno por sala).
    Cada mensaje JSON incluye: id, timestamp, room, temperature, humidity,
    air_quality y location (In/Out).
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

    count = 0
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_idx, row in enumerate(reader):
            for room_idx, (room, location) in enumerate(ROOMS.items()):
                msg_id = row_idx * len(ROOMS) + room_idx
                msg = {
                    "id":          msg_id,
                    "timestamp":   row["timestamp"],
                    "room":        room,
                    "temperature": float(row.get(f"temperature_{room}", 0) or 0),
                    "humidity":    float(row.get(f"humidity_{room}", 0) or 0),
                    "air_quality": float(row.get(f"air_{room}", 0) or 0),
                    "location":    location,
                }
                producer.send(KAFKA_TOPIC, key=str(msg_id), value=msg)
                count += 1

    producer.flush()
    producer.close()

    logger.info("Publicados %d mensajes en el topic '%s'.", count, KAFKA_TOPIC)
    context["ti"].xcom_push(key="messages_produced", value=count)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 – register_hdfs_connector
# ─────────────────────────────────────────────────────────────────────────────

def _register_hdfs_connector(**context):
    """
    Registra (o actualiza) el HDFS Sink Connector en Kafka Connect.
    Usa el conector HDFS3 (compatible con Hadoop 3.4.x).
    """
    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class":                 "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "tasks.max":                        "1",
            "topics":                           KAFKA_TOPIC,
            "hdfs.url":                         "hdfs://namenode:9000",
            "topics.dir":                       HDFS_TOPICS_DIR,
            "logs.dir":                         "/logs",
            "flush.size":                       "500",
            "rotate.interval.ms":               "30000",
            "locale":                           "en_US",
            "timezone":                         "UTC",
            "timestamp.extractor":              "Wallclock",
            "storage.class":                    "io.confluent.connect.hdfs3.storage.HdfsStorage",
            "format.class":                     "io.confluent.connect.hdfs3.json.JsonFormat",
            "value.converter":                  "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable":   "false",
            "key.converter":                    "org.apache.kafka.connect.storage.StringConverter",
            "consumer.auto.offset.reset":       "earliest",
            "hadoop.conf.dir":                  "/opt/hadoop/conf",
            "confluent.topic.bootstrap.servers": KAFKA_BROKER,
            "confluent.topic.replication.factor": "1",
        },
    }

    # Esperar a que Kafka Connect esté disponible (hasta 3 min)
    for attempt in range(18):
        try:
            r = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=10)
            if r.status_code == 200:
                logger.info("Kafka Connect listo en el intento %d.", attempt + 1)
                break
        except requests.exceptions.ConnectionError:
            pass
        logger.info("Kafka Connect no disponible – intento %d/18 – esperando 10 s...", attempt + 1)
        time.sleep(10)
    else:
        raise RuntimeError("Kafka Connect no estuvo disponible tras 3 minutos.")

    existing = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}", timeout=10)
    if existing.status_code == 200:
        logger.info("Conector ya existe – actualizando configuración.")
        resp = requests.put(
            f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
            json=connector_config["config"],
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
    else:
        logger.info("Registrando conector por primera vez.")
        resp = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            json=connector_config,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Error registrando conector: {resp.status_code} – {resp.text}")

    logger.info("Conector '%s' registrado/actualizado.", CONNECTOR_NAME)
    context["ti"].xcom_push(key="connector_name", value=CONNECTOR_NAME)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 – wait_hdfs_flush  (PythonSensor)
# ─────────────────────────────────────────────────────────────────────────────

def _check_hdfs_files(**context) -> bool:
    """
    Sensor: devuelve True cuando Kafka Connect ha escrito al menos un archivo
    en /topics/sensor_data/partition=0/ en HDFS.
    """
    try:
        resp = _webhdfs("LISTSTATUS", HDFS_SENSOR_PATH)
        if resp.status_code == 200:
            files = resp.json().get("FileStatuses", {}).get("FileStatus", [])
            ready = [f for f in files if f.get("type") == "FILE" and f.get("length", 0) > 0]
            if ready:
                logger.info("HDFS: %d archivo(s) encontrado(s) – sensor OK.", len(ready))
                return True
        logger.info("HDFS: todavía sin archivos en %s (HTTP %s).", HDFS_SENSOR_PATH, resp.status_code)
    except Exception as exc:
        logger.warning("Error consultando HDFS: %s", exc)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 – verify_hdfs_files
# ─────────────────────────────────────────────────────────────────────────────

def _verify_hdfs_files(**context):
    """Lista los archivos escritos por Kafka Connect en HDFS y registra sus tamaños."""
    resp = _webhdfs("LISTSTATUS", HDFS_SENSOR_PATH)
    resp.raise_for_status()

    files = resp.json()["FileStatuses"]["FileStatus"]
    file_list = [
        {"name": f["pathSuffix"], "size_bytes": f["length"]}
        for f in files if f.get("type") == "FILE"
    ]
    total_bytes = sum(fi["size_bytes"] for fi in file_list)

    logger.info("Archivos en HDFS (%s):", HDFS_SENSOR_PATH)
    for fi in file_list:
        logger.info("  %-70s  %8d B", fi["name"], fi["size_bytes"])
    logger.info("Total: %d archivo(s), %.1f KB", len(file_list), total_bytes / 1024)

    context["ti"].xcom_push(key="hdfs_files",      value=file_list)
    context["ti"].xcom_push(key="hdfs_file_count", value=len(file_list))


# ─────────────────────────────────────────────────────────────────────────────
# TASK 6 – transform_normalize
# ─────────────────────────────────────────────────────────────────────────────

def _transform_normalize(**context):
    """
    Lee todos los archivos JSON del directorio HDFS vía WebHDFS.
    Normaliza:
      - timestamp ya está en ISO 8601 → validar y mantener
      - temperature, humidity, air_quality → float redondeado
      - location → valor canónico In / Out
    Elimina duplicados por id.
    Guarda en TRANSFORMED_PATH para la carga en PostgreSQL.
    """
    resp = _webhdfs("LISTSTATUS", HDFS_SENSOR_PATH)
    resp.raise_for_status()
    files = [
        f["pathSuffix"]
        for f in resp.json()["FileStatuses"]["FileStatus"]
        if f.get("type") == "FILE" and f.get("length", 0) > 0
    ]

    records_raw = []
    for fname in files:
        content = _webhdfs_open(f"{HDFS_SENSOR_PATH}/{fname}")
        for line in content.splitlines():
            line = line.strip()
            if line:
                try:
                    records_raw.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning("Línea JSON inválida en %s: %s", fname, e)

    logger.info("Registros leídos de HDFS: %d", len(records_raw))

    seen_ids = set()
    records_clean = []
    for rec in records_raw:
        rid = rec.get("id")
        if rid in seen_ids:
            continue
        seen_ids.add(rid)

        # Validar timestamp
        ts = rec.get("timestamp", "")
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            ts_clean = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            ts_clean = ts

        loc = str(rec.get("location", "")).strip().capitalize()
        if loc not in ("In", "Out"):
            loc = "Unknown"

        records_clean.append({
            "id":          int(rid),
            "timestamp":   ts_clean,
            "room":        str(rec.get("room", "")).strip(),
            "temperature": round(float(rec.get("temperature", 0)), 2),
            "humidity":    round(float(rec.get("humidity", 0)), 2),
            "air_quality": round(float(rec.get("air_quality", 0)), 2),
            "location":    loc,
        })

    records_clean.sort(key=lambda r: r["id"])

    os.makedirs(os.path.dirname(TRANSFORMED_PATH), exist_ok=True)
    with open(TRANSFORMED_PATH, "w", encoding="utf-8") as f:
        json.dump(records_clean, f, ensure_ascii=False, indent=2)

    logger.info("Transformación completada: %d registros únicos → %s",
                len(records_clean), TRANSFORMED_PATH)
    context["ti"].xcom_push(key="transformed_count", value=len(records_clean))


# ─────────────────────────────────────────────────────────────────────────────
# TASK 7 – create_postgres_table  (PostgresOperator)
# ─────────────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = f"""
CREATE SCHEMA IF NOT EXISTS sensor_data;

CREATE TABLE IF NOT EXISTS sensor_data.sensor_readings (
    id           INTEGER,
    timestamp    TIMESTAMP     NOT NULL,
    room         VARCHAR(60)   NOT NULL,
    temperature  NUMERIC(6, 2),
    humidity     NUMERIC(5, 2),
    air_quality  NUMERIC(10, 2),
    location     VARCHAR(10)   NOT NULL,
    ingested_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_sensor_readings PRIMARY KEY (id),
    CONSTRAINT uq_ts_room UNIQUE (timestamp, room)
);

CREATE INDEX IF NOT EXISTS idx_sr_room
    ON sensor_data.sensor_readings (room);

CREATE INDEX IF NOT EXISTS idx_sr_timestamp
    ON sensor_data.sensor_readings (timestamp);
"""


# ─────────────────────────────────────────────────────────────────────────────
# TASK 8 – load_to_postgres
# ─────────────────────────────────────────────────────────────────────────────

def _load_to_postgres(**context):
    """
    Carga los datos transformados en PostgreSQL.
    ON CONFLICT DO NOTHING garantiza idempotencia (re-ejecuciones seguras).
    """
    with open(TRANSFORMED_PATH, "r", encoding="utf-8") as f:
        records = json.load(f)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    insert_sql = f"""
        INSERT INTO {PG_TABLE}
            (id, timestamp, room, temperature, humidity, air_quality, location)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """

    batch = [
        (r["id"], r["timestamp"], r["room"],
         r["temperature"], r["humidity"], r["air_quality"], r["location"])
        for r in records
    ]
    cur.executemany(insert_sql, batch)
    conn.commit()
    cur.close()
    conn.close()

    logger.info("Insertados %d registros en %s.", len(batch), PG_TABLE)
    context["ti"].xcom_push(key="rows_loaded", value=len(batch))


# ─────────────────────────────────────────────────────────────────────────────
# TASK 9 – run_analytical_queries
# ─────────────────────────────────────────────────────────────────────────────

def _run_analytical_queries(**context):
    """
    Ejecuta 5 consultas analíticas sobre los datos de sensores en PostgreSQL.
    Cada consulta aporta valor de negocio (eficiencia energética, confort, HVAC).
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    queries = {

        # Q1 ─ Temperatura y humedad media por sala
        "q1_avg_per_room": {
            "description": "Temperatura y humedad media por sala — línea base de cada sensor",
            "sql": f"""
                SELECT
                    room,
                    location,
                    ROUND(AVG(temperature)::numeric, 2) AS avg_temp,
                    ROUND(AVG(humidity)::numeric,    2) AS avg_humidity,
                    ROUND(AVG(air_quality)::numeric, 2) AS avg_air_quality,
                    COUNT(*) AS total_readings
                FROM {PG_TABLE}
                GROUP BY room, location
                ORDER BY avg_temp DESC;
            """,
        },

        # Q2 ─ Rango diario de temperatura por sala (anomalías HVAC)
        "q2_daily_temp_range": {
            "description": "Rango diario de temperatura — detecta anomalías y problemas HVAC",
            "sql": f"""
                SELECT
                    room,
                    DATE(timestamp)                               AS day,
                    ROUND(MIN(temperature)::numeric, 2)           AS min_temp,
                    ROUND(MAX(temperature)::numeric, 2)           AS max_temp,
                    ROUND((MAX(temperature) - MIN(temperature))::numeric, 2) AS temp_range
                FROM {PG_TABLE}
                GROUP BY room, DATE(timestamp)
                ORDER BY temp_range DESC
                LIMIT 20;
            """,
        },

        # Q3 ─ Lecturas con humedad crítica (>75%) por sala
        "q3_high_humidity": {
            "description": "Salas con humedad >75% — riesgo de moho e incomodidad",
            "sql": f"""
                SELECT
                    room,
                    location,
                    COUNT(*) AS readings_above_75pct,
                    ROUND(MAX(humidity)::numeric, 2) AS peak_humidity,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER ()::numeric, 2) AS pct_of_all_alerts
                FROM {PG_TABLE}
                WHERE humidity > 75
                GROUP BY room, location
                ORDER BY readings_above_75pct DESC;
            """,
        },

        # Q4 ─ Patrón horario: temperatura interior vs exterior
        "q4_hourly_indoor_vs_outdoor": {
            "description": "Temperatura media por hora del día — optimizar programas HVAC y ventilación",
            "sql": f"""
                SELECT
                    EXTRACT(HOUR FROM timestamp)::integer         AS hour_of_day,
                    ROUND(AVG(CASE WHEN location = 'In'  THEN temperature END)::numeric, 2) AS avg_indoor_temp,
                    ROUND(AVG(CASE WHEN location = 'Out' THEN temperature END)::numeric, 2) AS avg_outdoor_temp,
                    ROUND(AVG(CASE WHEN location = 'In'  THEN humidity   END)::numeric, 2) AS avg_indoor_humidity
                FROM {PG_TABLE}
                GROUP BY EXTRACT(HOUR FROM timestamp)
                ORDER BY hour_of_day;
            """,
        },

        # Q5 ─ Resumen ejecutivo del dataset
        "q5_global_summary": {
            "description": "Estadísticas globales — resumen ejecutivo para dirección",
            "sql": f"""
                SELECT
                    COUNT(*)                                        AS total_readings,
                    COUNT(DISTINCT room)                            AS unique_rooms,
                    MIN(timestamp)                                  AS date_from,
                    MAX(timestamp)                                  AS date_to,
                    ROUND(AVG(temperature)::numeric,  2)            AS overall_avg_temp,
                    ROUND(STDDEV(temperature)::numeric, 2)          AS stddev_temp,
                    ROUND(AVG(humidity)::numeric,     2)            AS overall_avg_humidity,
                    ROUND(MAX(air_quality)::numeric,  2)            AS max_air_quality,
                    ROUND(100.0 * SUM(CASE WHEN location = 'In'  THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS pct_indoor,
                    ROUND(100.0 * SUM(CASE WHEN location = 'Out' THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS pct_outdoor
                FROM {PG_TABLE};
            """,
        },
    }

    results = {}
    for key, q in queries.items():
        logger.info("── %s: %s", key, q["description"])
        rows = hook.get_records(q["sql"])
        serializable = [
            [str(v) if not isinstance(v, (int, float, type(None))) else v for v in row]
            for row in rows
        ]
        results[key] = {
            "description": q["description"],
            "row_count":   len(serializable),
            "data":        serializable,
        }
        logger.info("  → %d filas", len(serializable))
        for row in serializable[:5]:
            logger.info("    %s", row)

    context["ti"].xcom_push(key="analytics_results", value=results)
    logger.info("Consultas analíticas completadas.")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 10 – save_results
# ─────────────────────────────────────────────────────────────────────────────

def _save_results(**context):
    """
    Persiste en JSON el informe completo de la ejecución del pipeline:
    estadísticas del CSV, conteo de mensajes, archivos HDFS y resultados analíticos.
    """
    ti = context["ti"]

    report = {
        "dag_run_id":        context["run_id"],
        "execution_date":    str(context["execution_date"]),
        "csv_stats":         ti.xcom_pull(task_ids="read_csv",                key="csv_stats"),
        "messages_produced": ti.xcom_pull(task_ids="produce_to_kafka",        key="messages_produced"),
        "hdfs_file_count":   ti.xcom_pull(task_ids="verify_hdfs_files",       key="hdfs_file_count"),
        "transformed_count": ti.xcom_pull(task_ids="transform_normalize",     key="transformed_count"),
        "rows_loaded":       ti.xcom_pull(task_ids="load_to_postgres",        key="rows_loaded"),
        "analytics":         ti.xcom_pull(task_ids="run_analytical_queries",  key="analytics_results"),
    }

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    stats = report.get("csv_stats") or {}
    logger.info("══════════════════════════════════════════════════")
    logger.info("  PIPELINE COMPLETADO")
    logger.info("  Timestamps CSV:      %s", stats.get("total_timestamps"))
    logger.info("  Mensajes Kafka:      %s", report.get("messages_produced"))
    logger.info("  Archivos en HDFS:    %s", report.get("hdfs_file_count"))
    logger.info("  Registros en PG:     %s", report.get("rows_loaded"))
    logger.info("  Resultados en:       %s", RESULTS_PATH)
    logger.info("══════════════════════════════════════════════════")


# ─────────────────────────────────────────────────────────────────────────────
# TASK OBJECTS
# ─────────────────────────────────────────────────────────────────────────────

t1_read = PythonOperator(
    task_id="read_csv",
    python_callable=_read_csv,
    provide_context=True,
    dag=dag,
)

t2_produce = PythonOperator(
    task_id="produce_to_kafka",
    python_callable=_produce_to_kafka,
    provide_context=True,
    dag=dag,
)

t3_connector = PythonOperator(
    task_id="register_hdfs_connector",
    python_callable=_register_hdfs_connector,
    provide_context=True,
    dag=dag,
)

t4_wait = PythonSensor(
    task_id="wait_hdfs_flush",
    python_callable=_check_hdfs_files,
    poke_interval=30,
    timeout=600,
    mode="poke",
    dag=dag,
)

t5_verify = PythonOperator(
    task_id="verify_hdfs_files",
    python_callable=_verify_hdfs_files,
    provide_context=True,
    dag=dag,
)

t6_transform = PythonOperator(
    task_id="transform_normalize",
    python_callable=_transform_normalize,
    provide_context=True,
    dag=dag,
)

t7_create_table = PostgresOperator(
    task_id="create_postgres_table",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=CREATE_TABLE_SQL,
    dag=dag,
)

t8_load = PythonOperator(
    task_id="load_to_postgres",
    python_callable=_load_to_postgres,
    provide_context=True,
    dag=dag,
)

t9_analytics = PythonOperator(
    task_id="run_analytical_queries",
    python_callable=_run_analytical_queries,
    provide_context=True,
    dag=dag,
)

t10_save = PythonOperator(
    task_id="save_results",
    python_callable=_save_results,
    provide_context=True,
    dag=dag,
)

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE GRAPH
# ─────────────────────────────────────────────────────────────────────────────
(
    t1_read
    >> t2_produce
    >> t3_connector
    >> t4_wait
    >> t5_verify
    >> t6_transform
    >> t7_create_table
    >> t8_load
    >> t9_analytics
    >> t10_save
)
