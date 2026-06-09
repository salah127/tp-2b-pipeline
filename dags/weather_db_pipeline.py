import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_FILE = DATA_DIR / "raw_weather.json"
PROCESSED_FILE = DATA_DIR / "processed_weather.json"

CITY_LIST = [
    {"name": "Strasbourg", "latitude": 48.5734, "longitude": 7.7521},
    {"name": "Rennes", "latitude": 48.1173, "longitude": -1.6778},
    {"name": "Nice", "latitude": 43.7102, "longitude": 7.2620},
]

PIPELINE_CONFIG = {
    "cities": CITY_LIST,
    "postgres_conn_id": "weather_db_conn",
    "weather_table": "weather_data",
    "audit_table": "pipeline_audit_log",
    "start_date": None,
    "end_date": None,
}

METEO_API_URL = "https://api.open-meteo.com/v1/forecast"


def get_pipeline_config(dag_run: Any) -> Dict[str, Any]:
    config = (dag_run.conf if dag_run else {}) or {}
    result = PIPELINE_CONFIG.copy()
    result.update({k: config[k] for k in config if k in result})
    return result


def parse_date(date_value: Any, default: datetime.date) -> datetime.date:
    if not date_value:
        return default
    if isinstance(date_value, datetime):
        return date_value.date()
    if isinstance(date_value, str):
        return datetime.fromisoformat(date_value).date()
    raise ValueError(f"Invalid date format for {date_value}")


def build_open_meteo_url(latitude: float, longitude: float, start_date: str, end_date: str) -> str:
    return (
        f"{METEO_API_URL}?latitude={latitude}&longitude={longitude}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=Europe%2FParis&start_date={start_date}&end_date={end_date}"
    )


def fetch_city_weather(city: Dict[str, Any], start_date: str, end_date: str) -> Dict[str, Any]:
    url = build_open_meteo_url(city["latitude"], city["longitude"], start_date, end_date)
    request = Request(url, headers={"User-Agent": "airflow-open-meteo"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload)
    except HTTPError as exc:
        return {
            "error": "http",
            "status": exc.code,
            "message": str(exc),
            "city": city["name"],
        }
    except URLError as exc:
        return {
            "error": "url",
            "message": str(exc),
            "city": city["name"],
        }


def extract_weather(dag_run: Any = None) -> str:
    config = get_pipeline_config(dag_run)
    today = datetime.utcnow().date()
    start_date = parse_date(config["start_date"], today)
    end_date = parse_date(config["end_date"], start_date + timedelta(days=2))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_responses = {
        city["name"]: fetch_city_weather(city, start_date.isoformat(), end_date.isoformat())
        for city in config["cities"]
    }

    RAW_DATA_FILE.write_text(json.dumps(raw_responses, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Données brutes enregistrées dans {RAW_DATA_FILE}")
    return str(RAW_DATA_FILE)


def transform_weather() -> str:
    raw_text = RAW_DATA_FILE.read_text(encoding="utf-8")
    raw_records = json.loads(raw_text)
    transformed_rows: List[Dict[str, Any]] = []

    for city_name, response in raw_records.items():
        if "daily" not in response:
            print(f"Pas de données journalières pour {city_name}")
            continue

        daily = response["daily"]
        timezone = response.get("timezone", "Europe/Paris")

        for index, record_date in enumerate(daily["time"]):
            transformed_rows.append(
                {
                    "city": city_name,
                    "date": record_date,
                    "max_temperature_c": daily["temperature_2m_max"][index],
                    "min_temperature_c": daily["temperature_2m_min"][index],
                    "precipitation_mm": daily["precipitation_sum"][index],
                    "timezone": timezone,
                }
            )

    PROCESSED_FILE.write_text(json.dumps(transformed_rows, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Données traitées enregistrées dans {PROCESSED_FILE}")
    return str(PROCESSED_FILE)


def create_db_tables(dag_run: Any = None) -> str:
    config = get_pipeline_config(dag_run)
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError as exc:
        raise ImportError(
            "Le provider PostgreSQL Airflow n'est pas installé. "
            "Installez apache-airflow-providers-postgres dans le venv."
        ) from exc

    hook = PostgresHook(postgres_conn_id=config["postgres_conn_id"])
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {config['weather_table']} (
                    city TEXT NOT NULL,
                    date DATE NOT NULL,
                    max_temperature_c DOUBLE PRECISION,
                    min_temperature_c DOUBLE PRECISION,
                    precipitation_mm DOUBLE PRECISION,
                    timezone TEXT,
                    ingestion_ts TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (city, date)
                )
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {config['audit_table']} (
                    id SERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    execution_date TIMESTAMPTZ NOT NULL,
                    city_count INTEGER,
                    row_count INTEGER,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
        conn.commit()

    print(f"Tables PostgreSQL créées : {config['weather_table']} et {config['audit_table']}")
    return config["postgres_conn_id"]


def load_to_database(dag_run: Any = None) -> Dict[str, Any]:
    config = get_pipeline_config(dag_run)
    processed_text = PROCESSED_FILE.read_text(encoding="utf-8")
    rows = json.loads(processed_text)

    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError as exc:
        raise ImportError(
            "Le provider PostgreSQL Airflow n'est pas installé. "
            "Installez apache-airflow-providers-postgres dans le venv."
        ) from exc

    if not rows:
        raise ValueError("Aucune donnée disponible pour le chargement en base.")

    hook = PostgresHook(postgres_conn_id=config["postgres_conn_id"])
    inserted = 0

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            insert_sql = f"""
                INSERT INTO {config['weather_table']} (
                    city, date, max_temperature_c, min_temperature_c, precipitation_mm, timezone
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (city, date) DO UPDATE SET
                    max_temperature_c = EXCLUDED.max_temperature_c,
                    min_temperature_c = EXCLUDED.min_temperature_c,
                    precipitation_mm = EXCLUDED.precipitation_mm,
                    timezone = EXCLUDED.timezone,
                    ingestion_ts = NOW()
            """
            for row in rows:
                cursor.execute(
                    insert_sql,
                    [
                        row["city"],
                        row["date"],
                        row["max_temperature_c"],
                        row["min_temperature_c"],
                        row["precipitation_mm"],
                        row["timezone"],
                    ],
                )
                inserted += 1
        conn.commit()

    city_count = len({row["city"] for row in rows})
    print(f"{inserted} lignes chargées dans la table {config['weather_table']}")
    return {
        "row_count": inserted,
        "city_count": city_count,
        "status": "success",
        "message": "Chargement en base réalisé avec succès.",
    }


def audit_pipeline_run(dag_run: Any = None, ti: Any = None) -> str:
    config = get_pipeline_config(dag_run)
    load_result = ti.xcom_pull(task_ids="load_to_database") or {}
    run_id = dag_run.run_id if dag_run else "manual"
    execution_date = dag_run.execution_date if dag_run else datetime.utcnow()
    row_count = load_result.get("row_count", 0)
    city_count = load_result.get("city_count", 0)
    status = load_result.get("status", "error")
    message = load_result.get("message", "")

    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError as exc:
        raise ImportError(
            "Le provider PostgreSQL Airflow n'est pas installé. "
            "Installez apache-airflow-providers-postgres dans le venv."
        ) from exc

    hook = PostgresHook(postgres_conn_id=config["postgres_conn_id"])
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {config['audit_table']} (
                    run_id, execution_date, city_count, row_count, status, message
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [run_id, execution_date, city_count, row_count, status, message],
            )
        conn.commit()

    print(f"Audit enregistré : {status}, {row_count} lignes, {city_count} villes")
    return "ok"


with DAG(
    dag_id="weather_db_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    params=PIPELINE_CONFIG,
    tags=["tp", "airflow", "meteo", "database"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
        op_kwargs={"dag_run": "{{ dag_run }}"},
    )

    task_transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    task_ensure_tables = PythonOperator(
        task_id="create_db_tables",
        python_callable=create_db_tables,
        op_kwargs={"dag_run": "{{ dag_run }}"},
    )

    task_load = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
        op_kwargs={"dag_run": "{{ dag_run }}"},
    )

    task_track = PythonOperator(
        task_id="audit_pipeline_run",
        python_callable=audit_pipeline_run,
        op_kwargs={"dag_run": "{{ dag_run }}"},
    )

    task_extract >> task_transform >> task_ensure_tables >> task_load >> task_track
