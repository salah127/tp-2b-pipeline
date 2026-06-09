import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_FILE = DATA_DIR / "raw_weather.json"
PROCESSED_FILE = DATA_DIR / "processed_weather.json"
OUTPUT_CSV = DATA_DIR / "weather_results.csv"

CITIES = [
    {"name": "Strasbourg", "latitude": 48.5734, "longitude": 7.7521},
    {"name": "Rennes", "latitude": 48.1173, "longitude": -1.6778},
    {"name": "Nice", "latitude": 43.7102, "longitude": 7.2620},
]

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"


def build_open_meteo_url(latitude: float, longitude: float, start_date: str, end_date: str) -> str:
    return (
        f"{API_BASE_URL}?latitude={latitude}&longitude={longitude}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=Europe%2FParis&start_date={start_date}&end_date={end_date}"
    )


def fetch_city_weather(city: dict, start_date: str, end_date: str) -> dict:
    url = build_open_meteo_url(city["latitude"], city["longitude"], start_date, end_date)
    request = Request(url, headers={"User-Agent": "airflow-open-meteo"})
    try:
        with urlopen(request, timeout=20) as response:
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


def extract_weather() -> str:
    today = datetime.utcnow().date()
    end_date = today + timedelta(days=2)
    start_date = today.isoformat()
    end_date = end_date.isoformat()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    raw_responses = {
        city["name"]: fetch_city_weather(city, start_date, end_date)
        for city in CITIES
    }

    RAW_FILE.write_text(json.dumps(raw_responses, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Données brutes enregistrées dans {RAW_FILE}")
    return str(RAW_FILE)


def transform_weather() -> str:
    raw_text = RAW_FILE.read_text(encoding="utf-8")
    raw_records = json.loads(raw_text)

    transformed_rows = []

    for city_name, response in raw_records.items():
        if "daily" not in response:
            print(f"Pas de données journalières pour {city_name}.")
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


def save_to_csv() -> str:
    processed_text = PROCESSED_FILE.read_text(encoding="utf-8")
    rows = json.loads(processed_text)

    if not rows:
        raise ValueError("Aucune donnée disponible pour l'export CSV.")

    header = [
        "city",
        "date",
        "max_temperature_c",
        "min_temperature_c",
        "precipitation_mm",
        "timezone",
    ]

    with OUTPUT_CSV.open("w", encoding="utf-8") as output:
        output.write(",".join(header) + "\n"
        )
        for row in rows:
            output.write(",".join(
                [
                    str(row["city"]),
                    str(row["date"]),
                    str(row["max_temperature_c"]),
                    str(row["min_temperature_c"]),
                    str(row["precipitation_mm"]),
                    str(row["timezone"]),
                ]
            ) + "\n"
        )

    print(f"Fichier CSV généré dans {OUTPUT_CSV}")
    return str(OUTPUT_CSV)


with DAG(
    dag_id="weather_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tp", "airflow", "meteo"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    task_transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    task_load = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_to_csv,
    )

    task_extract >> task_transform >> task_load
