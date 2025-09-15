# `dags/aviationstack_departures_gig.py`
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from pendulum import timezone

SAO_PAULO_TZ = timezone("America/Sao_Paulo")

# ---------------- Basic Config ----------------
DEP_IATA = "GIG"                  # Rio de Janeiro International Airport (Galeão)
API_BASE = "http://api.aviationstack.com/v1/flights"
LIMIT = 100                       # plan free limit per request
MAX_PAGES = 5                     # safety limit to avoid infinite pagination
OUTPUT_DIR = "/opt/airflow/data"  # mounted volume
OUTPUT_FILE = "Departures.csv"


@dag(
    dag_id="aviationstack_departures_gig",
    description="Extract GIG departures from Aviationstack, transform with pandas, and save CSV.",
    # Use a fixed start_date; catchup disabled for simplicity
    start_date=datetime(2025, 9, 1, tzinfo=SAO_PAULO_TZ),
    schedule="0 6 * * *",  # every day at 06:00 (America/Sao_Paulo)
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["aviationstack", "flights", "departures", "etl", "gig"],
)
def aviationstack_departures_gig():
    @task
    def extract() -> list[dict]:
        """
        Fetches paginated responses from Aviationstack using simple offset-based pagination.

        Notes:
        - API key is read from environment variable 'AVIATIONSTACK_API_KEY'.
        """
        api_key = os.getenv("AVIATIONSTACK_API_KEY")
        if not api_key:
            raise AirflowFailException(
                "Missing API key. Set 'AVIATIONSTACK_API_KEY' in the environment."
            )

        all_records: list[dict] = []
        offset = 0
        pages = 0

        while pages < MAX_PAGES:
            params = {
                "access_key": api_key,
                "dep_iata": DEP_IATA,
                "limit": LIMIT,
                "offset": offset,
            }
            resp = requests.get(API_BASE, params=params, timeout=60)
            logging.info("Request URL: %s", resp.url)
            if resp.status_code != 200:
                raise AirflowFailException(
                    f"HTTP {resp.status_code}: {resp.text[:300]}"
                )

            payload = resp.json()
            if "data" not in payload or not isinstance(payload["data"], list):
                raise AirflowFailException(
                    f"Unexpected API response: {json.dumps(payload)[:500]}"
                )

            batch = payload["data"]
            logging.info("Page %s: %s records", pages + 1, len(batch))
            if not batch:
                break

            all_records.extend(batch)

            # Stop when we receive a short page (last page)
            if len(batch) < LIMIT:
                break

            pages += 1
            offset += LIMIT

        if not all_records:
            raise AirflowFailException("API returned no data.")
        return all_records

    @task
    def transform(records: list[dict]) -> str:
        """
        Normalizes nested JSON to a pandas DataFrame, performs codeshare mapping,
        builds date/time columns, selects final columns, and writes CSV.
        Returns the CSV file path.
        """
        # --- Normalize nested JSON ---
        df = pd.json_normalize(records)

        logging.info("Columns: %s", list(df.columns))
        if "flight_status" in df.columns:
            logging.info("Unique flight_status: %s", df["flight_status"].unique())

        # --- Codeshare mapping ---
        # Build a map from operated flight (flight.icao) -> "Airline / Flight Number" partners
        if "flight.codeshared.flight_icao" in df.columns:
            ref = (
                df.loc[
                    df["flight.codeshared.flight_icao"].notna(),
                    ["flight.codeshared.flight_icao", "airline.name", "flight.number"],
                ]
                .copy()
            )
            ref["pair"] = (
                ref["airline.name"].fillna("").astype(str)
                + " "
                + ref["flight.number"].astype(str)
            )

            agg = (
                ref.groupby(ref["flight.codeshared.flight_icao"].astype(str).str.upper())["pair"]
                .apply(lambda s: " / ".join(sorted(set(s))))
                .reset_index()
                .rename(columns={"flight.codeshared.flight_icao": "flight.icao", "pair": "codeshare"})
            )

            if "flight.icao" in df.columns:
                df["flight.icao"] = df["flight.icao"].astype(str).str.upper()
                df = df.merge(agg, how="left", on="flight.icao")
                # Keep 'codeshare' only on operated flights (where codeshared is null)
                df.loc[df["flight.codeshared.flight_icao"].notna(), "codeshare"] = pd.NA
            else:
                logging.warning("Missing 'flight.icao'; codeshare merge skipped.")
        else:
            logging.warning("Missing 'flight.codeshared.flight_icao'; codeshare mapping skipped.")

        # --- Handling null values ---
        if "codeshare" in df.columns:
            before = len(df)
            df = df.dropna(subset=["codeshare"])
            logging.info("Filtered by non-null codeshare: %s -> %s rows", before, len(df))
        else:
            logging.warning("'codeshare' not found; skipping null filter.")

        # --- Datetime conversions and split into date/time columns ---
        for col in ["departure.scheduled", "departure.estimated", "departure.actual"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if "departure.scheduled" in df.columns:
            df["scheduled_date"] = df["departure.scheduled"].dt.date
            df["scheduled_time"] = df["departure.scheduled"].dt.time
        if "departure.estimated" in df.columns:
            df["estimated_date"] = df["departure.estimated"].dt.date
            df["estimated_time"] = df["departure.estimated"].dt.time
        if "departure.actual" in df.columns:
            df["actual_date"] = df["departure.actual"].dt.date
            df["actual_time"] = df["departure.actual"].dt.time

        # --- Final column selection (Departure related) ---
        columns_to_keep = [
            "flight_status",
            "departure.airport",
            "departure.timezone",
            "departure.iata",
            "departure.icao",
            "departure.terminal",
            "departure.gate",
            "departure.delay",
            "arrival.airport",
            "arrival.iata",
            "arrival.icao",
            "airline.name",
            "airline.iata",
            "airline.icao",
            "flight.number",
            "flight.iata",
            "flight.icao",
            "codeshare",
            "scheduled_date",
            "scheduled_time",
            "estimated_date",
            "estimated_time",
            "actual_date",
            "actual_time",
        ]
        keep = [c for c in columns_to_keep if c in df.columns]
        missing = [c for c in columns_to_keep if c not in df.columns]
        if missing:
            logging.warning("Missing columns: %s", missing)

        df = df[keep].reset_index(drop=True)

        # --- Write CSV ---
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        df.to_csv(out_path, index=False)
        logging.info("CSV written: %s (rows=%s, cols=%s)", out_path, len(df), len(df.columns))
        return out_path

    @task
    def load(csv_path: str):
        """Logs the final CSV path for visibility."""
        logging.info("Pipeline finished. Output: %s", csv_path)

    # Orchestration
    records = extract()
    csv_path = transform(records)
    load(csv_path)


_ = aviationstack_departures_gig()
