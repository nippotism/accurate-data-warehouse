
import logging
import os
import time
from datetime import datetime

from anyio import Path
from anyio import Path
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_nested(obj: dict, *keys, default=None):
    """
    Safely traverse a nested dictionary.

    Example:
        get_nested(record, "currency", "code")  →  "IDR" or None
    """
    current = obj
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def calculate_age(date_str: str, fmt: str = "%d/%m/%Y") -> int | None:
    """
    Calculate the number of days from a date string to today.

    Returns None if date_str is None or unparseable.
    """
    if not date_str:
        return None
    try:
        parsed = datetime.strptime(date_str, fmt)
        return (datetime.now() - parsed).days
    except (ValueError, TypeError):
        return None


def fetch_list(endpoint: str, extra_params: dict = {}) -> list[dict]:
    """
    Fetch all pages from a list endpoint via POST requests.

    Paginates until an empty `d` array is returned.
    Returns a flat list of record dicts.
    """
    url = f"{BASE_URL}{endpoint}"
    all_records: list[dict] = []
    page = 1

    while True:
        params = {
            "sp.page":     page,
            "sp.pageSize": PAGE_SIZE,
            **extra_params,
        }
        try:
            response = requests.post(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.warning(
                "fetch_list | endpoint=%s page=%d | Request error: %s",
                endpoint, page, e,
            )
            break

        records = data.get("d", [])
        if not records:
            break

        all_records.extend(records)
        logger.info(
            "fetch_list | endpoint=%s | page=%d fetched %d records (total so far: %d)",
            endpoint, page, len(records), len(all_records),
        )

        page += 1
        time.sleep(RATE_LIMIT_LIST_SLEEP)

    return all_records


def fetch_detail(endpoint: str, record_id) -> dict | None:
    """
    Fetch a single detail record via GET request.

    Returns the parsed JSON dict, or None on failure.
    """
    url = f"{BASE_URL}{endpoint}"
    params = {"id": record_id}
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(
            "fetch_detail | endpoint=%s id=%s | Request error: %s",
            endpoint, record_id, e,
        )
        return None


def load_to_db(df: pd.DataFrame, table_name: str, engine: Engine) -> None:
    """
    Load a DataFrame into the raw schema of PostgreSQL.

    Uses append mode; table must already exist (DDL via init.sql).
    """
    if df.empty:
        logger.warning("load_to_db | table=raw.\"%s\" | DataFrame is empty, skipping load.", table_name)
        return

    logger.info("load_to_db | table=raw.\"%s\" | Starting load of %d rows...", table_name, len(df))
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
        )
        logger.info("load_to_db | table=raw.\"%s\" | Load complete. %d rows inserted.", table_name, len(df))
    except Exception as e:
        logger.error("load_to_db | table=raw.\"%s\" | Load failed: %s", table_name, e)
        raise

