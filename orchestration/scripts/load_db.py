"""
load_db.py
----------
Fungsi Airflow task callable untuk memuat CSV dari RAW_PATH
ke PostgreSQL raw schema.
"""

from __future__ import annotations

import logging
import os
from importlib import import_module
from datetime import datetime
from typing import Any, Iterable, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

RAW_PATH = "/opt/airflow/tmp"
DIMENSION_TABLES = [
    "customerCategory",
    "customer",
    "warehouse",
    "itemCategory",
    "item",
]


def _get_postgres_hook_class() -> Any:
    """Load PostgresHook lazily to keep module import-safe outside Airflow runtime."""
    candidates = [
        "airflow.providers.postgres.hooks.postgres",
        "airflow.hooks.postgres_hook",
    ]
    for module_name in candidates:
        try:
            module = import_module(module_name)
            return getattr(module, "PostgresHook")
        except Exception:
            continue

    raise ImportError("PostgresHook could not be imported. Ensure Airflow Postgres provider is installed.")


def _run_date_tag(data_interval_start: Any) -> str:
    """Build YYYYMMDD date tag from Airflow interval start."""
    return _coerce_datetime(data_interval_start).strftime("%Y%m%d")


def _invoice_date_tags(data_interval_start: Any, data_interval_end: Any) -> Tuple[str, str]:
    """Build YYYYMMDD date tags for invoice CSV naming."""
    start_dt = _coerce_datetime(data_interval_start)
    end_dt = _coerce_datetime(data_interval_end)
    return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")


def _coerce_datetime(value: Any) -> datetime:
    """Convert Airflow templated interval values into datetime."""
    if isinstance(value, datetime):
        return value

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid datetime value: {value}")
    return parsed.to_pydatetime()


def _read_csv_or_raise(file_path: str) -> pd.DataFrame:
    """Load CSV file or raise clear FileNotFoundError when missing."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    return pd.read_csv(file_path)


def _parse_datetime_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    fmt: Optional[str] = None,
) -> None:
    """Parse dataframe columns into datetime with tolerant coercion."""
    for col in columns:
        if col in df.columns:
            if fmt:
                df[col] = pd.to_datetime(
                    df[col],
                    format=fmt,
                    dayfirst=True,
                    errors="coerce",
                )
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce")


def load_dimensions_to_db(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Load full-refresh dimension CSV files into raw schema using truncate-then-insert."""
    del data_interval_end

    PostgresHook = _get_postgres_hook_class()
    hook = PostgresHook(postgres_conn_id="olap-db")
    engine = hook.get_sqlalchemy_engine()
    date_tag = _run_date_tag(data_interval_start)

    for table_name in DIMENSION_TABLES:
        file_path = os.path.join(RAW_PATH, f"{table_name}_{date_tag}.csv")
        df = _read_csv_or_raise(file_path)

        if df.empty:
            logger.warning("[load_dimensions] %s is empty, truncating and skipping insert.", table_name)
            hook.run(f'TRUNCATE TABLE raw."{table_name}";')
            continue

        _parse_datetime_columns(df, ["extractedAt"])

        hook.run(f'TRUNCATE TABLE raw."{table_name}";')
        df.to_sql(
            name=table_name,
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )
        logger.info("[load_dimensions] loaded %d rows into raw.\"%s\"", len(df), table_name)


def load_invoice_to_db(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Load interval invoice CSV files into raw schema with delete-then-insert idempotency."""
    PostgresHook = _get_postgres_hook_class()
    hook = PostgresHook(postgres_conn_id="olap-db")
    engine = hook.get_sqlalchemy_engine()

    start_tag, end_tag = _invoice_date_tags(data_interval_start, data_interval_end)
    invoice_file = os.path.join(RAW_PATH, f"invoice_{start_tag}_{end_tag}.csv")
    detail_file = os.path.join(RAW_PATH, f"invoiceDetail_{start_tag}_{end_tag}.csv")

    invoice_df = _read_csv_or_raise(invoice_file)
    detail_df = _read_csv_or_raise(detail_file)

    _parse_datetime_columns(invoice_df, ["transDate", "dueDate", "shipDate"], fmt="%d/%m/%Y")
    _parse_datetime_columns(invoice_df, ["printedTime"], fmt="%d/%m/%Y, %H:%M")
    _parse_datetime_columns(invoice_df, ["extractedAt"])
    _parse_datetime_columns(detail_df, ["extractedAt"])

    start_db = _coerce_datetime(data_interval_start).strftime("%Y-%m-%d")
    end_db = _coerce_datetime(data_interval_end).strftime("%Y-%m-%d")

    # Delete detail rows first, then invoice rows, to keep reruns idempotent.
    hook.run(
        f"""
        DELETE FROM raw."invoiceDetail"
        WHERE "invoiceId" IN (
            SELECT "invoiceId"
            FROM raw."invoice"
            WHERE "transDate" BETWEEN '{start_db}' AND '{end_db}'
        );

        DELETE FROM raw."invoice"
        WHERE "transDate" BETWEEN '{start_db}' AND '{end_db}';
        """
    )

    if invoice_df.empty:
        logger.warning("[load_invoice] invoice CSV is empty for interval %s-%s.", start_tag, end_tag)
    else:
        invoice_df.to_sql(
            name="invoice",
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )
        logger.info("[load_invoice] loaded %d rows into raw.\"invoice\"", len(invoice_df))

    if detail_df.empty:
        logger.warning("[load_invoice] invoiceDetail CSV is empty for interval %s-%s.", start_tag, end_tag)
    else:
        detail_df.to_sql(
            name="invoiceDetail",
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )
        logger.info("[load_invoice] loaded %d rows into raw.\"invoiceDetail\"", len(detail_df))
