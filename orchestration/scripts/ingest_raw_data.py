"""
ingest_raw_data.py
------------------
Fungsi Airflow task callable untuk mengekstrak data dari Accurate API
dan menyimpan sebagai CSV ke RAW_PATH (staging area sementara).

Setiap fungsi adalah task callable yang menerima Airflow context:
    data_interval_start, data_interval_end, **kwargs
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable

logger = logging.getLogger(__name__)

RAW_PATH = "/opt/airflow/tmp"
PAGE_SIZE = 100
LIST_SLEEP = 1.0
DETAIL_SLEEP = 1.5
BATCH_SIZE = 5


def get_accurate_config() -> Dict[str, str]:
    """Get Accurate API configuration from Airflow connection and variables."""
    conn = BaseHook.get_connection("accurate-api")
    extra = conn.extra_dejson or {}

    token = Variable.get("accurate_access_token")
    session_id = Variable.get("accurate_db_session")

    db_url = extra.get("db_url")
    if not db_url:
        raise ValueError("Connection 'accurate-api' must include 'db_url' in extra.")

    return {
        "token": token,
        "session_id": session_id,
        "db_url": db_url.rstrip("/"),
    }


def build_headers(token: str, session_id: str) -> Dict[str, str]:
    """Build shared headers for Accurate API requests."""
    return {
        "Authorization": f"Bearer {token}",
        "X-Session-ID": session_id,
        "Content-Type": "application/json",
    }


def get_nested(obj: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely traverse nested dictionary keys and return default when missing."""
    current: Any = obj
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def calculate_age(date_str: Optional[str], fmt: str = "%d/%m/%Y") -> Optional[int]:
    """Calculate day age from date string to current date."""
    if not date_str:
        return None
    try:
        parsed = datetime.strptime(date_str, fmt)
        return (datetime.now() - parsed).days
    except (ValueError, TypeError):
        return None


def fetch_list(
    db_url: str,
    headers: Dict[str, str],
    endpoint: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all pages from an Accurate list endpoint using POST."""
    url = f"{db_url}{endpoint}"
    all_records: List[Dict[str, Any]] = []
    page = 1
    params_override = extra_params or {}

    while True:
        params = {
            "sp.page": page,
            "sp.pageSize": PAGE_SIZE,
            **params_override,
        }
        try:
            response = requests.post(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.warning(
                "fetch_list | endpoint=%s page=%d | Request error: %s",
                endpoint,
                page,
                exc,
            )
            break

        records = data.get("d", [])
        if not records:
            break

        all_records.extend(records)
        logger.info(
            "fetch_list | endpoint=%s | page=%d fetched %d records (total: %d)",
            endpoint,
            page,
            len(records),
            len(all_records),
        )

        page += 1
        time.sleep(LIST_SLEEP)

    return all_records


def fetch_detail(
    db_url: str,
    headers: Dict[str, str],
    endpoint: str,
    record_id: Any,
) -> Optional[Dict[str, Any]]:
    """Fetch one Accurate detail record using GET."""
    url = f"{db_url}{endpoint}"
    try:
        response = requests.get(url, headers=headers, params={"id": record_id}, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        logger.warning(
            "fetch_detail | endpoint=%s id=%s | Request error: %s",
            endpoint,
            record_id,
            exc,
        )
        return None


def _ensure_raw_path() -> None:
    """Ensure local staging directory exists in Airflow container."""
    os.makedirs(RAW_PATH, exist_ok=True)


def _coerce_datetime(value: Any) -> datetime:
    """Convert Airflow templated interval values into datetime."""
    if isinstance(value, datetime):
        return value

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid datetime value: {value}")
    return parsed.to_pydatetime()


def _run_date_tag(data_interval_start: Any) -> str:
    """Build YYYYMMDD date tag for daily CSV naming."""
    return _coerce_datetime(data_interval_start).strftime("%Y%m%d")


def _interval_to_accurate_date(data_interval_dt: Any) -> str:
    """Convert Airflow interval datetime into Accurate API DD/MM/YYYY string."""
    return _coerce_datetime(data_interval_dt).strftime("%d/%m/%Y")


def _save_csv(rows: List[Dict[str, Any]], filename: str) -> str:
    """Persist rows to RAW_PATH CSV and return saved path."""
    _ensure_raw_path()
    file_path = os.path.join(RAW_PATH, filename)
    df = pd.DataFrame(rows)
    df.to_csv(file_path, index=False)
    logger.info("Saved %d rows to %s", len(df), file_path)
    return file_path


def ingest_customer_category(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Ingest customer categories (full refresh) and persist CSV staging file."""
    del data_interval_end

    config = get_accurate_config()
    headers = build_headers(config["token"], config["session_id"])
    endpoint = "/accurate/api/customer-category/list.do"

    raw_records = fetch_list(config["db_url"], headers, endpoint)
    logger.info("[customerCategory] total fetched: %d", len(raw_records))

    rows: List[Dict[str, Any]] = []
    extracted_at = datetime.now()
    for record in raw_records:
        try:
            rows.append(
                {
                    "id": record.get("id"),
                    "name": record.get("name"),
                    "defaultCategory": record.get("defaultCategory"),
                    "parentName": record.get("parentName"),
                    "extractedAt": extracted_at,
                }
            )
        except Exception as exc:
            logger.warning(
                "[customerCategory] skip id=%s | error=%s",
                record.get("id"),
                exc,
            )

    if not rows:
        logger.warning("[customerCategory] No rows collected from API.")

    file_name = f"customerCategory_{_run_date_tag(data_interval_start)}.csv"
    _save_csv(rows, file_name)


def ingest_customer(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Ingest customers (full refresh) and persist CSV staging file."""
    del data_interval_end

    config = get_accurate_config()
    headers = build_headers(config["token"], config["session_id"])
    endpoint = "/accurate/api/customer/list.do"

    raw_records = fetch_list(config["db_url"], headers, endpoint)
    logger.info("[customer] total fetched: %d", len(raw_records))

    rows: List[Dict[str, Any]] = []
    extracted_at = datetime.now()
    for record in raw_records:
        try:
            rows.append(
                {
                    "id": record.get("id"),
                    "name": record.get("name"),
                    "customerNo": record.get("customerNo"),
                    "categoryName": record.get("categoryName"),
                    "email": record.get("email"),
                    "mobilePhone": record.get("mobilePhone"),
                    "workPhone": record.get("workPhone"),
                    "npwpNo": record.get("npwpNo"),
                    "pkpNo": record.get("pkpNo"),
                    "billStreet": get_nested(record, "billAddress", "street"),
                    "billCity": get_nested(record, "billAddress", "city"),
                    "billProvince": get_nested(record, "billAddress", "province"),
                    "billCountry": get_nested(record, "billAddress", "country"),
                    "billZipCode": get_nested(record, "billAddress", "zipCode"),
                    "currencyCode": get_nested(record, "currency", "code"),
                    "termName": get_nested(record, "term", "name"),
                    "defaultIncTax": record.get("defaultIncTax"),
                    "customerLimitAge": record.get("customerLimitAge"),
                    "customerLimitAgeValue": record.get("customerLimitAgeValue"),
                    "customerLimitAmount": record.get("customerLimitAmount"),
                    "customerLimitAmountValue": record.get("customerLimitAmountValue"),
                    "priceCategoryName": get_nested(record, "priceCategory", "name"),
                    "discountCategoryName": get_nested(record, "discountCategory", "name"),
                    "branchId": record.get("branchId"),
                    "branchName": record.get("branchName"),
                    "extractedAt": extracted_at,
                }
            )
        except Exception as exc:
            logger.warning("[customer] skip id=%s | error=%s", record.get("id"), exc)

    if not rows:
        logger.warning("[customer] No rows collected from API.")

    file_name = f"customer_{_run_date_tag(data_interval_start)}.csv"
    _save_csv(rows, file_name)


def ingest_warehouse(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Ingest warehouses (full refresh) and persist CSV staging file."""
    del data_interval_end

    config = get_accurate_config()
    headers = build_headers(config["token"], config["session_id"])
    endpoint = "/accurate/api/warehouse/list.do"

    raw_records = fetch_list(config["db_url"], headers, endpoint)
    logger.info("[warehouse] total fetched: %d", len(raw_records))

    rows: List[Dict[str, Any]] = []
    extracted_at = datetime.now()
    for record in raw_records:
        try:
            rows.append(
                {
                    "id": record.get("id"),
                    "name": record.get("name"),
                    "street": record.get("street"),
                    "city": record.get("city"),
                    "province": record.get("province"),
                    "country": record.get("country"),
                    "zipCode": record.get("zipCode"),
                    "pic": record.get("pic"),
                    "scrapWarehouse": record.get("scrapWarehouse"),
                    "suspended": record.get("suspended"),
                    "extractedAt": extracted_at,
                }
            )
        except Exception as exc:
            logger.warning("[warehouse] skip id=%s | error=%s", record.get("id"), exc)

    if not rows:
        logger.warning("[warehouse] No rows collected from API.")

    file_name = f"warehouse_{_run_date_tag(data_interval_start)}.csv"
    _save_csv(rows, file_name)


def ingest_item_category(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Ingest item categories (full refresh) and persist CSV staging file."""
    del data_interval_end

    config = get_accurate_config()
    headers = build_headers(config["token"], config["session_id"])
    endpoint = "/accurate/api/item-category/list.do"

    raw_records = fetch_list(config["db_url"], headers, endpoint)
    logger.info("[itemCategory] total fetched: %d", len(raw_records))

    rows: List[Dict[str, Any]] = []
    extracted_at = datetime.now()
    for record in raw_records:
        try:
            rows.append(
                {
                    "id": record.get("id"),
                    "name": record.get("name"),
                    "defaultCategory": record.get("defaultCategory"),
                    "parentName": record.get("parentName"),
                    "extractedAt": extracted_at,
                }
            )
        except Exception as exc:
            logger.warning("[itemCategory] skip id=%s | error=%s", record.get("id"), exc)

    if not rows:
        logger.warning("[itemCategory] No rows collected from API.")

    file_name = f"itemCategory_{_run_date_tag(data_interval_start)}.csv"
    _save_csv(rows, file_name)


def ingest_item(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Ingest items (full refresh) and persist CSV staging file."""
    del data_interval_end

    config = get_accurate_config()
    headers = build_headers(config["token"], config["session_id"])
    endpoint = "/accurate/api/item/list.do"

    raw_records = fetch_list(config["db_url"], headers, endpoint)
    logger.info("[item] total fetched: %d", len(raw_records))

    rows: List[Dict[str, Any]] = []
    extracted_at = datetime.now()
    for record in raw_records:
        try:
            rows.append(
                {
                    "id": record.get("id"),
                    "no": record.get("no"),
                    "name": record.get("name"),
                    "itemType": record.get("itemType"),
                    "itemCategoryName": get_nested(record, "itemCategory", "name"),
                    "upcNo": record.get("upcNo"),
                    "unitPrice": record.get("unitPrice"),
                    "vendorPrice": record.get("vendorPrice"),
                    "unit1Name": get_nested(record, "unit1", "name"),
                    "vendorUnitName": get_nested(record, "vendorUnit", "name"),
                    "defaultDiscount": record.get("defaultDiscount"),
                    "usePpn": record.get("usePpn"),
                    "manageSN": record.get("manageSN"),
                    "controlQuantity": record.get("controlQuantity"),
                    "salesGlAccountNo": get_nested(record, "salesGlAccount", "no"),
                    "cogsGlAccountNo": get_nested(record, "cogsGlAccount", "no"),
                    "inventoryGlAccountNo": get_nested(record, "inventoryGlAccount", "no"),
                    "weight": record.get("weight"),
                    "notes": record.get("notes"),
                    "extractedAt": extracted_at,
                }
            )
        except Exception as exc:
            logger.warning("[item] skip id=%s | error=%s", record.get("id"), exc)

    if not rows:
        logger.warning("[item] No rows collected from API.")

    file_name = f"item_{_run_date_tag(data_interval_start)}.csv"
    _save_csv(rows, file_name)


def ingest_invoice(data_interval_start: Any, data_interval_end: Any, **_: Any) -> None:
    """Ingest invoice headers and details for Airflow interval and persist two CSV files."""
    config = get_accurate_config()
    headers = build_headers(config["token"], config["session_id"])

    list_endpoint = "/accurate/api/sales-invoice/list.do"
    detail_endpoint = "/accurate/api/sales-invoice/detail.do"

    start_filter = _interval_to_accurate_date(data_interval_start)
    end_filter = _interval_to_accurate_date(data_interval_end)
    logger.info("[invoice] Start ingest interval %s -> %s", start_filter, end_filter)

    filter_params = {
        "filter.transDate.op": "BETWEEN",
        "filter.transDate.val[0]": start_filter,
        "filter.transDate.val[1]": end_filter,
    }

    invoice_list = fetch_list(config["db_url"], headers, list_endpoint, extra_params=filter_params)
    invoice_ids = [inv.get("id") for inv in invoice_list if inv.get("id") is not None]
    logger.info("[invoice] total ids to fetch: %d", len(invoice_ids))

    extracted_at = datetime.now()
    invoice_rows: List[Dict[str, Any]] = []
    invoice_detail_rows: List[Dict[str, Any]] = []

    for batch_start in range(0, len(invoice_ids), BATCH_SIZE):
        batch = invoice_ids[batch_start : batch_start + BATCH_SIZE]

        for invoice_id in batch:
            try:
                detail_data = fetch_detail(config["db_url"], headers, detail_endpoint, invoice_id)
                if detail_data is None:
                    logger.warning("[invoice] No detail for id=%s, skipping.", invoice_id)
                    continue

                trans_date_str = detail_data.get("transDate")
                invoice_rows.append(
                    {
                        "invoiceId": detail_data.get("id"),
                        "invoiceNumber": detail_data.get("number"),
                        "transDate": trans_date_str,
                        "dueDate": detail_data.get("dueDate"),
                        "shipDate": detail_data.get("shipDate"),
                        "customerId": get_nested(detail_data, "customer", "id"),
                        "customerName": get_nested(detail_data, "customer", "name"),
                        "subTotal": detail_data.get("subTotal"),
                        "totalAmount": detail_data.get("totalAmount"),
                        "outstanding": detail_data.get("outstanding"),
                        "status": detail_data.get("status"),
                        "approvalStatus": detail_data.get("approvalStatus"),
                        "poNumber": detail_data.get("poNumber"),
                        "salesOrderId": detail_data.get("salesOrderId"),
                        "deliveryOrderId": detail_data.get("deliveryOrderId"),
                        "paymentTermId": get_nested(detail_data, "paymentTerm", "id"),
                        "paymentTermName": get_nested(detail_data, "paymentTerm", "name"),
                        "currencyId": get_nested(detail_data, "currency", "id"),
                        "currencyCode": get_nested(detail_data, "currency", "code"),
                        "rate": detail_data.get("rate"),
                        "branchId": detail_data.get("branchId"),
                        "branchName": detail_data.get("branchName"),
                        "invoiceAgeDays": calculate_age(trans_date_str, fmt="%d/%m/%Y"),
                        "createdBy": detail_data.get("createdBy"),
                        "printedTime": detail_data.get("printedTime"),
                        "extractedAt": extracted_at,
                    }
                )

                detail_items = detail_data.get("detailItem", []) or []
                for detail in detail_items:
                    try:
                        invoice_detail_rows.append(
                            {
                                "detailId": detail.get("id"),
                                "invoiceId": detail_data.get("id"),
                                "invoiceNumber": detail_data.get("number"),
                                "itemId": get_nested(detail, "item", "id"),
                                "itemNo": get_nested(detail, "item", "no"),
                                "itemName": get_nested(detail, "item", "name"),
                                "itemCategoryId": get_nested(detail, "item", "itemCategoryId"),
                                "quantity": detail.get("quantity"),
                                "unitId": get_nested(detail, "itemUnit", "id"),
                                "unitName": get_nested(detail, "itemUnit", "name"),
                                "unitRatio": detail.get("unitRatio"),
                                "unitPrice": detail.get("unitPrice"),
                                "grossAmount": detail.get("grossAmount"),
                                "salesAmount": detail.get("salesAmount"),
                                "warehouseId": get_nested(detail, "warehouse", "id"),
                                "warehouseName": get_nested(detail, "warehouse", "name"),
                                "salesOrderDetailId": detail.get("salesOrderDetailId"),
                                "deliveryOrderDetailId": detail.get("deliveryOrderDetailId"),
                                "seq": detail.get("seq"),
                                "extractedAt": extracted_at,
                            }
                        )
                    except Exception as exc:
                        logger.warning(
                            "[invoice] skip detail id=%s invoice id=%s | error=%s",
                            detail.get("id"),
                            invoice_id,
                            exc,
                        )

                logger.info("[invoice] processed id=%s with %d lines", invoice_id, len(detail_items))

            except Exception as exc:
                logger.warning("[invoice] skip id=%s | error=%s", invoice_id, exc)

        logger.info(
            "[invoice] batch %d-%d done; sleeping %.1fs",
            batch_start + 1,
            min(batch_start + BATCH_SIZE, len(invoice_ids)),
            DETAIL_SLEEP,
        )
        time.sleep(DETAIL_SLEEP)

    if not invoice_rows:
        logger.warning("[invoice] No invoice headers collected for interval.")
    if not invoice_detail_rows:
        logger.warning("[invoice] No invoice detail rows collected for interval.")

    start_tag = _coerce_datetime(data_interval_start).strftime("%Y%m%d")
    end_tag = _coerce_datetime(data_interval_end).strftime("%Y%m%d")

    invoice_file = f"invoice_{start_tag}_{end_tag}.csv"
    detail_file = f"invoiceDetail_{start_tag}_{end_tag}.csv"

    _save_csv(invoice_rows, invoice_file)
    _save_csv(invoice_detail_rows, detail_file)
