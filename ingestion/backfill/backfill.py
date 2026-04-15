"""
backfill.py
-----------
Backfill script for extracting data from Accurate API and loading it
into PostgreSQL raw schema. Processes all source tables in dependency order.
"""

# ─────────────────────────────────────────────
# 1. IMPORTS & SETUP
# ─────────────────────────────────────────────

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

from .utils import get_nested, calculate_age, fetch_list, fetch_detail, load_to_db
    
# Load environment variables from .env at project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# ── Environment variables ──────────────────────────────────────────────────────
SESSION_ID  = os.getenv("SESSION_ID")
TOKEN       = os.getenv("TOKEN")
BASE_URL    = os.getenv("BASE_URL")

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

START_DATE  = os.getenv("START_DATE")  # DD/MM/YYYY
END_DATE    = os.getenv("END_DATE")    # DD/MM/YYYY

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── SQLAlchemy engine ──────────────────────────────────────────────────────────
DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine: Engine = create_engine(DB_URL, pool_pre_ping=True)

# ── Shared HTTP headers ────────────────────────────────────────────────────────
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "X-Session-ID":  SESSION_ID,
    "Content-Type":  "application/json",
}

PAGE_SIZE = 100
RATE_LIMIT_LIST_SLEEP    = 1.0   # seconds between list pages
RATE_LIMIT_DETAIL_SLEEP  = 1.5   # seconds per batch of detail requests
DETAIL_BATCH_SIZE        = 5     # number of detail requests per batch



def backfill_customer_category() -> None:
    """
    Extract customerCategory from Accurate API and load into raw."customerCategory".
    """
    entity = "customerCategory"
    endpoint = "/accurate/api/customer-category/list.do"
    logger.info("[%s] Starting backfill...", entity)

    raw_records = fetch_list(endpoint)
    logger.info("[%s] Total records fetched: %d", entity, len(raw_records))

    rows = []
    for record in raw_records:
        try:
            rows.append({
                "id":              record.get("id"),
                "name":            record.get("name"),
                "defaultCategory": record.get("defaultCategory"),
                "parentName":      record.get("parentName"),
                "extractedAt":     datetime.now(),
            })
        except Exception as e:
            logger.warning("[%s] Skipping record id=%s | Error: %s", entity, record.get("id"), e)

    load_to_db(pd.DataFrame(rows), entity, engine)
    logger.info("[%s] Backfill complete.", entity)


def backfill_customer() -> None:
    """
    Extract customer from Accurate API and load into raw."customer".
    """
    entity = "customer"
    endpoint = "/accurate/api/customer/list.do"
    logger.info("[%s] Starting backfill...", entity)

    raw_records = fetch_list(endpoint)
    logger.info("[%s] Total records fetched: %d", entity, len(raw_records))

    rows = []
    for record in raw_records:
        try:
            rows.append({
                "id":                      record.get("id"),
                "name":                    record.get("name"),
                "customerNo":              record.get("customerNo"),
                "categoryName":            record.get("categoryName"),
                "email":                   record.get("email"),
                "mobilePhone":             record.get("mobilePhone"),
                "workPhone":               record.get("workPhone"),
                "npwpNo":                  record.get("npwpNo"),
                "pkpNo":                   record.get("pkpNo"),
                "billStreet":              get_nested(record, "billAddress", "street"),
                "billCity":                get_nested(record, "billAddress", "city"),
                "billProvince":            get_nested(record, "billAddress", "province"),
                "billCountry":             get_nested(record, "billAddress", "country"),
                "billZipCode":             get_nested(record, "billAddress", "zipCode"),
                "currencyCode":            get_nested(record, "currency", "code"),
                "termName":                get_nested(record, "term", "name"),
                "defaultIncTax":           record.get("defaultIncTax"),
                "customerLimitAge":        record.get("customerLimitAge"),
                "customerLimitAgeValue":   record.get("customerLimitAgeValue"),
                "customerLimitAmount":     record.get("customerLimitAmount"),
                "customerLimitAmountValue": record.get("customerLimitAmountValue"),
                "priceCategoryName":       get_nested(record, "priceCategory", "name"),
                "discountCategoryName":    get_nested(record, "discountCategory", "name"),
                "branchId":                record.get("branchId"),
                "branchName":              record.get("branchName"),
                "extractedAt":             datetime.now(),
            })
        except Exception as e:
            logger.warning("[%s] Skipping record id=%s | Error: %s", entity, record.get("id"), e)

    load_to_db(pd.DataFrame(rows), entity, engine)
    logger.info("[%s] Backfill complete.", entity)


def backfill_warehouse() -> None:
    """
    Extract warehouse from Accurate API and load into raw."warehouse".
    """
    entity = "warehouse"
    endpoint = "/accurate/api/warehouse/list.do"
    logger.info("[%s] Starting backfill...", entity)

    raw_records = fetch_list(endpoint)
    logger.info("[%s] Total records fetched: %d", entity, len(raw_records))

    rows = []
    for record in raw_records:
        try:
            rows.append({
                "id":             record.get("id"),
                "name":           record.get("name"),
                "street":         record.get("street"),
                "city":           record.get("city"),
                "province":       record.get("province"),
                "country":        record.get("country"),
                "zipCode":        record.get("zipCode"),
                "pic":            record.get("pic"),
                "scrapWarehouse": record.get("scrapWarehouse"),
                "suspended":      record.get("suspended"),
                "extractedAt":    datetime.now(),
            })
        except Exception as e:
            logger.warning("[%s] Skipping record id=%s | Error: %s", entity, record.get("id"), e)

    load_to_db(pd.DataFrame(rows), entity, engine)
    logger.info("[%s] Backfill complete.", entity)


def backfill_item_category() -> None:
    """
    Extract itemCategory from Accurate API and load into raw."itemCategory".
    """
    entity = "itemCategory"
    endpoint = "/accurate/api/item-category/list.do"
    logger.info("[%s] Starting backfill...", entity)

    raw_records = fetch_list(endpoint)
    logger.info("[%s] Total records fetched: %d", entity, len(raw_records))

    rows = []
    for record in raw_records:
        try:
            rows.append({
                "id":              record.get("id"),
                "name":            record.get("name"),
                "defaultCategory": record.get("defaultCategory"),
                "parentName":      record.get("parentName"),
                "extractedAt":     datetime.now(),
            })
        except Exception as e:
            logger.warning("[%s] Skipping record id=%s | Error: %s", entity, record.get("id"), e)

    load_to_db(pd.DataFrame(rows), entity, engine)
    logger.info("[%s] Backfill complete.", entity)


def backfill_item() -> None:
    """
    Extract item from Accurate API and load into raw."item".
    """
    entity = "item"
    endpoint = "/accurate/api/item/list.do"
    logger.info("[%s] Starting backfill...", entity)

    raw_records = fetch_list(endpoint)
    logger.info("[%s] Total records fetched: %d", entity, len(raw_records))

    rows = []
    for record in raw_records:
        try:
            rows.append({
                "id":                   record.get("id"),
                "no":                   record.get("no"),
                "name":                 record.get("name"),
                "itemType":             record.get("itemType"),
                "itemCategoryName":     get_nested(record, "itemCategory", "name"),
                "upcNo":                record.get("upcNo"),
                "unitPrice":            record.get("unitPrice"),
                "vendorPrice":          record.get("vendorPrice"),
                "unit1Name":            get_nested(record, "unit1", "name"),
                "vendorUnitName":       get_nested(record, "vendorUnit", "name"),
                "defaultDiscount":      record.get("defaultDiscount"),
                "usePpn":               record.get("usePpn"),
                "manageSN":             record.get("manageSN"),
                "controlQuantity":      record.get("controlQuantity"),
                "salesGlAccountNo":     get_nested(record, "salesGlAccount", "no"),
                "cogsGlAccountNo":      get_nested(record, "cogsGlAccount", "no"),
                "inventoryGlAccountNo": get_nested(record, "inventoryGlAccount", "no"),
                "weight":               record.get("weight"),
                "notes":                record.get("notes"),
                "extractedAt":          datetime.now(),
            })
        except Exception as e:
            logger.warning("[%s] Skipping record id=%s | Error: %s", entity, record.get("id"), e)

    load_to_db(pd.DataFrame(rows), entity, engine)
    logger.info("[%s] Backfill complete.", entity)


def backfill_invoice() -> None:
    """
    Extract sales invoices from Accurate API.

    Step 1 — Fetch all invoice IDs via list endpoint (filtered by date range).
    Step 2 — Fetch detail per invoice ID.
    Step 3 — Load invoice headers into raw."invoice".
    Step 4 — Load invoice line items into raw."invoiceDetail".
    """
    entity = "invoice"
    list_endpoint   = "/accurate/api/sales-invoice/list.do"
    detail_endpoint = "/accurate/api/sales-invoice/detail.do"

    logger.info("[%s] Starting backfill (date range: %s → %s)...", entity, START_DATE, END_DATE)

    # ── Step 1: Fetch list of invoices ─────────────────────────────────────────
    date_filter_params = {
        "filter.transDate.op":     "BETWEEN",
        "filter.transDate.val[0]": START_DATE,
        "filter.transDate.val[1]": END_DATE,
    }
    invoice_list = fetch_list(list_endpoint, extra_params=date_filter_params)
    logger.info("[%s] Total invoices found in list: %d", entity, len(invoice_list))

    invoice_ids = [inv.get("id") for inv in invoice_list if inv.get("id") is not None]
    logger.info("[%s] Fetching detail for %d invoices...", entity, len(invoice_ids))

    # ── Step 2: Fetch details with rate-limited batching ──────────────────────
    invoice_rows        = []
    invoice_detail_rows = []
    extracted_at        = datetime.now()

    for batch_start in range(0, len(invoice_ids), DETAIL_BATCH_SIZE):
        batch = invoice_ids[batch_start: batch_start + DETAIL_BATCH_SIZE]

        for invoice_id in batch:
            try:
                detail_data = fetch_detail(detail_endpoint, invoice_id)
                if detail_data is None:
                    logger.warning("[%s] No detail returned for invoice id=%s, skipping.", entity, invoice_id)
                    continue

                # ── Invoice header row ─────────────────────────────────────
                trans_date_str = detail_data.get("transDate")
                invoice_rows.append({
                    "invoiceId":        detail_data.get("id"),
                    "invoiceNumber":    detail_data.get("number"),
                    "transDate":        trans_date_str,
                    "dueDate":          detail_data.get("dueDate"),
                    "shipDate":         detail_data.get("shipDate"),
                    "customerId":       get_nested(detail_data, "customer", "id"),
                    "customerName":     get_nested(detail_data, "customer", "name"),
                    "subTotal":         detail_data.get("subTotal"),
                    "totalAmount":      detail_data.get("totalAmount"),
                    "outstanding":      detail_data.get("outstanding"),
                    "status":           detail_data.get("status"),
                    "approvalStatus":   detail_data.get("approvalStatus"),
                    "poNumber":         detail_data.get("poNumber"),
                    "salesOrderId":     detail_data.get("salesOrderId"),
                    "deliveryOrderId":  detail_data.get("deliveryOrderId"),
                    "paymentTermId":    get_nested(detail_data, "paymentTerm", "id"),
                    "paymentTermName":  get_nested(detail_data, "paymentTerm", "name"),
                    "currencyId":       get_nested(detail_data, "currency", "id"),
                    "currencyCode":     get_nested(detail_data, "currency", "code"),
                    "rate":             detail_data.get("rate"),
                    "branchId":         detail_data.get("branchId"),
                    "branchName":       detail_data.get("branchName"),
                    "invoiceAgeDays":   calculate_age(trans_date_str, fmt="%d/%m/%Y"),
                    "createdBy":        detail_data.get("createdBy"),
                    "printedTime":      detail_data.get("printedTime"),
                    "extractedAt":      extracted_at,
                })

                # ── Invoice detail rows (line items) ──────────────────────
                detail_items = detail_data.get("detailItem", []) or []
                for detail in detail_items:
                    try:
                        invoice_detail_rows.append({
                            "detailId":              detail.get("id"),
                            "invoiceId":             detail_data.get("id"),
                            "invoiceNumber":         detail_data.get("number"),
                            "itemId":                get_nested(detail, "item", "id"),
                            "itemNo":                get_nested(detail, "item", "no"),
                            "itemName":              get_nested(detail, "item", "name"),
                            "itemCategoryId":        get_nested(detail, "item", "itemCategoryId"),
                            "quantity":              detail.get("quantity"),
                            "unitId":                get_nested(detail, "itemUnit", "id"),
                            "unitName":              get_nested(detail, "itemUnit", "name"),
                            "unitRatio":             detail.get("unitRatio"),
                            "unitPrice":             detail.get("unitPrice"),
                            "grossAmount":           detail.get("grossAmount"),
                            "salesAmount":           detail.get("salesAmount"),
                            "warehouseId":           get_nested(detail, "warehouse", "id"),
                            "warehouseName":         get_nested(detail, "warehouse", "name"),
                            "salesOrderDetailId":    detail.get("salesOrderDetailId"),
                            "deliveryOrderDetailId": detail.get("deliveryOrderDetailId"),
                            "seq":                   detail.get("seq"),
                            "extractedAt":           extracted_at,
                        })
                    except Exception as e:
                        logger.warning(
                            "[%s] Skipping detail line id=%s in invoice id=%s | Error: %s",
                            entity, detail.get("id"), invoice_id, e,
                        )

                logger.info(
                    "[%s] Processed invoice id=%s | %d line items",
                    entity, invoice_id, len(detail_items),
                )

            except Exception as e:
                logger.warning("[%s] Skipping invoice id=%s | Error: %s", entity, invoice_id, e)

        # Rate limit: sleep after every batch of DETAIL_BATCH_SIZE requests
        logger.info(
            "[%s] Batch %d–%d done. Sleeping %.1fs...",
            entity,
            batch_start + 1,
            min(batch_start + DETAIL_BATCH_SIZE, len(invoice_ids)),
            RATE_LIMIT_DETAIL_SLEEP,
        )
        time.sleep(RATE_LIMIT_DETAIL_SLEEP)

    # ── Step 3 & 4: Load headers and line items ────────────────────────────────
    logger.info("[%s] Total invoice headers collected: %d", entity, len(invoice_rows))
    logger.info("[%s] Total invoice detail lines collected: %d", entity, len(invoice_detail_rows))

    load_to_db(pd.DataFrame(invoice_rows), "invoice", engine)
    load_to_db(pd.DataFrame(invoice_detail_rows), "invoiceDetail", engine)

    logger.info("[%s] Backfill complete.", entity)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> None:
    """
    Run all backfill functions in referential dependency order.
    """
    logger.info("=" * 60)
    logger.info("Accurate DWH Backfill — START")
    logger.info("Date range: %s → %s", START_DATE, END_DATE)
    logger.info("=" * 60)

    start_time = datetime.now()

    steps = [
        ("customerCategory", backfill_customer_category),
        ("customer",         backfill_customer),
        ("warehouse",        backfill_warehouse),
        ("itemCategory",     backfill_item_category),
        ("item",             backfill_item),
        ("invoice",          backfill_invoice),
    ]

    for name, fn in steps:
        logger.info("-" * 60)
        logger.info("Running: %s", name)
        try:
            fn()
        except Exception as e:
            logger.error("FATAL error in %s: %s — Continuing to next entity.", name, e)

    elapsed = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info("Accurate DWH Backfill — DONE (elapsed: %s)", str(elapsed).split(".")[0])
    logger.info("=" * 60)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    main()
