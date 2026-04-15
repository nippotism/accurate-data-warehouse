-- =============================================================================
-- Dummy Data Generator for raw schema (Hospital Supply Distributor)
-- Target:
-- - >= 100 invoices
-- - >= 50 customers
-- - >= 50 items
-- - 2-5 invoice details per invoice
-- - logical FK consistency across ids and names
-- =============================================================================

BEGIN;

-- Optional reset so script is idempotent for local development.
DELETE FROM raw.invoiceDetail;
DELETE FROM raw.invoice;
DELETE FROM raw.item;
DELETE FROM raw.itemCategory;
DELETE FROM raw.customer;
DELETE FROM raw.customerCategory;
DELETE FROM raw.warehouse;
DELETE FROM raw.branch;

-- -----------------------------------------------------------------------------
-- Master data
-- -----------------------------------------------------------------------------

INSERT INTO raw.customerCategory (id, name, defaultCategory, parentName)
VALUES
    (1, 'Hospital - A', FALSE, 'Hospital'),
    (2, 'Hospital - B', FALSE, 'Hospital'),
    (3, 'Clinic', FALSE, 'Healthcare'),
    (4, 'Laboratory', FALSE, 'Healthcare'),
    (5, 'Pharmacy Chain', FALSE, 'Retail Health'),
    (6, 'Government Facility', TRUE, 'Government');

INSERT INTO raw.branch (id, name, street, city, province, country, zipCode)
VALUES
    (1, 'Jakarta Branch', 'Jl. Industri Medika No. 1', 'Jakarta Selatan', 'DKI Jakarta', 'Indonesia', '12950'),
    (2, 'Bandung Branch', 'Jl. Kesehatan Prima No. 12', 'Bandung', 'Jawa Barat', 'Indonesia', '40115'),
    (3, 'Surabaya Branch', 'Jl. Logistik Sehat No. 8', 'Surabaya', 'Jawa Timur', 'Indonesia', '60234'),
    (4, 'Semarang Branch', 'Jl. Distribusi Farma No. 20', 'Semarang', 'Jawa Tengah', 'Indonesia', '50241'),
    (5, 'Medan Branch', 'Jl. Utama Medika No. 5', 'Medan', 'Sumatera Utara', 'Indonesia', '20111');

INSERT INTO raw.warehouse (id, name, street, city, province, country, zipCode, pic, scrapWarehouse, suspended)
VALUES
    (1, 'WH Jakarta Utama', 'Kawasan Industri Pulogadung Blok A1', 'Jakarta Timur', 'DKI Jakarta', 'Indonesia', '13920', 'Anwar Pratama', FALSE, FALSE),
    (2, 'WH Jakarta Cold Storage', 'Kawasan Industri Pulogadung Blok C2', 'Jakarta Timur', 'DKI Jakarta', 'Indonesia', '13920', 'Dita Mulyani', FALSE, FALSE),
    (3, 'WH Bandung Pusat', 'Jl. Soekarno Hatta No. 88', 'Bandung', 'Jawa Barat', 'Indonesia', '40235', 'Rian Kurnia', FALSE, FALSE),
    (4, 'WH Surabaya Distribusi', 'Jl. Rungkut Industri No. 22', 'Surabaya', 'Jawa Timur', 'Indonesia', '60293', 'Sinta Anggraini', FALSE, FALSE),
    (5, 'WH Semarang Hub', 'Jl. Tugu Industri No. 16', 'Semarang', 'Jawa Tengah', 'Indonesia', '50152', 'Bagus Saputra', FALSE, FALSE),
    (6, 'WH Medan Utara', 'Jl. Gatot Subroto Km 8', 'Medan', 'Sumatera Utara', 'Indonesia', '20128', 'Robby Siregar', FALSE, FALSE),
    (7, 'WH Return Center', 'Jl. Logistik Nasional No. 9', 'Bekasi', 'Jawa Barat', 'Indonesia', '17530', 'Nina Kusuma', TRUE, FALSE),
    (8, 'WH Buffer Stock', 'Jl. Ringroad Barat No. 77', 'Yogyakarta', 'DI Yogyakarta', 'Indonesia', '55285', 'Eko Wijaya', FALSE, FALSE);

INSERT INTO raw.itemCategory (id, name, defaultCategory, parentName)
VALUES
    (1, 'PPE', FALSE, 'Consumables'),
    (2, 'Syringe & Needle', FALSE, 'Consumables'),
    (3, 'Infusion Set', FALSE, 'Consumables'),
    (4, 'Wound Care', FALSE, 'Consumables'),
    (5, 'Diagnostic', FALSE, 'Medical Device'),
    (6, 'Surgical Instrument', FALSE, 'Medical Device'),
    (7, 'Patient Monitor Accessories', FALSE, 'Medical Device'),
    (8, 'Lab Consumables', FALSE, 'Laboratory'),
    (9, 'Hospital Furniture', FALSE, 'Non Medical'),
    (10, 'General Supplies', TRUE, 'Non Medical');

-- 60 items (minimum requested: 50)
INSERT INTO raw.item (
    id, no, name, itemType, itemCategoryName, upcNo,
    unitPrice, vendorPrice, unit1Name, vendorUnitName, defaultDiscount,
    usePpn, manageSN, controlQuantity,
    salesGlAccountNo, cogsGlAccountNo, inventoryGlAccountNo,
    weight, notes
)
SELECT
    i,
    'ITM-' || LPAD(i::text, 4, '0') AS no,
    CASE
        WHEN i % 10 = 1 THEN 'Surgical Mask 3 Ply Box ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 2 THEN 'Nitrile Gloves Powder Free Size M ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 3 THEN 'Syringe 5ml Luer Lock ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 4 THEN 'Infusion Set Adult ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 5 THEN 'Rapid Test Kit Multipanel ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 6 THEN 'Scalpel Blade Sterile No.11 ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 7 THEN 'ECG Electrode Disposable ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 8 THEN 'Vacutainer Tube EDTA 3ml ' || LPAD(i::text, 3, '0')
        WHEN i % 10 = 9 THEN 'Hospital Bed Sheet Set ' || LPAD(i::text, 3, '0')
        ELSE 'Disinfectant Wipe Canister ' || LPAD(i::text, 3, '0')
    END AS name,
    CASE
        WHEN i % 10 IN (5, 6, 7, 9) THEN 'NON_STOCK'
        ELSE 'STOCK'
    END AS itemType,
    CASE
        WHEN i % 10 = 1 THEN 'PPE'
        WHEN i % 10 = 2 THEN 'PPE'
        WHEN i % 10 = 3 THEN 'Syringe & Needle'
        WHEN i % 10 = 4 THEN 'Infusion Set'
        WHEN i % 10 = 5 THEN 'Diagnostic'
        WHEN i % 10 = 6 THEN 'Surgical Instrument'
        WHEN i % 10 = 7 THEN 'Patient Monitor Accessories'
        WHEN i % 10 = 8 THEN 'Lab Consumables'
        WHEN i % 10 = 9 THEN 'Hospital Furniture'
        ELSE 'General Supplies'
    END AS itemCategoryName,
    '8999' || LPAD((100000 + i)::text, 6, '0') AS upcNo,
    (5000 + (i * 1250) + ((i % 7) * 275))::numeric(22,6) AS unitPrice,
    (3500 + (i * 900) + ((i % 5) * 200))::numeric(22,6) AS vendorPrice,
    CASE WHEN i % 10 IN (3, 4, 8) THEN 'PCS' ELSE 'BOX' END AS unit1Name,
    CASE WHEN i % 10 IN (3, 4, 8) THEN 'CARTON' ELSE 'BOX' END AS vendorUnitName,
    CASE
        WHEN i % 10 IN (1, 2, 10) THEN '5%'
        WHEN i % 10 IN (3, 4, 8) THEN '2%'
        ELSE '0%'
    END AS defaultDiscount,
    TRUE AS usePpn,
    CASE WHEN i % 10 IN (5, 6, 7) THEN TRUE ELSE FALSE END AS manageSN,
    TRUE AS controlQuantity,
    '4-1000' AS salesGlAccountNo,
    '5-2000' AS cogsGlAccountNo,
    '1-3000' AS inventoryGlAccountNo,
    (0.05 + (i % 9) * 0.12)::numeric(22,6) AS weight,
    'Dummy item for hospital supply distributor'
FROM generate_series(1, 60) AS g(i);

-- 60 customers (minimum requested: 50)
INSERT INTO raw.customer (
    id, name, customerNo, categoryName, email, mobilePhone, workPhone,
    npwpNo, pkpNo,
    billStreet, billCity, billProvince, billCountry, billZipCode,
    currencyCode, termName,
    defaultIncTax, customerLimitAge, customerLimitAgeValue,
    customerLimitAmount, customerLimitAmountValue,
    priceCategoryName, discountCategoryName,
    branchId, branchName
)
SELECT
    c,
    CASE
        WHEN c % 6 = 1 THEN 'RS Sehat Sentosa ' || LPAD(c::text, 3, '0')
        WHEN c % 6 = 2 THEN 'RSU Mitra Medika ' || LPAD(c::text, 3, '0')
        WHEN c % 6 = 3 THEN 'Klinik Prima Husada ' || LPAD(c::text, 3, '0')
        WHEN c % 6 = 4 THEN 'Laboratorium Diagnostik ' || LPAD(c::text, 3, '0')
        WHEN c % 6 = 5 THEN 'Apotek Jaringan Sejahtera ' || LPAD(c::text, 3, '0')
        ELSE 'Fasilitas Kesehatan Pemda ' || LPAD(c::text, 3, '0')
    END AS name,
    'CUST-' || LPAD(c::text, 5, '0') AS customerNo,
    CASE
        WHEN c % 6 = 1 THEN 'Hospital - A'
        WHEN c % 6 = 2 THEN 'Hospital - B'
        WHEN c % 6 = 3 THEN 'Clinic'
        WHEN c % 6 = 4 THEN 'Laboratory'
        WHEN c % 6 = 5 THEN 'Pharmacy Chain'
        ELSE 'Government Facility'
    END AS categoryName,
    'procurement' || c::text || '@customer-health.id' AS email,
    '08' || LPAD((1100000000 + c)::text, 10, '0') AS mobilePhone,
    '021' || LPAD((7000000 + c)::text, 7, '0') AS workPhone,
    '01.234.' || LPAD((100 + c)::text, 3, '0') || '.9-555.' || LPAD((100 + c)::text, 3, '0') AS npwpNo,
    'PKP-' || LPAD((90000 + c)::text, 5, '0') AS pkpNo,
    'Jl. Kesehatan No. ' || c::text AS billStreet,
    CASE
        WHEN c % 5 = 1 THEN 'Jakarta'
        WHEN c % 5 = 2 THEN 'Bandung'
        WHEN c % 5 = 3 THEN 'Surabaya'
        WHEN c % 5 = 4 THEN 'Semarang'
        ELSE 'Medan'
    END AS billCity,
    CASE
        WHEN c % 5 = 1 THEN 'DKI Jakarta'
        WHEN c % 5 = 2 THEN 'Jawa Barat'
        WHEN c % 5 = 3 THEN 'Jawa Timur'
        WHEN c % 5 = 4 THEN 'Jawa Tengah'
        ELSE 'Sumatera Utara'
    END AS billProvince,
    'Indonesia' AS billCountry,
    LPAD((10000 + c)::text, 5, '0') AS billZipCode,
    'IDR' AS currencyCode,
    CASE WHEN c % 2 = 0 THEN 'Net 30' ELSE 'Net 14' END AS termName,
    TRUE AS defaultIncTax,
    TRUE AS customerLimitAge,
    CASE WHEN c % 2 = 0 THEN 30 ELSE 14 END AS customerLimitAgeValue,
    TRUE AS customerLimitAmount,
    (50000000 + (c * 1500000))::numeric(22,6) AS customerLimitAmountValue,
    CASE WHEN c % 3 = 0 THEN 'Tier B' ELSE 'Tier A' END AS priceCategoryName,
    CASE WHEN c % 4 = 0 THEN 'Promo-Health' ELSE 'Regular' END AS discountCategoryName,
    ((c - 1) % 5) + 1 AS branchId,
    CASE ((c - 1) % 5) + 1
        WHEN 1 THEN 'Jakarta Branch'
        WHEN 2 THEN 'Bandung Branch'
        WHEN 3 THEN 'Surabaya Branch'
        WHEN 4 THEN 'Semarang Branch'
        ELSE 'Medan Branch'
    END AS branchName
FROM generate_series(1, 60) AS gen(c);

-- -----------------------------------------------------------------------------
-- Invoice header and detail
-- -----------------------------------------------------------------------------

-- 120 invoices (minimum requested: 100)
INSERT INTO raw.invoice (
    invoiceId, invoiceNumber,
    transDate, dueDate, shipDate,
    customerId, customerName,
    subTotal, totalAmount, outstanding,
    status, approvalStatus,
    poNumber, salesOrderId, deliveryOrderId,
    paymentTermId, paymentTermName,
    currencyId, currencyCode, rate,
    branchId, branchName,
    invoiceAgeDays, createdBy, printedTime
)
SELECT
    (100000 + i) AS invoiceId,
    'INV-2026-' || LPAD(i::text, 5, '0') AS invoiceNumber,
    (DATE '2025-10-01' + ((i * 2) % 180)) AS transDate,
    (DATE '2025-10-01' + ((i * 2) % 180) + CASE WHEN i % 2 = 0 THEN 30 ELSE 14 END) AS dueDate,
    (DATE '2025-10-01' + ((i * 2) % 180) + (1 + (i % 3))) AS shipDate,
    c.id AS customerId,
    c.name AS customerName,
    0::numeric(22,6) AS subTotal,
    0::numeric(22,6) AS totalAmount,
    0::numeric(22,6) AS outstanding,
    CASE
        WHEN i % 5 = 0 THEN 'PAID'
        WHEN i % 5 = 1 THEN 'OPEN'
        WHEN i % 5 = 2 THEN 'PARTIAL'
        WHEN i % 5 = 3 THEN 'OPEN'
        ELSE 'PAID'
    END AS status,
    'APPROVED' AS approvalStatus,
    'PO-' || LPAD((900000 + i)::text, 6, '0') AS poNumber,
    (700000 + i) AS salesOrderId,
    (800000 + i) AS deliveryOrderId,
    CASE WHEN i % 2 = 0 THEN 30 ELSE 14 END AS paymentTermId,
    CASE WHEN i % 2 = 0 THEN 'Net 30' ELSE 'Net 14' END AS paymentTermName,
    1 AS currencyId,
    'IDR' AS currencyCode,
    1::numeric(22,6) AS rate,
    c.branchId,
    c.branchName,
    GREATEST((CURRENT_DATE - (DATE '2025-10-01' + ((i * 2) % 180))), 0)::integer AS invoiceAgeDays,
    CASE WHEN i % 3 = 0 THEN 'sales.ops' WHEN i % 3 = 1 THEN 'inside.sales' ELSE 'account.exec' END AS createdBy,
    NOW() - ((i % 15) || ' days')::interval AS printedTime
FROM generate_series(1, 120) AS g(i)
JOIN raw.customer c ON c.id = ((i - 1) % 60) + 1;

-- Generate 2-5 detail lines per invoice.
WITH detail_source AS (
    SELECT
        inv.invoiceId,
        inv.invoiceNumber,
        seq.line_no,
        ((inv.invoiceId + seq.line_no * 7) % 60) + 1 AS itemId,
        CASE
            WHEN inv.branchId = 1 THEN CASE WHEN seq.line_no % 2 = 0 THEN 1 ELSE 2 END
            WHEN inv.branchId = 2 THEN 3
            WHEN inv.branchId = 3 THEN 4
            WHEN inv.branchId = 4 THEN 5
            ELSE 6
        END AS warehouseId,
        (1 + ((inv.invoiceId + seq.line_no) % 20))::numeric(22,6) AS quantity,
        seq.line_no
    FROM raw.invoice inv
    JOIN LATERAL generate_series(1, 2 + (inv.invoiceId % 4)) AS seq(line_no) ON TRUE
),
detail_calculated AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ds.invoiceId, ds.line_no) AS detailId,
        ds.invoiceId,
        ds.invoiceNumber,
        it.id AS itemId,
        it.no AS itemNo,
        it.name AS itemName,
        ic.id AS itemCategoryId,
        ds.quantity,
        1 AS unitId,
        it.unit1Name AS unitName,
        1::numeric(22,6) AS unitRatio,
        it.unitPrice,
        (ds.quantity * it.unitPrice)::numeric(22,6) AS grossAmount,
        (ds.quantity * it.unitPrice * (1 - CASE WHEN ds.line_no % 5 = 0 THEN 0.10 WHEN ds.line_no % 3 = 0 THEN 0.05 ELSE 0 END))::numeric(22,6) AS salesAmount,
        w.id AS warehouseId,
        w.name AS warehouseName,
        (900000 + ds.invoiceId + ds.line_no) AS salesOrderDetailId,
        (950000 + ds.invoiceId + ds.line_no) AS deliveryOrderDetailId,
        ds.line_no AS seq
    FROM detail_source ds
    JOIN raw.item it ON it.id = ds.itemId
    JOIN raw.itemCategory ic ON ic.name = it.itemCategoryName
    JOIN raw.warehouse w ON w.id = ds.warehouseId
)
INSERT INTO raw.invoiceDetail (
    detailId, invoiceId, invoiceNumber,
    itemId, itemNo, itemName, itemCategoryId,
    quantity, unitId, unitName, unitRatio,
    unitPrice, grossAmount, salesAmount,
    warehouseId, warehouseName,
    salesOrderDetailId, deliveryOrderDetailId,
    seq
)
SELECT
    detailId, invoiceId, invoiceNumber,
    itemId, itemNo, itemName, itemCategoryId,
    quantity, unitId, unitName, unitRatio,
    unitPrice, grossAmount, salesAmount,
    warehouseId, warehouseName,
    salesOrderDetailId, deliveryOrderDetailId,
    seq
FROM detail_calculated;

-- Sync financial values on header based on details.
WITH inv_totals AS (
    SELECT
        invoiceId,
        SUM(grossAmount)::numeric(22,6) AS subTotal,
        SUM(salesAmount)::numeric(22,6) AS totalAmount
    FROM raw.invoiceDetail
    GROUP BY invoiceId
)
UPDATE raw.invoice i
SET
    subTotal = t.subTotal,
    totalAmount = t.totalAmount,
    outstanding = CASE
        WHEN i.status = 'PAID' THEN 0::numeric(22,6)
        WHEN i.status = 'PARTIAL' THEN ROUND(t.totalAmount * 0.35, 6)
        ELSE t.totalAmount
    END
FROM inv_totals t
WHERE i.invoiceId = t.invoiceId;

COMMIT;

-- Quick sanity checks:
-- SELECT COUNT(*) AS customer_count FROM raw.customer;
-- SELECT COUNT(*) AS item_count FROM raw.item;
-- SELECT COUNT(*) AS invoice_count FROM raw.invoice;
-- SELECT MIN(line_count), MAX(line_count)
-- FROM (
--     SELECT invoiceId, COUNT(*) AS line_count
--     FROM raw.invoiceDetail
--     GROUP BY invoiceId
-- ) x;
