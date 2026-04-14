-- =============================================================================
-- RAW SCHEMA - Data Warehouse Ingestion
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- -----------------------------------------------------------------------------
-- raw.customerCategory
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.customerCategory (
    id                INTEGER         NOT NULL,
    name              VARCHAR(255)    NOT NULL,
    defaultCategory   BOOLEAN,
    parentName        VARCHAR(255),
    extractedAt       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- raw.customer
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.customer (
    id                        INTEGER         NOT NULL,
    name                      VARCHAR(255)    NOT NULL,
    customerNo                VARCHAR(100),
    categoryName              VARCHAR(255),
    email                     VARCHAR(255),
    mobilePhone               VARCHAR(50),
    workPhone                 VARCHAR(50),
    npwpNo                    VARCHAR(50),
    pkpNo                     VARCHAR(50),

    -- Billing Address
    billStreet                VARCHAR(255),
    billCity                  VARCHAR(100),
    billProvince              VARCHAR(100),
    billCountry               VARCHAR(100),
    billZipCode               VARCHAR(20),

    -- Financial Settings
    currencyCode              VARCHAR(10),
    termName                  VARCHAR(255),
    defaultIncTax             BOOLEAN,
    customerLimitAge          BOOLEAN,
    customerLimitAgeValue     INTEGER,
    customerLimitAmount       BOOLEAN,
    customerLimitAmountValue  NUMERIC(22, 6),

    -- Categorization
    priceCategoryName         VARCHAR(255),
    discountCategoryName      VARCHAR(255),
    branchId                  INTEGER,
    branchName                VARCHAR(255),

    extractedAt               TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- raw.warehouse
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.warehouse (
    id                INTEGER         NOT NULL,
    name              VARCHAR(255)    NOT NULL,
    street            VARCHAR(255),
    city              VARCHAR(100),
    province          VARCHAR(100),
    country           VARCHAR(100),
    zipCode           VARCHAR(20),
    pic               VARCHAR(255),
    scrapWarehouse    BOOLEAN,
    suspended         BOOLEAN,
    extractedAt       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- raw.branch
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.branch (
    id              INTEGER         NOT NULL,
    name            VARCHAR(255)    NOT NULL,
    street          VARCHAR(255),
    city            VARCHAR(100),
    province        VARCHAR(100),
    country         VARCHAR(100),
    zipCode         VARCHAR(20),
    extractedAt     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
 
    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- raw.itemCategory
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.itemCategory (
    id                INTEGER         NOT NULL,
    name              VARCHAR(255)    NOT NULL,
    defaultCategory   BOOLEAN,
    parentName        VARCHAR(255),
    extractedAt       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- raw.item
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.item (
    id                    INTEGER         NOT NULL,
    no                    VARCHAR(100),
    name                  VARCHAR(255)    NOT NULL,
    itemType              VARCHAR(50)     NOT NULL,
    itemCategoryName      VARCHAR(255),
    upcNo                 VARCHAR(100),

    -- Pricing & Units
    unitPrice             NUMERIC(22, 6),
    vendorPrice           NUMERIC(22, 6),
    unit1Name             VARCHAR(100),
    vendorUnitName        VARCHAR(100),
    defaultDiscount       VARCHAR(50),

    -- Flags
    usePpn                BOOLEAN,
    manageSN              BOOLEAN,
    controlQuantity       BOOLEAN,

    -- GL Accounts
    salesGlAccountNo      VARCHAR(100),
    cogsGlAccountNo       VARCHAR(100),
    inventoryGlAccountNo  VARCHAR(100),

    -- Physical
    weight                NUMERIC(22, 6),

    notes                 TEXT,
    extractedAt           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- raw.invoice (order header)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.invoice (
    invoiceId         INTEGER         NOT NULL,
    invoiceNumber     VARCHAR(100),

    -- Dates
    transDate         DATE,
    dueDate           DATE,
    shipDate          DATE,

    -- Customer
    customerId        INTEGER,
    customerName      VARCHAR(255),

    -- Financial
    subTotal          NUMERIC(22, 6),
    totalAmount       NUMERIC(22, 6),
    outstanding       NUMERIC(22, 6),

    -- Status
    status            VARCHAR(50),
    approvalStatus    VARCHAR(50),

    -- Reference
    poNumber          VARCHAR(100),
    salesOrderId      INTEGER,
    deliveryOrderId   INTEGER,

    -- Payment & Currency
    paymentTermId     INTEGER,
    paymentTermName   VARCHAR(255),
    currencyId        INTEGER,
    currencyCode      VARCHAR(10),
    rate              NUMERIC(22, 6),

    -- Organization
    branchId          INTEGER,
    branchName        VARCHAR(255),

    -- Metadata
    invoiceAgeDays    INTEGER,
    createdBy         VARCHAR(255),
    printedTime       TIMESTAMP,
    extractedAt       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (invoiceId)
);

-- -----------------------------------------------------------------------------
-- raw.invoiceDetail (order detail)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.invoiceDetail (
    detailId                  INTEGER         NOT NULL,
    invoiceId                 INTEGER         NOT NULL,
    invoiceNumber             VARCHAR(100),

    -- Item
    itemId                    INTEGER,
    itemNo                    VARCHAR(100),
    itemName                  VARCHAR(255),
    itemCategoryId            INTEGER,

    -- Quantity & Unit
    quantity                  NUMERIC(22, 6),
    unitId                    INTEGER,
    unitName                  VARCHAR(100),
    unitRatio                 NUMERIC(22, 6),

    -- Pricing
    unitPrice                 NUMERIC(22, 6),
    grossAmount               NUMERIC(22, 6),
    salesAmount               NUMERIC(22, 6),

    -- Warehouse
    warehouseId               INTEGER,
    warehouseName             VARCHAR(255),

    -- Reference
    salesOrderDetailId        INTEGER,
    deliveryOrderDetailId     INTEGER,

    seq                       INTEGER,
    extractedAt               TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (detailId)
);

-- -----------------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_invoice_customerId       ON raw.invoice (customerId);
CREATE INDEX IF NOT EXISTS idx_invoice_transDate        ON raw.invoice (transDate);
CREATE INDEX IF NOT EXISTS idx_invoice_status           ON raw.invoice (status);
CREATE INDEX IF NOT EXISTS idx_invoice_branchId         ON raw.invoice (branchId);
CREATE INDEX IF NOT EXISTS idx_invoice_extractedAt      ON raw.invoice (extractedAt);

CREATE INDEX IF NOT EXISTS idx_invoiceDetail_invoiceId      ON raw.invoiceDetail (invoiceId);
CREATE INDEX IF NOT EXISTS idx_invoiceDetail_itemId         ON raw.invoiceDetail (itemId);
CREATE INDEX IF NOT EXISTS idx_invoiceDetail_warehouseId    ON raw.invoiceDetail (warehouseId);
CREATE INDEX IF NOT EXISTS idx_invoiceDetail_extractedAt    ON raw.invoiceDetail (extractedAt);

CREATE INDEX IF NOT EXISTS idx_customer_categoryName    ON raw.customer (categoryName);
CREATE INDEX IF NOT EXISTS idx_customer_branchId        ON raw.customer (branchId);

CREATE INDEX IF NOT EXISTS idx_item_no                  ON raw.item (no);
CREATE INDEX IF NOT EXISTS idx_item_itemCategoryName    ON raw.item (itemCategoryName);
CREATE INDEX IF NOT EXISTS idx_item_itemType            ON raw.item (itemType);