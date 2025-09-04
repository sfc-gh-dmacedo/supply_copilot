--------------------------------------------------------------------------------
-- DIMENSION TABLES
--------------------------------------------------------------------------------

-- Store dimension (one row per store)
create or replace table SUPPLY_SILVER.DIM_STORE (
    STORE_ID     string      not null comment 'Unique store identifier',
    STORE_NAME   string      comment 'Store display name',
    REGION       string      comment 'Geographic region',
    CLUSTER      string      comment 'Store cluster or grouping',
    OPENED_DATE  date        comment 'Store opening date',
    constraint PK_DIM_STORE primary key (STORE_ID)
)
comment='Master data for stores. Used to join sales, inventory and shipments.';

-- SKU dimension (one row per product/SKU)
create or replace table SUPPLY_SILVER.DIM_SKU (
    SKU_ID       string      not null comment 'Unique SKU identifier',
    SKU_NAME     string      comment 'Product/SKU name',
    CATEGORY     string      comment 'Category (e.g. Beverages, Snacks)',
    SUBCATEGORY  string      comment 'Subcategory',
    UNIT_COST    number(12,4) comment 'Unit cost (may be masked by policy)',
    UNIT_PRICE   number(12,4) comment 'Unit selling price',
    constraint PK_DIM_SKU primary key (SKU_ID)
)
comment='Master data for SKUs. Used to join sales, inventory and OTIF metrics.';

-- Date dimension (calendar table for joins and seasonality)
create or replace table SUPPLY_SILVER.DIM_DATE (
    D            date        not null comment 'Calendar date',
    DOW          tinyint     comment 'Day of week (1=Mon, 7=Sun)',
    DOM          tinyint     comment 'Day of month',
    MONTH        tinyint     comment 'Month number',
    YEAR         smallint    comment 'Year number',
    IS_WEEKEND   boolean     comment 'Weekend flag',
    WEEK_OF_YEAR tinyint     comment 'Week number',
    MONTH_NAME   string      comment 'Month name',
    IS_HOLIDAY   boolean     comment 'Holiday flag',
    constraint PK_DIM_DATE primary key (D)
)
comment='Calendar dimension to support time-based joins and seasonality features.';

--------------------------------------------------------------------------------
-- FACT TABLES (RAW / BRONZE)
--------------------------------------------------------------------------------

-- Sales transactions (raw POS data)
create or replace table SUPPLY_BRONZE.FACT_SALES_RAW (
    TXN_TS   timestamp_ntz comment 'Transaction timestamp',
    STORE_ID string        comment 'Store ID (FK to DIM_STORE)',
    SKU_ID   string        comment 'SKU ID (FK to DIM_SKU)',
    QTY      number(12,3)  comment 'Quantity sold',
    REVENUE  number(12,2)  comment 'Total revenue of transaction line'
)
comment='Raw sales transactions before aggregation.';

-- Inventory snapshots (daily or intraday)
create or replace table SUPPLY_BRONZE.FACT_INVENTORY_RAW (
    AS_OF_DATE date         comment 'Snapshot date',
    STORE_ID   string       comment 'Store ID (FK to DIM_STORE)',
    SKU_ID     string       comment 'SKU ID (FK to DIM_SKU)',
    ON_HAND    number(12,3) comment 'Units physically on hand',
    ON_ORDER   number(12,3) comment 'Units already ordered but not yet received'
)
comment='Inventory position snapshots per store and SKU.';

-- Purchase orders and shipments
create or replace table SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW (
    PO_ID          string       comment 'Purchase order identifier',
    STORE_ID       string       comment 'Store ID (FK to DIM_STORE)',
    SKU_ID         string       comment 'SKU ID (FK to DIM_SKU)',
    ORDERED_QTY    number(12,3) comment 'Ordered quantity',
    PROMISED_DATE  date         comment 'Promised delivery date',
    SHIPPED_QTY    number(12,3) comment 'Shipped quantity',
    SHIPPED_DATE   date         comment 'Date order was shipped',
    DELIVERED_DATE date         comment 'Date order was delivered'
)
comment='Purchase orders and shipment records for OTIF calculation.';

-- Promotions or campaigns impacting demand
create or replace table SUPPLY_BRONZE.FACT_PROMO_RAW (
    START_DATE date         comment 'Promotion start date',
    END_DATE   date         comment 'Promotion end date',
    STORE_ID   string       comment 'Store ID (FK to DIM_STORE)',
    SKU_ID     string       comment 'SKU ID (FK to DIM_SKU)',
    PROMO_TYPE string       comment 'Promotion type (discount, bundle, etc.)',
    LIFT_FACTOR number(6,3) comment 'Expected uplift in demand (multiplier)'
)
comment='Promotions and campaigns applied to stores/SKUs.';

--------------------------------------------------------------------------------
-- GOVERNANCE / METADATA
--------------------------------------------------------------------------------

-- Data quality tests and contracts
create or replace table SUPPLY_META.DATA_TESTS (
    TEST_NAME string comment 'Unique name for the test',
    SEVERITY  string comment 'Severity (ERROR, WARNING)',
    SQL_TEXT  string comment 'SQL expression to run as test'
)
comment='Repository of data quality tests executed via tasks/procedures.';

-- Model registry for ML artifacts
create or replace table SUPPLY_ML.MODEL_REGISTRY (
    STORE_ID   string comment 'Store ID',
    SKU_ID     string comment 'SKU ID',
    MODEL_URI  string comment 'Path to stored model artifact',
    CREATED_AT timestamp_ntz default current_timestamp(),
    UPDATED_AT timestamp_ntz
)
comment='Registry of trained ML models by store/SKU with artifact URI.';
