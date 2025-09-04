-- DAILY SALES


create or replace dynamic table SUPPLY_SILVER.FACT_SALES_DAILY
target_lag = '5 minutes'
warehouse = WH_ETL
as
select to_date(txn_ts) as d, store_id, sku_id,
       sum(qty) as qty, sum(revenue) as revenue
from SUPPLY_BRONZE.FACT_SALES_RAW
group by 1,2,3;