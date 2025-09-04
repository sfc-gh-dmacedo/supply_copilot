-- OTIF


create or replace dynamic table SUPPLY_GOLD.OTIF_ORDERLINE
target_lag = '15 minutes'
warehouse = WH_ETL
as
select po_id, store_id, sku_id, ordered_qty, shipped_qty, promised_date, delivered_date,
       iff(delivered_date <= promised_date, 1, 0) as on_time,
       iff(shipped_qty >= ordered_qty, 1, 0) as in_full,
       iff(delivered_date <= promised_date and shipped_qty >= ordered_qty, 1, 0) as otif_flag
from SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW;



SELECT * FROM SUPPLY_GOLD.OTIF_ORDERLINE;