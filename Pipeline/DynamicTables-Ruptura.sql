create or replace dynamic table SUPPLY_GOLD.STOCKOUT_DAILY
target_lag = '15 minutes' warehouse = WH_ETL as
select s.d, s.store_id, s.sku_id,
s.qty, i.on_hand,
iff(i.on_hand <= 0 and s.qty > 0, 1, 0) as lost_sale_flag
from SUPPLY_SILVER.FACT_SALES_DAILY s
join SUPPLY_SILVER.INVENTORY_POSITION i
on s.d = i.d and s.store_id=i.store_id and s.sku_id=i.sku_id;