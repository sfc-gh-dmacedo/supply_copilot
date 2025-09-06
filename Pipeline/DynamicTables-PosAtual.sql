create or replace dynamic table SUPPLY_SILVER.INVENTORY_POSITION
target_lag = '5 minutes'
warehouse = WH_ETL
as
select i.as_of_date as d, i.store_id, i.sku_id,
       i.on_hand, i.on_order,
       coalesce(p.promo_active, false) as promo_active
from SUPPLY_BRONZE.FACT_INVENTORY_RAW i
left join (
  select dd.d, pr.store_id, pr.sku_id, true as promo_active
  from SUPPLY_BRONZE.FACT_PROMO_RAW pr
  join SUPPLY_SILVER.DIM_DATE dd
    on dd.d between pr.start_date and pr.end_date
) p on p.d = i.as_of_date and p.store_id = i.store_id and p.sku_id = i.sku_id;