create or replace dynamic table SUPPLY_FEATURES.DEMAND_FEATURES
target_lag='1 hour' warehouse=WH_ETL as
with base as (
  select s.d, s.store_id, s.sku_id, s.qty,
         lag(s.qty,1) over(partition by s.store_id, s.sku_id order by s.d) as qty_lag1,
         lag(s.qty,7) over(partition by s.store_id, s.sku_id order by s.d) as qty_lag7,
         avg(s.qty) over(partition by s.store_id, s.sku_id order by s.d rows between 6 preceding and current row) as qty_avg7,
         dd.dow, dd.is_holiday, 
         coalesce(ip.promo_active, false) as promo_active
  from SUPPLY_SILVER.FACT_SALES_DAILY s
  join SUPPLY_SILVER.DIM_DATE dd on dd.d = s.d
  left join SUPPLY_SILVER.INVENTORY_POSITION ip on ip.d = s.d and ip.store_id = s.store_id and ip.sku_id = s.sku_id
)
select * from base where qty is not null;