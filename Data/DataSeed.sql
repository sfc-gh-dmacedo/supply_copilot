use database SUPPLY_COPILOT;

-- Quantidades (ajuste à vontade)
set store_count = 8;
set sku_count   = 700;
set days_hist   = 365;  -- dias para trás a partir de hoje


truncate table if exists SUPPLY_SILVER.DIM_STORE;
truncate table if exists SUPPLY_SILVER.DIM_SKU;
truncate table if exists SUPPLY_SILVER.DIM_DATE;

truncate table if exists SUPPLY_BRONZE.FACT_SALES_RAW;
truncate table if exists SUPPLY_BRONZE.FACT_INVENTORY_RAW;
truncate table if exists SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW;
truncate table if exists SUPPLY_BRONZE.FACT_PROMO_RAW;


-- Lojas
insert into SUPPLY_SILVER.DIM_STORE (store_id, store_name, region, cluster, opened_date)
select 'S' || (seq4()+1), 'Store ' || (seq4()+1),
       iff(mod(seq4(),2)=0,'NORTH','SOUTH') as region,
       'Cluster ' || mod(seq4(),3) as cluster,
       dateadd('day', -uniform(2000,4000,random()), current_date())
from table(generator(rowcount => $store_count));

-- SKUs
insert into SUPPLY_SILVER.DIM_SKU (sku_id, sku_name, category, subcategory, unit_cost, unit_price)
select 'SKU' || (seq4()+1), 'Product ' || (seq4()+1),
       'Category ' || mod(seq4(),5), 'Sub ' || mod(seq4(),10),
       round(10 + uniform(1,10,random()),2) as unit_cost,
       round(20 + uniform(5,30,random()),2)  as unit_price
from table(generator(rowcount => $sku_count));

-- Calendário (de hoje - days_hist até hoje)
insert into SUPPLY_SILVER.DIM_DATE (d, dow, dom, month, year, is_weekend, week_of_year, month_name, is_holiday)
select d,
       dayofweekiso(d) as dow,
       day(d)          as dom,
       month(d)        as month,
       year(d)         as year,
       iff(dayofweekiso(d) in (6,7), true,false) as is_weekend,
       weekiso(d)      as week_of_year,
       to_char(d,'MON') as month_name,
       false as is_holiday
from (
  select dateadd('day', -$days_hist + seq4(), current_date()) as d
  from table(generator(rowcount => $days_hist+1))
);


-- ~10% das combinações com promoções de 3 a 10 dias
insert into SUPPLY_BRONZE.FACT_PROMO_RAW (start_date, end_date, store_id, sku_id, promo_type, lift_factor)
select start_date,
       dateadd('day', uniform(3,10,random()), start_date) as end_date,
       s.store_id, k.sku_id,
       case when mod(abs(hash(s.store_id||k.sku_id)),3)=0 then 'DISCOUNT'
            when mod(abs(hash(s.store_id||k.sku_id)),3)=1 then 'BUNDLE'
            else 'DISPLAY' end as promo_type,
       round(1.2 + uniform(0,0.8,random()),2) as lift_factor
from SUPPLY_SILVER.DIM_STORE s
join SUPPLY_SILVER.DIM_SKU   k
  on uniform(0,100,random()) < 10  -- 10% sampling
join (
  select dateadd('day', -uniform(0,60,random()), current_date()) as start_date
  from table(generator(rowcount => 100))
) pick on true;


-- Gera algumas linhas de PO/ship para os últimos 60 dias (~20% dos dias)
insert into SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW
select
  'PO_'||to_varchar(d)||'_'||s.store_id||'_'||k.sku_id as po_id,
  s.store_id, k.sku_id,
  round(uniform(20,80,random()),0) as ordered_qty,
  dateadd('day', uniform(1,7,random()), d) as promised_date,
  -- às vezes embarca menos que o pedido
  round(iff(uniform(0,100,random())<85, ordered_qty, ordered_qty - uniform(1,10,random())),0) as shipped_qty,
  dateadd('day', uniform(0,5,random()), d) as shipped_date,
  dateadd('day', uniform(1,10,random()), d) as delivered_date
from SUPPLY_SILVER.DIM_STORE s
cross join SUPPLY_SILVER.DIM_SKU k
join (
  select dateadd('day', -seq4(), current_date()) as d
  from table(generator(rowcount => 60))
) days on uniform(0,100,random()) < 20; -- ~20% dos dias têm PO


-- Snapshot diário simples por loja/SKU
insert into SUPPLY_BRONZE.FACT_INVENTORY_RAW
select d.d as as_of_date, s.store_id, k.sku_id,
       greatest(0, round(50 + uniform(0,150,random()) - mod(abs(hash(k.sku_id)),20),0)) as on_hand,
       greatest(0, round(uniform(0,50,random()),0)) as on_order
from SUPPLY_SILVER.DIM_DATE d
cross join SUPPLY_SILVER.DIM_STORE s
cross join SUPPLY_SILVER.DIM_SKU   k
where d.d >= dateadd('day', -$days_hist, current_date());



-- 6.6) Inserir no RAW
insert into SUPPLY_BRONZE.FACT_SALES_RAW (txn_ts, store_id, sku_id, qty, revenue)
with promo_days as (
  select p.store_id, p.sku_id, dd.d, p.lift_factor
  from SUPPLY_BRONZE.FACT_PROMO_RAW p
  join SUPPLY_SILVER.DIM_DATE dd
    on dd.d between p.start_date and p.end_date
),
base_grid as (
  select dd.d, s.store_id, k.sku_id
  from SUPPLY_SILVER.DIM_DATE dd
  cross join SUPPLY_SILVER.DIM_STORE s
  cross join SUPPLY_SILVER.DIM_SKU   k
  where dd.d >= dateadd('day', -$days_hist, current_date())
    and uniform(0,100,random()) < 70
),
daily_qty as (
  select g.d,
         g.store_id,
         g.sku_id,
         case dayofweekiso(g.d) when 6 then 1.1 when 7 then 1.2 else 1.0 end as dow_boost,
         coalesce(p.lift_factor, 1.0) as lift,
         greatest(
           0,
           floor( (5 + mod(abs(hash(g.sku_id||g.store_id)),10)) * 
                  case dayofweekiso(g.d) when 6 then 1.1 when 7 then 1.2 else 1.0 end *
                  coalesce(p.lift_factor, 1.0) +
                  uniform(-2,2,random())
           )
         )::number(12,0) as qty
  from base_grid g
  left join promo_days p
    on p.d=g.d and p.store_id=g.store_id and p.sku_id=g.sku_id
),
priced as (
  select q.d,
         q.store_id,
         q.sku_id,
         q.qty,
         sk.unit_price,
         round(q.qty * sk.unit_price * (1 + uniform(-0.05,0.05,random())), 2) as revenue
  from daily_qty q
  join SUPPLY_SILVER.DIM_SKU sk
    on sk.sku_id = q.sku_id
  where q.qty > 0
),
expanded as (
  select
    timestampadd(
      second,
      cast(uniform(0, 86399, random()) as int),
      to_timestamp_ntz(d)
    ) as txn_ts,
    store_id,
    sku_id,
    qty::number(12,3) as qty,
    revenue
  from priced
)
select txn_ts, store_id, sku_id, qty, revenue
from expanded;




-- Contagens básicas
select count(*) as stores from SUPPLY_SILVER.DIM_STORE;
select count(*) as skus   from SUPPLY_SILVER.DIM_SKU;
select min(d), max(d), count(*) from SUPPLY_SILVER.DIM_DATE;

select count(*) as sales_rows from SUPPLY_BRONZE.FACT_SALES_RAW;
select count(*) as inv_rows   from SUPPLY_BRONZE.FACT_INVENTORY_RAW;
select count(*) as po_rows    from SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW;
select count(*) as promo_rows from SUPPLY_BRONZE.FACT_PROMO_RAW;

-- Amostras
select * from SUPPLY_BRONZE.FACT_SALES_RAW order by txn_ts desc limit 20;
select * from SUPPLY_BRONZE.FACT_INVENTORY_RAW order by as_of_date desc limit 20;
select * from SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW order by promised_date desc limit 20;
select * from SUPPLY_BRONZE.FACT_PROMO_RAW order by start_date desc limit 20;
