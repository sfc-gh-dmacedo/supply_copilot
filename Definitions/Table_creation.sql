create or replace table SUPPLY_SILVER.DIM_STORE (
  store_id string primary key,
  store_name string,
  region string,
  cluster string,
  opened_date date
);

create or replace table SUPPLY_SILVER.DIM_SKU (
  sku_id string primary key,
  sku_name string,
  category string,
  subcategory string,
  unit_cost number(12,4),     -- mascarado por política
  unit_price number(12,4)
);
