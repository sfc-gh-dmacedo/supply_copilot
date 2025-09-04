create or replace schema SUPPLY_RAW
  comment = 'Landing zone: raw files and untouched ingestions';

create or replace schema SUPPLY_BRONZE
  comment = 'Normalized staging with initial typing and light cleansing';

create or replace schema SUPPLY_SILVER
  comment = 'Curated, conformed data (dimensions, validated facts)';

create or replace schema SUPPLY_GOLD
  comment = 'Business metrics & KPIs (OTIF, stockout, dashboards)';

create or replace schema SUPPLY_FEATURES
  comment = 'Feature tables and time-series features for ML';

create or replace schema SUPPLY_ML
  comment = 'Model artifacts, forecasts, anomalies, registries';

create or replace schema SUPPLY_APP
  comment = 'Secure, consumable views/tables exposed to apps (Streamlit)';

create or replace schema SUPPLY_META
  comment = 'Metadata, data contracts, policies, glossary, QA results';