comment on schema SUPPLY_RAW     is 'Landing zone for raw data. Ownership: Data Engineering.';
comment on schema SUPPLY_BRONZE  is 'Staging/normalized layer. Ownership: Data Engineering.';
comment on schema SUPPLY_SILVER  is 'Curated/conformed layer. Ownership: Data Engineering.';
comment on schema SUPPLY_GOLD    is 'Business metric layer. Ownership: Analytics/Planning.';
comment on schema SUPPLY_FEATURES is 'Feature store / time-series features. Ownership: DS/ML.';
comment on schema SUPPLY_ML      is 'Models, forecasts, anomalies. Ownership: DS/ML.';
comment on schema SUPPLY_APP     is 'App-facing views/tables for Streamlit/BI. Ownership: App Team.';
comment on schema SUPPLY_META    is 'Policies, glossary, DQ tests, contracts. Ownership: Governance.';