create or replace stage SUPPLY_RAW.STAGE_RAW
  comment='Internal stage for raw ingestions (CSV/JSON/Parquet)';

create or replace stage SUPPLY_ML.STAGE_MODELS
  comment='Internal stage for serialized ML artifacts (e.g., xgb, pickle)';

create or replace stage SUPPLY_APP.STAGE_ASSETS
  comment='Internal stage for Streamlit/static assets';