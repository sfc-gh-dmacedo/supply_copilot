create warehouse if not exists WH_ETL
  with warehouse_size = 'MEDIUM'
  warehouse_type = 'STANDARD'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'ETL/Dynamic Tables/feature builds';

create warehouse if not exists WH_ML
  with warehouse_size = 'MEDIUM'
  warehouse_type = 'STANDARD'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'Model training & inference (Snowpark)';

create warehouse if not exists WH_APP
  with warehouse_size = 'SMALL'
  warehouse_type = 'STANDARD'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'Streamlit / BI / lightweight queries';