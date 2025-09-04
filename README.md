# Goal
Improve On-Time In-Full (OTIF), reduce stockouts, and cut inventory days by forecasting demand, detecting anomalies, and proposing cost-benefit actions per store/SKU.

# Data & Model
- **Layers**: `RAW → BRONZE → SILVER → GOLD → FEATURES → ML → APP → META`
- **Core entities**: stores, SKUs, sales, inventory, POs/shipments, promos, calendar
- **GOLD metrics**: OTIF (on-time, in-full, combined) and daily stockout/lost-sales flags

# Pipeline & Architecture
- **Dynamic Tables** for curating and aggregating sales/inventory and computing KPIs  
- **Snowpark ML** (e.g., XGBoost/Prophet) for daily forecasts; UDF for inference  
- **Residuals/z-scores** for anomaly detection; days-of-cover for stockout risk  

# AI Recommendations
- **Cortex LLM** generates grounded action plans (reorder/transfer/price/exposure)  
- Outputs estimated cost/impact as validated JSON for the app  

# Application
- **Streamlit in Snowflake UI**:  
  - KPI overview  
  - Prioritized action backlog  
  - Store/SKU detail (actual vs forecast, inventory, anomalies, coverage)  

# Security & Governance
- **RBAC roles**  
- **Row Access Policies** (by region)  
- **Masking Policies** (sensitive costs)  
- Tags, lineage, data-quality tests, and alerts  

# Orchestration
- **Tasks** for:  
  - Ingestion  
  - Feature refresh  
  - Nightly training  
  - Daily inference  
  - Risk/alerts  
  - Recommendation generation  

# Success Metrics
- ↑ OTIF points  
- ↓ Stockout rate  
- ↓ Inventory days  
- Measurable savings  

_Optional: A/B test across stores to quantify impact._
