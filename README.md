Goal
Improve On-Time In-Full (OTIF), reduce stockouts, and cut inventory days by forecasting demand, detecting anomalies, and proposing cost-benefit actions per store/SKU.

Data & Model

Layers: RAW → BRONZE → SILVER → GOLD → FEATURES → ML → APP → META.

Core entities: stores, SKUs, sales, inventory, POs/shipments, promos, calendar.

GOLD metrics: OTIF (on-time, in-full, combined) and daily stockout/lost-sales flags.

Pipeline & Architecture

Dynamic Tables for curating and aggregating sales/inventory and computing KPIs.

Snowpark ML (e.g., XGBoost/Prophet) for daily forecasts; UDF for inference.

Residuals/z-scores for anomaly detection; days-of-cover for stockout risk.

AI Recommendations

Cortex LLM generates grounded action plans (reorder/transfer/price/exposure) with estimated cost/impact, returned as validated JSON for the app.

Application

Streamlit in Snowflake UI: KPI overview, prioritized action backlog, and store/SKU detail (actual vs forecast, inventory, anomalies, coverage).

Security & Governance

RBAC roles, Row Access Policies (by region), Masking Policies (sensitive costs), tags, lineage, data-quality tests and alerts.

Orchestration

Tasks for ingestion, feature refresh, nightly training, daily inference, risk/alerts, and recommendation generation.

Success Metrics

↑ OTIF points, ↓ stockout rate, ↓ inventory days, and measurable savings.

Optional A/B test across stores to quantify impact.