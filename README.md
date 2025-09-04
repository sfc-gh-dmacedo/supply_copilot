Visão geral

Objetivo: Prever demanda e risco de ruptura por loja/SKU, sugerir planos de ação com custo/benefício (transferência, reorder, ajuste de preço/exposição) e medir impacto em OTIF e dias de estoque.

Componentes Snowflake:

Ingest/Governança: Stages → RAW/BRONZE/SILVER/GOLD, tags, masking, row access

Feature/Model: Dynamic Tables, Snowpark (XGBoost/Prophet), UDF de inferência

Detecção: Dynamic Tables (resíduos/z-score), Alerts/Tasks

IA: Cortex LLM (recomendações textuais, grounded em fatos)

App: Streamlit in Snowflake (UI para planners)

Modelo de dados (camadas)
Schemas

SUPPLY_RAW: ingestão “como veio”

SUPPLY_BRONZE: normalizado, tipado

SUPPLY_SILVER: curado (chaves, dimensões, qualidade)

SUPPLY_GOLD: métricas de negócio (OTIF, ruptura, S&OP)

SUPPLY_FEATURES: séries/featurização para ML

SUPPLY_ML: artefatos de modelo e outputs

SUPPLY_APP: views seguras para o app

SUPPLY_META: config, contratos de dados, catálogo semântico

Tabelas (chave/colunas principais)

Dimensões

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


Fatos operacionais

create or replace table SUPPLY_BRONZE.FACT_SALES_RAW (
  txn_ts timestamp_ntz, store_id string, sku_id string, qty number(12,3), revenue number(12,2)
);

create or replace table SUPPLY_BRONZE.FACT_INVENTORY_RAW (
  as_of_date date, store_id string, sku_id string, on_hand number(12,3), on_order number(12,3)
);

create or replace table SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW (
  po_id string, store_id string, sku_id string, ordered_qty number(12,3),
  promised_date date, shipped_qty number(12,3), shipped_date date, delivered_date date
);

create or replace table SUPPLY_BRONZE.FACT_PROMO_RAW (
  start_date date, end_date date, store_id string, sku_id string, promo_type string, lift_factor number(6,3)
);


Calendário

create or replace table SUPPLY_SILVER.DIM_DATE (
  d date primary key, dow tinyint, dom tinyint, month tinyint, year smallint,
  is_weekend boolean, week_of_year tinyint, month_name string, is_holiday boolean
);


Dica: para dados sintéticos rápidos, você pode usar TABLE(GENERATOR(ROWCOUNT=>...)) e arrays para distribuir store_id/sku_id.

Qualidade & contratos de dados

Regras básicas (exemplos):

qty >= 0, on_hand >= 0, unit_cost <= unit_price * 1.5 (sanidade)

DIM_* sem nulos em chaves; FKs válidas nos fatos

Tabela de checagens

create or replace table SUPPLY_META.DATA_TESTS (
  test_name string, severity string, sql_text string
);

insert into SUPPLY_META.DATA_TESTS values
('no_negative_qty', 'ERROR', 'select count(*) c from SUPPLY_BRONZE.FACT_SALES_RAW where qty < 0 having c>0'),
('fk_sales_store', 'ERROR', 'select count(*) c from SUPPLY_BRONZE.FACT_SALES_RAW s left join SUPPLY_SILVER.DIM_STORE d using(store_id) where d.store_id is null having c>0');


Runner (Task + Procedure)

Procedure executa cada sql_text; grava resultados em SUPPLY_META.DATA_TEST_RESULTS e dispara ALERT em caso ERROR.

Curadoria e enriquecimento (Dynamic Tables)

Normalização BRONZE → SILVER (exemplo vendas diárias)

create or replace dynamic table SUPPLY_SILVER.FACT_SALES_DAILY
target_lag = '5 minutes'
warehouse = WH_ETL
as
select
  to_date(txn_ts) as d, store_id, sku_id,
  sum(qty) as qty, sum(revenue) as revenue
from SUPPLY_BRONZE.FACT_SALES_RAW
group by 1,2,3;


Inventário atual + posição (SILVER)

create or replace dynamic table SUPPLY_SILVER.INVENTORY_POSITION
target_lag = '5 minutes'
warehouse = WH_ETL
as
select i.as_of_date d, i.store_id, i.sku_id,
       i.on_hand, i.on_order,
       coalesce(p.promo_active, false) as promo_active
from SUPPLY_BRONZE.FACT_INVENTORY_RAW i
left join (
  select d::date as d, store_id, sku_id, true as promo_active
  from SUPPLY_BRONZE.FACT_PROMO_RAW, lateral flatten(input => sequence(start_date, end_date))
) p using (d, store_id, sku_id);

Métricas GOLD (OTIF e Ruptura)

OTIF por pedido (GOLD)

create or replace dynamic table SUPPLY_GOLD.OTIF_ORDERLINE
target_lag = '15 minutes' warehouse = WH_ETL as
select
  po_id, store_id, sku_id, ordered_qty, shipped_qty, promised_date, delivered_date,
  iff(delivered_date <= promised_date, 1, 0) as on_time,
  iff(shipped_qty >= ordered_qty, 1, 0)      as in_full,
  iff(delivered_date <= promised_date and shipped_qty >= ordered_qty, 1, 0) as otif_flag
from SUPPLY_BRONZE.FACT_POSHIPMENTS_RAW;

create or replace view SUPPLY_GOLD.OTIF_KPI as
select store_id, sku_id, date_trunc('week', promised_date) wk,
       avg(otif_flag) as otif_rate,
       avg(on_time)  as on_time_rate,
       avg(in_full)  as in_full_rate
from SUPPLY_GOLD.OTIF_ORDERLINE
group by 1,2,3;


Ruptura (stockout)

create or replace dynamic table SUPPLY_GOLD.STOCKOUT_DAILY
target_lag = '15 minutes' warehouse = WH_ETL as
select s.d, s.store_id, s.sku_id,
       s.qty, i.on_hand,
       iff(i.on_hand <= 0 and s.qty > 0, 1, 0) as lost_sale_flag
from SUPPLY_SILVER.FACT_SALES_DAILY s
join SUPPLY_SILVER.INVENTORY_POSITION i
  on s.d = i.d and s.store_id=i.store_id and s.sku_id=i.sku_id;

Featurização e Treinamento (Snowpark)
Features (lags, média móvel, sazonalidade)
create or replace dynamic table SUPPLY_FEATURES.DEMAND_FEATURES
target_lag='1 hour' warehouse=WH_ETL as
with base as (
  select d, store_id, sku_id, qty,
         lag(qty,1) over(partition by store_id,sku_id order by d) as qty_lag1,
         lag(qty,7) over(partition by store_id,sku_id order by d) as qty_lag7,
         avg(qty) over(partition by store_id,sku_id order by d rows between 6 preceding and current row) as qty_ma7,
         dow, is_holiday, promo_active
  from SUPPLY_SILVER.FACT_SALES_DAILY s
  join SUPPLY_SILVER.DIM_DATE dd on dd.d = s.d
  left join SUPPLY_SILVER.INVENTORY_POSITION ip on ip.d = s.d and ip.store_id=s.store_id and ip.sku_id=s.sku_id
)
select * from base where qty is not null;

Treinamento (ex.: XGBoost regressão) — Stored Procedure Snowpark Python

Salva modelo por store_id, sku_id em stage @ml/models/ e registra em tabela.

-- Python (Snowpark) pseudo-código dentro de uma PROC
create or replace procedure SUPPLY_ML.TRAIN_XGB(MIN_HISTORY_DAYS INT)
returns string
language python
runtime_version = '3.10'
packages = ('pandas','numpy','xgboost','scikit-learn','cloudpickle')
handler = 'run'
as
$$
from snowflake.snowpark import Session
import pandas as pd, numpy as np, cloudpickle, os, json
from xgboost import XGBRegressor
def run(session: Session, MIN_HISTORY_DAYS: int):
    df = session.table("SUPPLY_FEATURES.DEMAND_FEATURES")
    # Filtra séries com histórico mínimo
    series = (df.group_by('STORE_ID','SKU_ID')
                .agg((session.sql('count(*)')).alias('N'))
                .filter(f'N >= {MIN_HISTORY_DAYS}').collect())
    saved = 0
    for r in series:
        sid, kid = r['STORE_ID'], r['SKU_ID']
        data = (df.filter(f"STORE_ID='{sid}' AND SKU_ID='{kid}'")
                  .to_pandas())
        y  = data['QTY'].astype(float).values
        X  = data[['QTY_LAG1','QTY_LAG7','QTY_MA7','DOW','IS_HOLIDAY','PROMO_ACTIVE']].fillna(0).values
        if len(y) < MIN_HISTORY_DAYS: 
            continue
        model = XGBRegressor(max_depth=6, n_estimators=200, learning_rate=0.1)
        model.fit(X, y)
        artifact = cloudpickle.dumps(model)
        path = f"@ML/MODELS/{sid}__{kid}.xgb"
        session.sql("put file://model.bin @ML/MODELS overwrite=true").collect()  # (use temp file)
        session.file.put_stream(path, artifact)  # se sua conta suportar put_stream
        session.sql(f"""
            merge into SUPPLY_ML.MODEL_REGISTRY t
            using (select '{sid}' STORE_ID, '{kid}' SKU_ID) s
            on t.STORE_ID=s.STORE_ID and t.SKU_ID=s.SKU_ID
            when matched then update set UPDATED_AT=current_timestamp()
            when not matched then insert (STORE_ID,SKU_ID,MODEL_URI,CREATED_AT)
                 values ('{sid}','{kid}','@ML/MODELS/{sid}__{kid}.xgb',current_timestamp())
        """).collect()
        saved += 1
    return f"saved {saved} models"
$$;


Obs.: conforme o setup da conta, você pode preferir 1 modelo global com STORE_ID/SKU_ID como features e embeddings categóricas, ou Prophet por série. Ajuste pacotes e runtime à sua versão de Snowpark.

Inferência como UDF
create or replace function SUPPLY_ML.PREDICT_XGB(store_id string, sku_id string,
                                                 qty_lag1 float, qty_lag7 float, qty_ma7 float,
                                                 dow int, is_holiday boolean, promo_active boolean)
returns float
language python
runtime_version='3.10'
packages=('cloudpickle','numpy','xgboost')
handler='predict'
as
$$
import cloudpickle, numpy as np
from snowflake.snowpark import Session
def load_model(session, store_id, sku_id):
    path = f"@ML/MODELS/{store_id}__{sku_id}.xgb"
    b = session.file.get_stream(path).read()
    return cloudpickle.loads(b)
def predict(session, store_id, sku_id, q1,q7,ma7,dow,hday,promo):
    m = load_model(session, store_id, sku_id)
    X = np.array([[q1,q7,ma7,dow,1 if hday else 0, 1 if promo else 0]], dtype=float)
    return float(m.predict(X)[0])
$$;

Pipeline de previsão diária
create or replace dynamic table SUPPLY_ML.FORECAST_DAILY
target_lag='1 hour' warehouse=WH_ETL as
select f.d, f.store_id, f.sku_id,
       SUPPLY_ML.PREDICT_XGB(f.store_id, f.sku_id, f.qty_lag1, f.qty_lag7, f.qty_ma7, f.dow, f.is_holiday, f.promo_active) as yhat
from SUPPLY_FEATURES.DEMAND_FEATURES f
qualify row_number() over(partition by store_id, sku_id order by d desc)=1; -- previsão do próximo dia

Detecção de anomalias e risco de ruptura

Resíduo & z-score

create or replace dynamic table SUPPLY_ML.RESIDUALS
target_lag='1 hour' warehouse=WH_ETL as
select s.d, s.store_id, s.sku_id, s.qty as actual, f.yhat as forecast,
       (s.qty - f.yhat) as resid
from SUPPLY_SILVER.FACT_SALES_DAILY s
join SUPPLY_ML.FORECAST_DAILY f using (d, store_id, sku_id);

create or replace dynamic table SUPPLY_ML.ANOMALIES
target_lag='1 hour' warehouse=WH_ETL as
with stats as (
  select store_id, sku_id, avg(resid) mu, stddev_samp(resid) sd
  from SUPPLY_ML.RESIDUALS
  group by 1,2
)
select r.*, (r.resid - s.mu)/nullif(s.sd,0) as zscore,
       iff(abs((r.resid - s.mu)/nullif(s.sd,0)) >= 3, 1, 0) as anomaly_flag
from SUPPLY_ML.RESIDUALS r join stats s using (store_id, sku_id);


Risco de ruptura (projeção de dias de cobertura)

create or replace dynamic table SUPPLY_GOLD.STOCKOUT_RISK
target_lag='1 hour' warehouse=WH_ETL as
select i.d, i.store_id, i.sku_id, i.on_hand, i.on_order, f.yhat as next_day_demand,
       iff(f.yhat>0, i.on_hand / f.yhat, null) as days_of_cover,
       iff(coalesce(on_hand,0) < f.yhat*2, 1, 0) as risk_flag
from SUPPLY_SILVER.INVENTORY_POSITION i
join SUPPLY_ML.FORECAST_DAILY f using (d, store_id, sku_id);

Recomendações (Cortex LLM, grounded)

Tabela de contexto (fatos resumidos)

create or replace view SUPPLY_APP.RECO_CONTEXT as
select r.d, r.store_id, r.sku_id, d.category, d.unit_cost, d.unit_price,
       k.otif_rate, s.days_of_cover, a.zscore, a.anomaly_flag, inv.on_hand, inv.on_order,
       avg(sales.qty) over(partition by r.store_id, r.sku_id order by r.d rows between 28 preceding and current row) as avg28
from SUPPLY_GOLD.STOCKOUT_RISK s
join SUPPLY_ML.ANOMALIES a using (d, store_id, sku_id)
join SUPPLY_SILVER.DIM_SKU d using (sku_id)
join SUPPLY_GOLD.OTIF_KPI k using (store_id, sku_id)
join SUPPLY_SILVER.INVENTORY_POSITION inv on inv.d=s.d and inv.store_id=s.store_id and inv.sku_id=s.sku_id
join SUPPLY_SILVER.FACT_SALES_DAILY sales using (d, store_id, sku_id);


Prompt template parametrizado

-- Config
create or replace table SUPPLY_META.LLM_CONFIG (model_name string, temperature number(3,2), top_p number(3,2));
insert into SUPPLY_META.LLM_CONFIG values ('<seu_modelo_cortex>', 0.2, 0.9);

-- Chamada (exemplo seletivo)
with cfg as (select * from SUPPLY_META.LLM_CONFIG limit 1),
ctx as (
  select * from SUPPLY_APP.RECO_CONTEXT
  where d = current_date()
  and (risk_flag=1 or anomaly_flag=1)
  qualify row_number() over(partition by store_id, sku_id order by abs(zscore) desc)=1
)
select
  store_id, sku_id,
  snowflake.cortex.complete(
    (select model_name from cfg),
    $$Você é um planejador de supply. Com base nos dados, recomende até 3 ações priorizadas com custo/benefício e impacto em OTIF e risco de ruptura. 
    Dê números estimados (ordem de grandeza) e justificativas curtas. 
    Dados:
    {{JSON_DADOS}}
    Saída esperada (JSON): 
    { "acoes": [ { "acao": "...", "custo_estimado": "...", "beneficio_estimado": "...", "racional": "..." } ], "resumo": "..." }$$
    , object_construct('temperature',(select temperature from cfg),'top_p',(select top_p from cfg)),
    object_construct('JSON_DADOS', to_json(object_construct(*)))
  ) as plano_acao_json
from ctx;


Boas práticas:

Forçar saída JSON e validar com try_parse_json.

Parametrizar o modelo em tabela/config.

Opcional: Cortex Search com embeddings de playbooks / políticas logísticas para citar fontes internas do cliente.

App (Streamlit in Snowflake)
Estrutura (resumo)

Página 1: Overview (KPI cards: OTIF semana, rupturas, savings estimado)

Página 2: Backlog de Ações (tabela/kanban das recomendações; “Aceitar”, “Ajustar”, “Ignorar”)

Página 3: Loja/SKU (série temporal: demanda real vs previsão, on_hand, anomalias, dias de cobertura)

Página 4: AB Test & Impacto (grupo teste vs controle)

Esqueleto (trecho)
import streamlit as st
from snowflake.snowpark.context import get_active_session
session = get_active_session()

st.title("Supply Copilot – OTIF & Ruptura")

tab1, tab2, tab3 = st.tabs(["KPI", "Ações", "Detalhe"])
with tab1:
    kpi = session.sql("select avg(otif_rate) otif, sum(lost_sale_flag) lost from SUPPLY_GOLD.OTIF_KPI k join SUPPLY_GOLD.STOCKOUT_DAILY s using(store_id,sku_id)").to_pandas()
    st.metric("OTIF (semana)", f"{kpi['OTIF'][0]:.1%}")
    st.metric("Possíveis vendas perdidas (Mês)", int(kpi['LOST'][0]))
with tab2:
    actions = session.sql("select store_id, sku_id, plano_acao_json from SUPPLY_APP.RECOMMENDATIONS order by 1,2").to_pandas()
    st.dataframe(actions)
with tab3:
    store = st.selectbox("Store", session.sql("select distinct store_id from SUPPLY_SILVER.DIM_STORE").to_pandas()['STORE_ID'])
    sku   = st.selectbox("SKU", session.sql(f"select distinct sku_id from SUPPLY_SILVER.FACT_SALES_DAILY where store_id='{store}'").to_pandas()['SKU_ID'])
    df = session.sql(f"""
       select s.d, s.qty actual, f.yhat forecast, i.on_hand, a.zscore
       from SUPPLY_SILVER.FACT_SALES_DAILY s
       left join SUPPLY_ML.FORECAST_DAILY f using(d,store_id,sku_id)
       left join SUPPLY_SILVER.INVENTORY_POSITION i using(d,store_id,sku_id)
       left join SUPPLY_ML.ANOMALIES a using(d,store_id,sku_id)
       where s.store_id='{store}' and s.sku_id='{sku}'
       order by d
    """).to_pandas()
    st.line_chart(df.set_index('D')[['ACTUAL','FORECAST','ON_HAND']])

Orquestração (Tasks)

Ordem sugerida:

Ingest (RAW loaders)

Dynamic Tables (BRONZE→SILVER→GOLD→FEATURES) — target_lag já cuida

Treinamento (noite/semana): call SUPPLY_ML.TRAIN_XGB(90);

Inferência (diário) — FORECAST_DAILY

Anomalia/Risco — ANOMALIES, STOCKOUT_RISK

Recomendações — view/tabela SUPPLY_APP.RECOMMENDATIONS populada por SELECT com cortex.complete

Alertas — e-mail/Slack via external function (opcional)

Exemplo:

create or replace task SUPPLY_ORCH.TRAIN_NIGHTLY
warehouse=WH_ETL schedule='USING CRON 0 2 * * * UTC'
as call SUPPLY_ML.TRAIN_XGB(90);

Segurança & Governança
RBAC (funções)

ROLE_SUPPLY_VIEWER: leitura em SUPPLY_APP

ROLE_SUPPLY_PLANNER: viewer + acionar recomendações (insert/approve)

ROLE_SUPPLY_DATAENG: DDL/DML nas camadas

ROLE_SUPPLY_ADMIN: tudo + políticas/tagging

Row Access (região da loja)
create or replace row access policy SUPPLY_META.RAP_REGION as
(store_region string) returns boolean ->
  case when current_role() in ('ROLE_SUPPLY_ADMIN','ROLE_SUPPLY_DATAENG')
       then true
       when current_role() = 'ROLE_SUPPLY_PLANNER'
       then store_region in (select region from SUPPLY_META.USER_REGION_ACCESS where user_name = current_user())
       else false end;

alter table SUPPLY_SILVER.DIM_STORE add row access policy SUPPLY_META.RAP_REGION on (region);

Masking (custo unitário)
create or replace masking policy SUPPLY_META.MP_UNIT_COST as (val number) returns number ->
  case when current_role() in ('ROLE_SUPPLY_ADMIN','ROLE_SUPPLY_DATAENG') then val else null end;

alter table SUPPLY_SILVER.DIM_SKU modify column unit_cost set masking policy SUPPLY_META.MP_UNIT_COST;

Tags de classificação
create or replace tag DATA_CATEGORY;
create or replace tag SENSITIVITY_LEVEL;

alter table SUPPLY_SILVER.DIM_SKU set tag DATA_CATEGORY = 'masterdata';
alter table SUPPLY_BRONZE.FACT_SALES_RAW set tag SENSITIVITY_LEVEL = 'confidential';

Linhagem & catálogo

Padronize nomes/descrições (COMMENT ON ...)

Use OBJECT_DEPENDENCIES, DYNAMIC TABLE GRAPH (quando disponível) para inspecionar DAG

Mantenha glossário semântico em SUPPLY_META.BUSINESS_GLOSSARY (colunas: termo, definição, table.column, owner)

Métricas de sucesso & experimento

KPIs primários

OTIF: +X pontos (semanal)

Dias de estoque: −Y% nas SKUs com plano

Rupturas: −Z% (contagem de lost_sale_flag)

Avaliação causal (A/B por loja)

-- Define grupos
create or replace table SUPPLY_META.EXPERIMENT_STORES as
select store_id, iff(uniform(0,1,random())<0.5,'TEST','CONTROL') as group from SUPPLY_SILVER.DIM_STORE;

-- Scoreboard diário
create or replace view SUPPLY_GOLD.EXP_SCORE as
select e.group, date_trunc('week', d) wk,
       avg(otif_flag) otif, avg(lost_sale_flag) rupture_rate, avg(days_of_cover) doc
from SUPPLY_META.EXPERIMENT_STORES e
join SUPPLY_GOLD.OTIF_ORDERLINE o using(store_id)
join SUPPLY_GOLD.STOCKOUT_RISK r using(store_id, sku_id)
group by 1,2;

Operação & Custos

Warehouses: WH_ETL (M) p/ DT & treinamentos; WH_APP (S) p/ Streamlit/BI

Resource Monitor: limite diário para cada WH

Partition & Pruning: clusterizar fatos por (store_id, sku_id, d)

Caching & Result Scan: use warehouse caching para o app

Roadmap de implantação

Semana 1–2: ingestão, DIMs, métricas OTIF/ruptura, Streamlit v0 (apenas viz)

Semana 3–4: features + treino Snowpark; previsões diárias; anomalias

Semana 5: recomendações LLM (JSON) + aprovação humana + Alerts

Semana 6: RBAC, masking, row-access, testes automatizados e experimento A/B

Semana 7+: ajuste fino de modelo (hierarchical, sazonalidade avançada, cold-start), integração ERP/WMS

O que você pode executar já (checklist rápido)

 Criar schemas e tabelas DIM/FACT

 Popular DIM_DATE e carregar amostras (ou gerar sintético)

 Criar Dynamic Tables: FACT_SALES_DAILY, INVENTORY_POSITION, OTIF_ORDERLINE, STOCKOUT_DAILY

 Montar DEMAND_FEATURES

 Implementar PROC de treino e UDF de predição

 FORECAST_DAILY + ANOMALIES + STOCKOUT_RISK

 View RECO_CONTEXT + chamada cortex.complete (saída JSON)

 Streamlit básico (3 abas)

 RBAC, Masking, Row Policy, Tags

 Experimento e scoreboard

Se você quiser, eu adapto este blueprint ao seu domínio (número real de lojas/SKUs, janelas de previsão, políticas de entrega/lead time) e já te entrego:

um pacote .sql com todo o DDL/DT/Tasks,

a PROC/UDF Snowpark prontas,

a página Streamlit mínima funcional.