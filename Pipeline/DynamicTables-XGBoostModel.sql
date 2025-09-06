create or replace procedure SUPPLY_ML.TRAIN_XGB(MIN_HISTORY_DAYS INT)
returns string
language python
runtime_version = '3.10'
packages = ('pandas','numpy','xgboost','scikit-learn','cloudpickle','snowflake-snowpark-python')
handler = 'run'
as
$$
from snowflake.snowpark import Session
import pandas as pd, numpy as np, cloudpickle, os, tempfile
from xgboost import XGBRegressor

def run(session: Session, MIN_HISTORY_DAYS: int):
    try:
        df = session.table("SUPPLY_FEATURES.DEMAND_FEATURES")
        
        # Filtra séries com histórico mínimo
        from snowflake.snowpark.functions import count
        series = (df.group_by('STORE_ID','SKU_ID')
                   .agg(count('*').alias('N'))
                   .filter(f'N >= {MIN_HISTORY_DAYS}').collect())
        
        saved = 0
        errors = 0
        
        for r in series:
            try:
                sid, kid = r['STORE_ID'], r['SKU_ID']
                
                # Busca dados da série temporal
                data = (df.filter(f"STORE_ID='{sid}' AND SKU_ID='{kid}'")
                         .sort('D')
                         .to_pandas())
                
                if len(data) < MIN_HISTORY_DAYS:
                    continue
                
                # Prepara features (usando QTY_AVG7 conforme DemandFeature)
                y = data['QTY'].astype(float).values
                X = data[['QTY_LAG1','QTY_LAG7','QTY_AVG7','DOW','IS_HOLIDAY','PROMO_ACTIVE']].fillna(0).astype(float).values
                
                # Remove linhas com NaN no target
                valid_idx = ~np.isnan(y)
                X, y = X[valid_idx], y[valid_idx]
                
                if len(y) < MIN_HISTORY_DAYS:
                    continue
                
                # Treina modelo
                model = XGBRegressor(
                    max_depth=6, 
                    n_estimators=200, 
                    learning_rate=0.1,
                    random_state=42
                )
                model.fit(X, y)
                
                # Salva modelo na sua stage SUPPLY.ML.STAGE_MODELS
                model_filename = f"{sid}__{kid}.pkl"
                
                # Cria arquivo temporário
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.pkl') as tmp_file:
                    cloudpickle.dump(model, tmp_file)
                    tmp_path = tmp_file.name
                
                # Upload para sua stage
                session.file.put(tmp_path, f"@SUPPLY.ML.STAGE_MODELS/{model_filename}", overwrite=True)
                
                # Remove arquivo temporário
                os.unlink(tmp_path)
                
                # Atualiza registry
                session.sql(f"""
                    merge into SUPPLY_ML.MODEL_REGISTRY t
                    using (select '{sid}' as STORE_ID, '{kid}' as SKU_ID) s
                    on t.STORE_ID = s.STORE_ID and t.SKU_ID = s.SKU_ID
                    when matched then 
                        update set 
                            MODEL_URI = '@SUPPLY.ML.STAGE_MODELS/{model_filename}',
                            UPDATED_AT = current_timestamp()
                    when not matched then 
                        insert (STORE_ID, SKU_ID, MODEL_URI, CREATED_AT, UPDATED_AT)
                        values ('{sid}', '{kid}', '@SUPPLY.ML.STAGE_MODELS/{model_filename}', current_timestamp(), current_timestamp())
                """).collect()
                
                saved += 1
                
            except Exception as e:
                errors += 1
                continue
        
        return f"Treinamento concluído: {saved} modelos salvos, {errors} erros"
        
    except Exception as e:
        return f"Erro geral no treinamento: {str(e)}"
$$;