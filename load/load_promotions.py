from transform.transfomations import *

import pandas as pd
import traceback

def load_promotions(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        promotion_dic = {
            "promo_id" : [],
            "promo_name" : [],
            "promo_cost" : [],
            "promo_begin_date" : [],
            "promo_end_date" : [],
            "etl_id" : [],
        }

        promotion_tra = pd.read_sql("SELECT PROMO_ID,PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tra WHERE ETL_ID = {etl_id}", ses_db_stg)

        if not promotion_tra.empty:
            for id, nam, cos, beg, end \
                in zip(promotion_tra['PROMO_ID'],
                        promotion_tra['PROMO_NAME'],
                        promotion_tra['PROMO_COST'],
                        promotion_tra['PROMO_BEGIN_DATE'],
                        promotion_tra['PROMO_END_DATE']): 

                        promotion_dic["promo_id"].append(id),
                        promotion_dic["promo_name"].append(nam),
                        promotion_dic["promo_cost"].append(cos),
                        promotion_dic["promo_begin_date"].append(beg),
                        promotion_dic["promo_end_date"].append(end),
                        promotion_dic["etl_id"].append(etl_id)
        if promotion_dic["promo_id"]:
            df_dim_prom = pd.DataFrame(promotion_dic)
            df_dim_prom.to_sql('promotions',
                                    ses_db_sor, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass