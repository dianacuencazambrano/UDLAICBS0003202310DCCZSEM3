from transform.transfomations import *
from util.functions import *

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
        }

        promotion_dic_dim = promotion_dic

        promotion_tra = pd.read_sql(f"SELECT PROMO_ID,PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        promotion_dim = pd.read_sql("SELECT PROMO_ID,PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions", ses_db_sor)
        
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
                        promotion_dic["promo_end_date"].append(end)
            if promotion_dic_dim["promo_id"]:
                resp = 'Promotion : Sucess' if (append(promotion_dic_dim, 'promotions', ses_db_sor) == 1) else 'Promotion : Fail'

        if not promotion_dim.empty:
            for id, nam, cos, beg, end \
                in zip(promotion_dim['PROMO_ID'],
                        promotion_dim['PROMO_NAME'],
                        promotion_dim['PROMO_COST'],
                        promotion_dim['PROMO_BEGIN_DATE'],
                        promotion_dim['PROMO_END_DATE']): 

                        promotion_dic_dim["promo_id"].append(id),
                        promotion_dic_dim["promo_name"].append(nam),
                        promotion_dic_dim["promo_cost"].append(cos),
                        promotion_dic_dim["promo_begin_date"].append(beg),
                        promotion_dic_dim["promo_end_date"].append(end),

            if promotion_dic_dim["promo_id"]:
                resp = 'Promotion : Sucess' if (merge_tables(promotion_dic, promotion_dic_dim, 'promotions', ses_db_sor) == 1) else 'Promotion : Fail'
            
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass