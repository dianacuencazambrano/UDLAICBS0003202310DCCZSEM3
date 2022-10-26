from transform.transfomations import *

import pandas as pd
import traceback

def tra_promotions(etl_id, ses_db_stg):
    try:
        ses_db_stg.connect().execute("TRUNCATE TABLE promotions_tra")
        #Diccionario de los valores
        promotion_tra_dic = {
            "promo_id" : [],
            "promo_name" : [],
            "promo_cost" : [],
            "promo_begin_date" : [],
            "promo_end_date" : [],
            "etl_id" : [],
        }

        promotion_ext = pd.read_sql("SELECT PROMO_ID,PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext", ses_db_stg)

        if not promotion_ext.empty:
            for id, nam, cos, beg, end \
                in zip(promotion_ext['PROMO_ID'],
                        promotion_ext['PROMO_NAME'],
                        promotion_ext['PROMO_COST'],
                        promotion_ext['PROMO_BEGIN_DATE'],
                        promotion_ext['PROMO_END_DATE']): 

                        promotion_tra_dic["promo_id"].append(str_2_int(id)),
                        promotion_tra_dic["promo_name"].append(nam),
                        promotion_tra_dic["promo_cost"].append(str_2_float(cos)),
                        promotion_tra_dic["promo_begin_date"].append(obt_date(beg)),
                        promotion_tra_dic["promo_end_date"].append(obt_date(end)),
                        promotion_tra_dic["etl_id"].append(etl_id)
        if promotion_tra_dic["promo_id"]:
            df_promotion_tra = pd.DataFrame(promotion_tra_dic)
            df_promotion_tra.to_sql('promotions_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass