from settings import settings
from util.db_connection import Db_Connection

import pandas as pd
import traceback

def ext_promotions():
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_STG)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        path = "csvs/promotions.csv"
        promotion_dic = {
            "promo_id" : [],
            "promo_name" : [],
            "promo_cost" : [],
            "promo_begin_date" : [],
            "promo_end_date" : []
        }

        promotion_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not promotion_csv.empty:
            for id, nam, cos, beg, end \
                in zip(promotion_csv['PROMO_ID'],
                        promotion_csv['PROMO_NAME'],
                        promotion_csv['PROMO_COST'],
                        promotion_csv['PROMO_BEGIN_DATE'],
                        promotion_csv['PROMO_END_DATE']): 

                        promotion_dic["promo_id"].append(id),
                        promotion_dic["promo_name"].append(nam),
                        promotion_dic["promo_cost"].append(cos),
                        promotion_dic["promo_begin_date"].append(beg),
                        promotion_dic["promo_end_date"].append(end)
            if promotion_dic["promo_id"]:
                ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
                df_promotions_ext = pd.DataFrame(promotion_dic)
                df_promotions_ext.to_sql('promotions_ext',
                                        ses_db_stg, 
                                        if_exists='append',
                                        index=False)

    except:
        traceback.print_exc()
    finally:
        pass