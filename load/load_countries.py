from transform.transfomations import *

import pandas as pd
import traceback

def load_countries(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        country_dic = {
            "country_id" : [],
            "country_name" : [],
            "country_region" : [],
            "country_region_id" : [],
            "etl_id" : []
        }

        country_tra = pd.read_sql("SELECT COUNTRY_ID,COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_tra WHERE ETL_ID = {etl_id}", ses_db_stg)

        if not country_tra.empty:
            for id, nam, reg, reg_id \
                in zip(country_tra['COUNTRY_ID'],
                        country_tra['COUNTRY_NAME'],
                        country_tra['COUNTRY_REGION'],
                        country_tra['COUNTRY_REGION_ID']): 

                        country_dic["country_id"].append(id),
                        country_dic["country_name"].append(nam),
                        country_dic["country_region"].append(reg),
                        country_dic["country_region_id"].append(reg_id),
                        country_dic["etl_id"].append(etl_id)
        if country_dic["channel_id"]:
            df_dim_cou = pd.DataFrame(country_dic)
            df_dim_cou.to_sql('countries',
                                    ses_db_sor, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass