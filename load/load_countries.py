from transform.transfomations import *

import pandas as pd
import traceback

from util.functions import *

def load_countries(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        country_dic = {
            "country_id" : [],
            "country_name" : [],
            "country_region" : [],
            "country_region_id" : [],
        }

        country_dic_dim = country_dic

        country_tra = pd.read_sql(f"SELECT COUNTRY_ID,COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        country_dim = pd.read_sql("SELECT COUNTRY_ID,COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries", ses_db_sor)
        
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
            if country_dic_dim["country_id"]:
                resp = 'Country : Sucess' if (append(country_dic_dim, 'countries', ses_db_sor) == 1) else 'Country : Fail'
        
        if not country_dim.empty:
            for id, nam, reg, reg_id \
                in zip(country_dim['COUNTRY_ID'],
                        country_dim['COUNTRY_NAME'],
                        country_dim['COUNTRY_REGION'],
                        country_dim['COUNTRY_REGION_ID']): 

                        country_dic_dim["country_id"].append(id),
                        country_dic_dim["country_name"].append(nam),
                        country_dic_dim["country_region"].append(reg),
                        country_dic_dim["country_region_id"].append(reg_id),

            if country_dic_dim["country_id"]:
                resp = 'Country : Sucess' if (append(country_dic_dim, 'countries', ses_db_sor) == 1) else 'Country : Fail'
            
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass