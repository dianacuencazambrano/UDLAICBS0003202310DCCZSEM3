from transform.transfomations import *

import pandas as pd
import traceback

def tra_countries(etl_id, ses_db_stg):
    try:
        ses_db_stg.connect().execute("TRUNCATE TABLE countries_tra")
        #Diccionario de los valores
        country_tra_dic = {
            "country_id" : [],
            "country_name" : [],
            "country_region" : [],
            "country_region_id" : [],
            "etl_id" : []
        }

        countries_ext = pd.read_sql("SELECT COUNTRY_ID,COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_ext", ses_db_stg)

        if not countries_ext.empty:
            for id, nam, reg, reg_id \
                in zip(countries_ext['COUNTRY_ID'],
                        countries_ext['COUNTRY_NAME'],
                        countries_ext['COUNTRY_REGION'],
                        countries_ext['COUNTRY_REGION_ID']): 

                        country_tra_dic["country_id"].append(str_2_int(id)),
                        country_tra_dic["country_name"].append(nam),
                        country_tra_dic["country_region"].append(reg),
                        country_tra_dic["country_region_id"].append(str_2_int(reg_id)),
                        country_tra_dic["etl_id"].append(etl_id)
        if country_tra_dic["country_id"]:
            df_country_tra = pd.DataFrame(country_tra_dic)
            df_country_tra.to_sql('countries_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass