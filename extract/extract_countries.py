import pandas as pd
import traceback

def ext_countries(ses_db_stg):
    try:
        #Diccionario de los valores
        path = "csvs/countries.csv"
        country_dic = {
            "country_id" : [],
            "country_name" : [],
            "country_region" : [],
            "country_region_id" : []
        }

        country_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not country_csv.empty:
            for id, nam, reg, reg_id \
                in zip(country_csv['COUNTRY_ID'],
                        country_csv['COUNTRY_NAME'],
                        country_csv['COUNTRY_REGION'],
                        country_csv['COUNTRY_REGION_ID']): 

                        country_dic["country_id"].append(id),
                        country_dic["country_name"].append(nam),
                        country_dic["country_region"].append(reg),
                        country_dic["country_region_id"].append(reg_id)
            if country_dic["country_id"]:
                ses_db_stg.connect().execute("TRUNCATE TABLE countries_ext")
                df_countries_ext = pd.DataFrame(country_dic)
                df_countries_ext.to_sql('countries_ext',
                                        ses_db_stg, 
                                        if_exists='append',
                                        index=False)

    except:
        traceback.print_exc()
    finally:
        pass