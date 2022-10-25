from settings import settings
from util.db_connection import Db_Connection
from transform.transfomations import *

import pandas as pd
import traceback

def load_person():
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_STG)
        con_db_sor = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_SOR)
        ses_db_stg = con_db_stg.start()
        ses_db_sor = con_db_sor.start()

        if ses_db_stg == -1 or ses_db_sor == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2 or ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        #path = "csvs/channels.csv"
        dim_per_dic = {
            "per_cod" : [],
            "per_nom_com" : [],
            "per_gen" : [],
            "per_fech_nac" : [],
        }

        per_tra = pd.read_sql("SELECT per_cod, per_nom_com, per_gen, per_fech_nac FROM persona_tra", ses_db_stg)
        #print(per_ext)

        #Procesar los archivos csv

        if not per_tra.empty:
            for cod, nom_com, gen, fec_nac \
                in zip(per_tra['per_cod'],
                        per_tra['per_nom_com'],
                        per_tra['per_gen'],
                        per_tra['per_fech_nac']): 

                        dim_per_dic["per_cod"].append(cod),
                        dim_per_dic["per_nom_com"].append(nom_com),
                        dim_per_dic["per_gen"].append(gen),
                        dim_per_dic["per_fech_nac"].append(fec_nac)
        if dim_per_dic["per_cod"]:
            df_dim_per = pd.DataFrame(dim_per_dic)
            df_dim_per.to_sql('dim_persona',
                                    ses_db_sor, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass