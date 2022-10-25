from settings import settings
from util.db_connection import Db_Connection
from transform.transfomations import *

import pandas as pd
import traceback

def tra_person():
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_STG)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        #path = "csvs/channels.csv"
        per_tra_dic = {
            "per_cod" : [],
            "per_nombres" : [],
            "per_apellidos" : [],
            "per_nom_com" : [],
            "per_genero" : [],
            "per_gen" : [],
            "per_fech_nac" : [],
        }

        per_ext = pd.read_sql("SELECT per_cod, per_nombres, per_apellidos, per_genero, per_fech_nac FROM persona_ext", ses_db_stg)
        #print(per_ext)

        #Procesar los archivos csv

        if not per_ext.empty:
            for cod, nom, ape, gen, fec_nac \
                in zip(per_ext['per_cod'],
                        per_ext['per_nombres'],
                        per_ext['per_apellidos'],
                        per_ext['per_genero'],
                        per_ext['per_fech_nac']): 

                        per_tra_dic["per_cod"].append(cod),
                        per_tra_dic["per_nombres"].append(nom),
                        per_tra_dic["per_apellidos"].append(ape),
                        per_tra_dic["per_nom_com"].append(join_2_strings(nom,ape)),
                        per_tra_dic["per_genero"].append(gen),
                        per_tra_dic["per_gen"].append(obt_gender(gen)),
                        per_tra_dic["per_fech_nac"].append(obt_date(fec_nac))
        if per_tra_dic["per_cod"]:
            df_per_tra = pd.DataFrame(per_tra_dic)
            df_per_tra.to_sql('persona_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass