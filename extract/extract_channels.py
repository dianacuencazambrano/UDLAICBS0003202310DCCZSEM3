from settings import settings
from util.db_connection import Db_Connection

import pandas as pd
import traceback

def ext_channels():
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        path = "csvs/channels.csv"
        channel_dic = {
            "channel_id" : [],
            "channel_desc" : [],
            "channel_class" : [],
            "channel_class_id" : []
        }

        channel_csv = pd.read_csv(path)
        #print(channel_csv)

        #Procesar los archivos csv

        if not channel_csv.empty:
            for id, des, cla, cla_id \
                in zip(channel_csv['CHANNEL_ID'],
                        channel_csv['CHANNEL_DESC'],
                        channel_csv['CHANNEL_CLASS'],
                        channel_csv['CHANNEL_CLASS_ID']): 

                        channel_dic["channel_id"].append(id),
                        channel_dic["channel_desc"].append(des),
                        channel_dic["channel_class"].append(cla),
                        channel_dic["channel_class_id"].append(cla_id)
        if channel_dic["channel_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            df_channels_ext = pd.DataFrame(channel_dic)
            df_channels_ext.to_sql('channels_ext',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass