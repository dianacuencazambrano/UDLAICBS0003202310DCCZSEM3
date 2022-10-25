from settings import settings
from util.db_connection import Db_Connection
from transform.transfomations import *

import pandas as pd
import traceback

def tra_channels(etl_id):
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_STG)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        channel_tra_dic = {
            "channel_id" : [],
            "channel_desc" : [],
            "channel_class" : [],
            "channel_class_id" : [],
            "etl_id" : [],
        }

        channel_ext = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext", ses_db_stg)

        if not channel_ext.empty:
            for id, des, cla, cla_id \
                in zip(channel_ext['CHANNEL_ID'],
                        channel_ext['CHANNEL_DESC'],
                        channel_ext['CHANNEL_CLASS'],
                        channel_ext['CHANNEL_CLASS_ID']): 

                        channel_tra_dic["channel_id"].append(str_2_int(id)),
                        channel_tra_dic["channel_desc"].append(des),
                        channel_tra_dic["channel_class"].append(cla),
                        channel_tra_dic["channel_class_id"].append(str_2_int(cla_id)),
                        channel_tra_dic["etl_id"].append(etl_id)
        if channel_tra_dic["channel_id"]:
            df_channel_tra = pd.DataFrame(channel_tra_dic)
            df_channel_tra.to_sql('channels_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass