from transform.transfomations import *

import pandas as pd
import traceback

def load_channels(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        channel_dic = {
            "channel_id" : [],
            "channel_desc" : [],
            "channel_class" : [],
            "channel_class_id" : [],
            "etl_id" : [],
        }

        channel_tra = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra WHERE ETL_ID = {etl_id}", ses_db_stg)

        if not channel_tra.empty:
            for id, des, cla, cla_id \
                in zip(channel_tra['CHANNEL_ID'],
                        channel_tra['CHANNEL_DESC'],
                        channel_tra['CHANNEL_CLASS'],
                        channel_tra['CHANNEL_CLASS_ID']): 

                        channel_dic["channel_id"].append(id),
                        channel_dic["channel_desc"].append(des),
                        channel_dic["channel_class"].append(cla),
                        channel_dic["channel_class_id"].append(cla_id),
                        channel_dic["etl_id"].append(etl_id)
        if channel_dic["channel_id"]:
            df_dim_cha = pd.DataFrame(channel_dic)
            df_dim_cha.to_sql('channels',
                                    ses_db_sor, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass