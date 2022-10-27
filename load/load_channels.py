from util.functions import *
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
        }

        channel_dic_dim = channel_dic

        channel_tra = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        channel_dim = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels", ses_db_sor)

        if not channel_tra.empty:
            for id, des, cla, cla_id \
                in zip(channel_tra['CHANNEL_ID'],
                        channel_tra['CHANNEL_DESC'],
                        channel_tra['CHANNEL_CLASS'],
                        channel_tra['CHANNEL_CLASS_ID']): 

                        channel_dic["channel_id"].append(id),
                        channel_dic["channel_desc"].append(des),
                        channel_dic["channel_class"].append(cla),
                        channel_dic["channel_class_id"].append(cla_id)
            if channel_dic_dim["channel_id"]:
                resp = 'Channel : Sucess' if (append(channel_dic_dim, 'channels', ses_db_sor) == 1) else 'Channel : Fail'
        if not channel_dim.empty:
            for id, des, cla, cla_id \
                in zip(channel_dim['CHANNEL_ID'],
                        channel_dim['CHANNEL_DESC'],
                        channel_dim['CHANNEL_CLASS'],
                        channel_dim['CHANNEL_CLASS_ID']): 

                        channel_dic_dim["channel_id"].append(id),
                        channel_dic_dim["channel_desc"].append(des),
                        channel_dic_dim["channel_class"].append(cla),
                        channel_dic_dim["channel_class_id"].append(cla_id),

            if channel_dic_dim["channel_id"]:
                resp = 'Channel : Sucess' if (merge_tables(channel_dic, channel_dic_dim, 'channels', ses_db_sor) == 1) else 'Channel : Fail'
        
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass