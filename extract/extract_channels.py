import pandas as pd
import traceback

def ext_channels(ses_db_stg):
    try:
        #Diccionario de los valores
        path = "csvs/channels.csv"
        channel_dic = {
            "channel_id" : [],
            "channel_desc" : [],
            "channel_class" : [],
            "channel_class_id" : []
        }

        channel_csv = pd.read_csv(path)

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