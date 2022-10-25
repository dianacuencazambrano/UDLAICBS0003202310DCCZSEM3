from settings import settings
from util.db_connection import Db_Connection
from transform.transfomations import *

import pandas as pd
import traceback

def tra_times(etl_id):
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_STG)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        time_tra_dic = {
            "time_id" : [],
            "day_name" : [],
            "day_number_in_week" : [],
            "day_number_in_month" : [],
            "calendar_week_number" : [],
            "calendar_month_number" : [],
            "calendar_month_desc" : [],
            "end_of_cal_month" : [],
            "calendar_quarter_desc" : [],
            "calendar_year" : [],
            "etl_id" : [],
        }

        times_ext = pd.read_sql("SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_ext", ses_db_stg)

        if not times_ext.empty:
            for id, nam, day_wee, day_mon, cal_wee, cal_mon, cal_mon_des, end_cal, cal_qua, cal_yea \
                in zip(times_ext['TIME_ID'],
                        times_ext['DAY_NAME'],
                        times_ext['DAY_NUMBER_IN_WEEK'],
                        times_ext['DAY_NUMBER_IN_MONTH'],
                        times_ext['CALENDAR_WEEK_NUMBER'],
                        times_ext['CALENDAR_MONTH_NUMBER'],
                        times_ext['CALENDAR_MONTH_DESC'],
                        times_ext['END_OF_CAL_MONTH'],
                        times_ext['CALENDAR_QUARTER_DESC'],
                        times_ext['CALENDAR_YEAR']): 

                        time_tra_dic["time_id"].append(str_2_int(id)),
                        time_tra_dic["day_name"].append(nam),
                        time_tra_dic["day_number_in_week"].append(str_2_int(day_wee)),
                        time_tra_dic["day_number_in_month"].append(str_2_int(day_mon)),
                        time_tra_dic["calendar_week_number"].append(str_2_int(cal_wee)),
                        time_tra_dic["calendar_month_number"].append(str_2_int(cal_mon)),
                        time_tra_dic["calendar_month_desc"].append(cal_mon_des),
                        time_tra_dic["end_of_cal_month"].append(obt_date(end_cal)),
                        time_tra_dic["calendar_quarter_desc"].append(cal_qua),
                        time_tra_dic["calendar_year"].append(str_2_int(cal_yea)),
                        time_tra_dic["etl_id"].append(etl_id)
        if time_tra_dic["time_id"]:
            df_time_tra = pd.DataFrame(time_tra_dic)
            df_time_tra.to_sql('times_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass