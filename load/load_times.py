from transform.transfomations import *
from util.functions import *

import pandas as pd
import traceback

def load_times(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        time_dic = {
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
        }

        time_dic_dim = time_dic

        time_tra = pd.read_sql(f"SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        time_dim = pd.read_sql("SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times", ses_db_sor)
        
        if not time_tra.empty:
            for id, nam, day_wee, day_mon, cal_wee, cal_mon, cal_mon_des, end_cal, cal_qua, cal_yea \
                in zip(time_tra['TIME_ID'],
                        time_tra['DAY_NAME'],
                        time_tra['DAY_NUMBER_IN_WEEK'],
                        time_tra['DAY_NUMBER_IN_MONTH'],
                        time_tra['CALENDAR_WEEK_NUMBER'],
                        time_tra['CALENDAR_MONTH_NUMBER'],
                        time_tra['CALENDAR_MONTH_DESC'],
                        time_tra['END_OF_CAL_MONTH'],
                        time_tra['CALENDAR_QUARTER_DESC'],
                        time_tra['CALENDAR_YEAR']): 

                        time_dic["time_id"].append(id),
                        time_dic["day_name"].append(nam),
                        time_dic["day_number_in_week"].append(day_wee),
                        time_dic["day_number_in_month"].append(day_mon),
                        time_dic["calendar_week_number"].append(cal_wee),
                        time_dic["calendar_month_number"].append(cal_mon),
                        time_dic["calendar_month_desc"].append(cal_mon_des),
                        time_dic["end_of_cal_month"].append(end_cal),
                        time_dic["calendar_quarter_desc"].append(cal_qua),
                        time_dic["calendar_year"].append(cal_yea)

            if time_dic_dim["time_id"]:
                resp = 'Time : Sucess' if (append(time_dic_dim, 'times', ses_db_sor) == 1) else 'Time : Fail'

        if not time_dim.empty:
            for id, nam, day_wee, day_mon, cal_wee, cal_mon, cal_mon_des, end_cal, cal_qua, cal_yea \
                in zip(time_dim['TIME_ID'],
                        time_dim['DAY_NAME'],
                        time_dim['DAY_NUMBER_IN_WEEK'],
                        time_dim['DAY_NUMBER_IN_MONTH'],
                        time_dim['CALENDAR_WEEK_NUMBER'],
                        time_dim['CALENDAR_MONTH_NUMBER'],
                        time_dim['CALENDAR_MONTH_DESC'],
                        time_dim['END_OF_CAL_MONTH'],
                        time_dim['CALENDAR_QUARTER_DESC'],
                        time_dim['CALENDAR_YEAR']): 

                        time_dic_dim["time_id"].append(id),
                        time_dic_dim["day_name"].append(nam),
                        time_dic_dim["day_number_in_week"].append(day_wee),
                        time_dic_dim["day_number_in_month"].append(day_mon),
                        time_dic_dim["calendar_week_number"].append(cal_wee),
                        time_dic_dim["calendar_month_number"].append(cal_mon),
                        time_dic_dim["calendar_month_desc"].append(cal_mon_des),
                        time_dic_dim["end_of_cal_month"].append(end_cal),
                        time_dic_dim["calendar_quarter_desc"].append(cal_qua),
                        time_dic_dim["calendar_year"].append(cal_yea)

            if time_dic_dim["time_id"]:
                resp = 'Time : Sucess' if (merge_tables(time_dic, time_dic_dim, 'times', ses_db_sor) == 1) else 'Time : Fail'
        
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass