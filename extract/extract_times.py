import pandas as pd
import traceback

def ext_times(ses_db_stg):
    try:
        #Diccionario de los valores de channels_ext
        path = "csvs/times.csv"
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
            "calendar_year" : []
        }

        time_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not time_csv.empty:
            for id, nam, day_wee, day_mon, cal_wee, cal_mon, cal_mon_des, end_cal, cal_qua, cal_yea \
                in zip(time_csv['TIME_ID'],
                        time_csv['DAY_NAME'],
                        time_csv['DAY_NUMBER_IN_WEEK'],
                        time_csv['DAY_NUMBER_IN_MONTH'],
                        time_csv['CALENDAR_WEEK_NUMBER'],
                        time_csv['CALENDAR_MONTH_NUMBER'],
                        time_csv['CALENDAR_MONTH_DESC'],
                        time_csv['END_OF_CAL_MONTH'],
                        time_csv['CALENDAR_QUARTER_DESC'],
                        time_csv['CALENDAR_YEAR']): 

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
            if time_dic["time_id"]:
                ses_db_stg.connect().execute("TRUNCATE TABLE times_ext")
                df_times_ext = pd.DataFrame(time_dic)
                df_times_ext.to_sql('times_ext',
                                        ses_db_stg, 
                                        if_exists='append',
                                        index=False)

    except:
        traceback.print_exc()
    finally:
        pass