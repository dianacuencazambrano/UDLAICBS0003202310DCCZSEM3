from settings import settings
from util.db_connection import Db_Connection

import pandas as pd
import traceback

def ext_sales():
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        path = "csvs/sales.csv"
        sales_dic = {
            "prod_id" : [],
            "cust_id" : [],
            "time_id" : [],
            "channel_id" : [],
            "promo_id" : [],
            "quantity_sold" : [],
            "amount_sold" : []
        }

        sales_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not sales_csv.empty:
            for pro_id, cus_id, tim_id,cha_id,prom_id,qua,amo \
                in zip(sales_csv['PROD_ID'],
                        sales_csv['CUST_ID'],
                        sales_csv['TIME_ID'],
                        sales_csv['CHANNEL_ID'],
                        sales_csv['PROMO_ID'],
                        sales_csv['QUANTITY_SOLD'],
                        sales_csv['AMOUNT_SOLD'],): 

                        sales_dic["prod_id"].append(pro_id),
                        sales_dic["cust_id"].append(cus_id),
                        sales_dic["time_id"].append(tim_id),
                        sales_dic["channel_id"].append(cha_id),
                        sales_dic["promo_id"].append(prom_id),
                        sales_dic["quantity_sold"].append(qua),
                        sales_dic["amount_sold"].append(amo),
            if sales_dic["prod_id"]:
                ses_db_stg.connect().execute("TRUNCATE TABLE sales_ext")
                df_sales_ext = pd.DataFrame(sales_dic)
                df_sales_ext.to_sql('sales_ext',
                                        ses_db_stg, 
                                        if_exists='append',
                                        index=False)

    except:
        traceback.print_exc()
    finally:
        pass