from transform.transfomations import *
from util.functions import *

import pandas as pd
import traceback

def load_sales(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        sale_dic = {
            "prod_id" : [],
            "cust_id" : [],
            "time_id" : [],
            "channel_id" : [],
            "promo_id" : [],
            "quantity_sold" : [],
            "amount_sold" : [],
            "etl_id" : [],
        }

        sale_dic_dim = sale_dic

        sale_tra = pd.read_sql(f"SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        sale_dim = pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales", ses_db_sor)

        dic_product = relations('ID','PROD_ID','products', ses_db_sor)
        dic_customer = relations('ID','CUST_ID','customers', ses_db_sor)
        dic_time = relations('ID','TIME_ID','times', ses_db_sor)
        dic_channel = relations('ID','CHANNEL_ID','channels', ses_db_sor)
        dic_promotion = relations('ID','PROMO_ID','promotions', ses_db_sor)
        
        if not sale_tra.empty:
            for prod_id, cust_id, time_id, channel_id, promo_id, qua, amo \
                in zip(sale_tra['PROD_ID'],
                        sale_tra['CUST_ID'],
                        sale_tra['TIME_ID'],
                        sale_tra['CHANNEL_ID'],
                        sale_tra['PROMO_ID'],
                        sale_tra['QUANTITY_SOLD'],
                        sale_tra['AMOUNT_SOLD']): 

                        sale_dic["prod_id"].append(dic_product[prod_id]),
                        sale_dic["cust_id"].append(dic_customer[cust_id]),
                        sale_dic["time_id"].append(dic_time[time_id]),
                        sale_dic["channel_id"].append(dic_channel[channel_id]),
                        sale_dic["promo_id"].append(dic_promotion[promo_id]),
                        sale_dic["quantity_sold"].append(qua),
                        sale_dic["amount_sold"].append(amo),
        
        if not sale_dim.empty:
            for pro_id, cus_id, tim_id, cha_id, prom_id, qua, amo \
                in zip(sale_dim['PROD_ID'],
                        sale_dim['CUST_ID'],
                        sale_dim['TIME_ID'],
                        sale_dim['CHANNEL_ID'],
                        sale_dim['PROMO_ID'],
                        sale_dim['QUANTITY_SOLD'],
                        sale_dim['AMOUNT_SOLD']): 

                        sale_dic_dim["prod_id"].append(pro_id),
                        sale_dic_dim["cust_id"].append(cus_id),
                        sale_dic_dim["time_id"].append(tim_id),
                        sale_dic_dim["channel_id"].append(cha_id),
                        sale_dic_dim["promo_id"].append(prom_id),
                        sale_dic_dim["quantity_sold"].append(qua),
                        sale_dic_dim["amount_sold"].append(amo),

        if sale_dic_dim["prod_id"]:
            resp = 'Sale : Sucess' if (append(sale_dic, sale_dic_dim, 'sales', ses_db_sor) == 1) else 'Sale : Fail'
        else:
            resp = 'Sale : Sucess' if (append(sale_dic_dim, 'sales', ses_db_sor) == 1) else 'Sale : Fail'
            
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass