from transform.transfomations import *

import pandas as pd
import traceback

def tra_sales(etl_id, ses_db_stg):
    try:
        ses_db_stg.connect().execute("TRUNCATE TABLE sales_tra")
        #Diccionario de los valores
        sale_tra_dic = {
            "prod_id" : [],
            "cust_id" : [],
            "time_id" : [],
            "channel_id" : [],
            "promo_id" : [],
            "quantity_sold" : [],
            "amount_sold" : [],
            "etl_id" : [],
        }

        sale_ext = pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_ext", ses_db_stg)

        if not sale_ext.empty:
            for pro_id, cus_id, tim_id, cha_id, prom_id, qua, amo \
                in zip(sale_ext['PROD_ID'],
                        sale_ext['CUST_ID'],
                        sale_ext['TIME_ID'],
                        sale_ext['CHANNEL_ID'],
                        sale_ext['PROMO_ID'],
                        sale_ext['QUANTITY_SOLD'],
                        sale_ext['AMOUNT_SOLD']): 

                        sale_tra_dic["prod_id"].append(str_2_int(pro_id)),
                        sale_tra_dic["cust_id"].append(str_2_int(cus_id)),
                        sale_tra_dic["time_id"].append(obt_date(tim_id)),
                        sale_tra_dic["channel_id"].append(str_2_int(cha_id)),
                        sale_tra_dic["promo_id"].append(str_2_int(prom_id)),
                        sale_tra_dic["quantity_sold"].append(str_2_float(qua)),
                        sale_tra_dic["amount_sold"].append(str_2_float(amo)),
                        sale_tra_dic["etl_id"].append(etl_id)
        if sale_tra_dic["prod_id"]:
            df_sale_tra = pd.DataFrame(sale_tra_dic)
            df_sale_tra.to_sql('sales_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass