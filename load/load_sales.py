from transform.transfomations import *

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

        sale_tra = pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_tra WHERE ETL_ID = {etl_id}", ses_db_stg)

        if not sale_tra.empty:
            for pro_id, cus_id, tim_id, cha_id, prom_id, qua, amo \
                in zip(sale_tra['PROD_ID'],
                        sale_tra['CUST_ID'],
                        sale_tra['TIME_ID'],
                        sale_tra['CHANNEL_ID'],
                        sale_tra['PROMO_ID'],
                        sale_tra['QUANTITY_SOLD'],
                        sale_tra['AMOUNT_SOLD']): 

                        sale_dic["prod_id"].append(pro_id),
                        sale_dic["cust_id"].append(cus_id),
                        sale_dic["time_id"].append(tim_id),
                        sale_dic["channel_id"].append(cha_id),
                        sale_dic["promo_id"].append(prom_id),
                        sale_dic["quantity_sold"].append(qua),
                        sale_dic["amount_sold"].append(amo),
                        sale_dic["etl_id"].append(etl_id)
        if sale_dic["prod_id"]:
            df_dim_sale = pd.DataFrame(sale_dic)
            df_dim_sale.to_sql('sales',
                                    ses_db_sor, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass