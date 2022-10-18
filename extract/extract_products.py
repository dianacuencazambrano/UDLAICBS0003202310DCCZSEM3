from settings import settings
from util.db_connection import Db_Connection

import pandas as pd
import traceback

def ext_products():
    try:
        con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The given database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception(f"Error trying to connect to the database")

        #Diccionario de los valores
        path = "csvs/products.csv"
        product_dic = {
            "prod_id" : [],
            "prod_name" : [],
            "prod_desc" : [],
            "prod_category" : [],
            "prod_category_id" : [],
            "prod_category_desc" : [],
            "prod_weight_class" : [],
            "supplier_id" : [],
            "prod_status" : [],
            "prod_list_price" : [],
            "prod_min_price" : [],
        }

        product_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not product_csv.empty:
            for id, nam, des, cat, cat_id, cat_des, wei, sup, sta, lis, min \
                in zip(product_csv['PROD_ID'],
                        product_csv['PROD_NAME'],
                        product_csv['PROD_DESC'],
                        product_csv['PROD_CATEGORY'],
                        product_csv['PROD_CATEGORY_ID'],
                        product_csv['PROD_CATEGORY_DESC'],
                        product_csv['PROD_WEIGHT_CLASS'],
                        product_csv['SUPPLIER_ID'],
                        product_csv['PROD_STATUS'],
                        product_csv['PROD_LIST_PRICE'],
                        product_csv['PROD_MIN_PRICE'],): 

                        product_dic["prod_id"].append(id),
                        product_dic["prod_name"].append(nam),
                        product_dic["prod_desc"].append(des),
                        product_dic["prod_category"].append(cat),
                        product_dic["prod_category_id"].append(cat_id),
                        product_dic["prod_category_desc"].append(cat_des),
                        product_dic["prod_weight_class"].append(wei),
                        product_dic["supplier_id"].append(sup),
                        product_dic["prod_status"].append(sta),
                        product_dic["prod_list_price"].append(lis),
                        product_dic["prod_min_price"].append(min),
            if product_dic["prod_id"]:
                ses_db_stg.connect().execute("TRUNCATE TABLE products_ext")
                df_products_ext = pd.DataFrame(product_dic)
                df_products_ext.to_sql('products_ext',
                                        ses_db_stg, 
                                        if_exists='append',
                                        index=False)

    except:
        traceback.print_exc()
    finally:
        pass