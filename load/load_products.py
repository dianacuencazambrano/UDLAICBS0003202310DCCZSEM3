from transform.transfomations import *
from util.functions import *

import pandas as pd
import traceback

def load_products(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
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
            "etl_id" : [],
        }

        product_dic_dim = product_dic

        product_tra = pd.read_sql(f"SELECT PROD_ID,PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        product_dim = pd.read_sql("SELECT PROD_ID,PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products", ses_db_sor)

        if not product_tra.empty:
            for id, nam, des, cat, cat_id, cat_des, wei, sup, sta, lis, min \
                in zip(product_tra['PROD_ID'],
                        product_tra['PROD_NAME'],
                        product_tra['PROD_DESC'],
                        product_tra['PROD_CATEGORY'],
                        product_tra['PROD_CATEGORY_ID'],
                        product_tra['PROD_CATEGORY_DESC'],
                        product_tra['PROD_WEIGHT_CLASS'],
                        product_tra['SUPPLIER_ID'],
                        product_tra['PROD_STATUS'],
                        product_tra['PROD_LIST_PRICE'],
                        product_tra['PROD_MIN_PRICE']): 

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
                        product_dic["prod_min_price"].append(min)
        
        if not product_dim.empty:
            for id, nam, des, cat, cat_id, cat_des, wei, sup, sta, lis, min \
                in zip(product_dim['PROD_ID'],
                        product_dim['PROD_NAME'],
                        product_dim['PROD_DESC'],
                        product_dim['PROD_CATEGORY'],
                        product_dim['PROD_CATEGORY_ID'],
                        product_dim['PROD_CATEGORY_DESC'],
                        product_dim['PROD_WEIGHT_CLASS'],
                        product_dim['SUPPLIER_ID'],
                        product_dim['PROD_STATUS'],
                        product_dim['PROD_LIST_PRICE'],
                        product_dim['PROD_MIN_PRICE']): 

                        product_dic_dim["prod_id"].append(id),
                        product_dic_dim["prod_name"].append(nam),
                        product_dic_dim["prod_desc"].append(des),
                        product_dic_dim["prod_category"].append(cat),
                        product_dic_dim["prod_category_id"].append(cat_id),
                        product_dic_dim["prod_category_desc"].append(cat_des),
                        product_dic_dim["prod_weight_class"].append(wei),
                        product_dic_dim["supplier_id"].append(sup),
                        product_dic_dim["prod_status"].append(sta),
                        product_dic_dim["prod_list_price"].append(lis),
                        product_dic_dim["prod_min_price"].append(min)

        if product_dic_dim["prod_id"]:
            resp = 'Product : Sucess' if (append(product_dic, product_dic_dim, 'products', ses_db_sor) == 1) else 'Product : Fail'
        else:
            resp = 'Product : Sucess' if (append(product_dic_dim, 'products', ses_db_sor) == 1) else 'Product : Fail'
            
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass