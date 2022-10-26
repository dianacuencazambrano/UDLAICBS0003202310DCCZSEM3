from transform.transfomations import *

import pandas as pd
import traceback

def tra_products(etl_id, ses_db_stg):
    try:
        ses_db_stg.connect().execute("TRUNCATE TABLE products_tra")
        #Diccionario de los valores
        product_tra_dic = {
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

        products_ext = pd.read_sql("SELECT PROD_ID,PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext", ses_db_stg)

        if not products_ext.empty:
            for id, nam, des, cat, cat_id, cat_des, wei, sup, sta, lis, min \
                in zip(products_ext['PROD_ID'],
                        products_ext['PROD_NAME'],
                        products_ext['PROD_DESC'],
                        products_ext['PROD_CATEGORY'],
                        products_ext['PROD_CATEGORY_ID'],
                        products_ext['PROD_CATEGORY_DESC'],
                        products_ext['PROD_WEIGHT_CLASS'],
                        products_ext['SUPPLIER_ID'],
                        products_ext['PROD_STATUS'],
                        products_ext['PROD_LIST_PRICE'],
                        products_ext['PROD_MIN_PRICE']): 

                        product_tra_dic["prod_id"].append(str_2_int(id)),
                        product_tra_dic["prod_name"].append(nam),
                        product_tra_dic["prod_desc"].append(des),
                        product_tra_dic["prod_category"].append(cat),
                        product_tra_dic["prod_category_id"].append(str_2_int(cat_id)),
                        product_tra_dic["prod_category_desc"].append(cat_des),
                        product_tra_dic["prod_weight_class"].append(wei),
                        product_tra_dic["supplier_id"].append(str_2_int(sup)),
                        product_tra_dic["prod_status"].append(sta),
                        product_tra_dic["prod_list_price"].append(str_2_float(lis)),
                        product_tra_dic["prod_min_price"].append(str_2_float(min)),
                        product_tra_dic["etl_id"].append(etl_id)
        if product_tra_dic["prod_id"]:
            df_product_tra = pd.DataFrame(product_tra_dic)
            df_product_tra.to_sql('products_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass