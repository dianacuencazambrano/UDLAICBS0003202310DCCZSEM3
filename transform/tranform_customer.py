from transform.transfomations import *

import pandas as pd
import traceback

def tra_customers(etl_id, ses_db_stg):
    try:
        ses_db_stg.connect().execute("TRUNCATE TABLE customers_tra")
        #Diccionario de los valores
        customer_tra_dic = {
            "cust_id" : [],
            "cust_first_name" : [],
            "cust_last_name" : [],
            "cust_gender" : [],
            "cust_year_of_birth" : [],
            "cust_marital_status" : [],
            "cust_street_address" : [],
            "cust_postal_code" : [],
            "cust_city" : [],
            "cust_state_province" : [],
            "country_id" : [],
            "cust_main_phone_number" : [],
            "cust_income_level" : [],
            "cust_credit_limit" : [],
            "cust_email" : [],
            "etl_id" : [],
        }

        customer_ext = pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_ext", ses_db_stg)

        if not customer_ext.empty:
            for id, nam, las, gen, bir, sta, add, pos, cit, pro, cou, pho, inc, cre, ema \
                in zip(customer_ext['CUST_ID'],
                        customer_ext['CUST_FIRST_NAME'],
                        customer_ext['CUST_LAST_NAME'],
                        customer_ext['CUST_GENDER'],
                        customer_ext['CUST_YEAR_OF_BIRTH'],
                        customer_ext['CUST_MARITAL_STATUS'],
                        customer_ext['CUST_STREET_ADDRESS'],
                        customer_ext['CUST_POSTAL_CODE'],
                        customer_ext['CUST_CITY'],
                        customer_ext['CUST_STATE_PROVINCE'],
                        customer_ext['COUNTRY_ID'],
                        customer_ext['CUST_MAIN_PHONE_NUMBER'],
                        customer_ext['CUST_INCOME_LEVEL'],
                        customer_ext['CUST_CREDIT_LIMIT'],
                        customer_ext['CUST_EMAIL']): 

                        customer_tra_dic["cust_id"].append(str_2_int(id)),
                        customer_tra_dic["cust_first_name"].append(nam),
                        customer_tra_dic["cust_last_name"].append(las),
                        customer_tra_dic["cust_gender"].append(gen)
                        customer_tra_dic["cust_year_of_birth"].append(str_2_int(bir))
                        customer_tra_dic["cust_marital_status"].append(sta)
                        customer_tra_dic["cust_street_address"].append(add)
                        customer_tra_dic["cust_postal_code"].append(pos)
                        customer_tra_dic["cust_city"].append(cit)
                        customer_tra_dic["cust_state_province"].append(pro)
                        customer_tra_dic["country_id"].append(str_2_int(cou))
                        customer_tra_dic["cust_main_phone_number"].append(pho)
                        customer_tra_dic["cust_income_level"].append(inc)
                        customer_tra_dic["cust_credit_limit"].append(str_2_int(cre))
                        customer_tra_dic["cust_email"].append(ema),
                        customer_tra_dic["etl_id"].append(etl_id)
        if customer_tra_dic["cust_id"]:
            df_customer_tra = pd.DataFrame(customer_tra_dic)
            df_customer_tra.to_sql('customers_tra',
                                    ses_db_stg, 
                                    if_exists='append',
                                    index=False)
    except:
        traceback.print_exc()
    finally:
        pass