from transform.transfomations import *

import pandas as pd
import traceback

def load_customers(etl_id, ses_db_stg, ses_db_sor):
    try:
        #Diccionario de los valores
        customer_dic = {
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

        customer_tra = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra WHERE ETL_ID = {etl_id}", ses_db_stg)

        if not customer_tra.empty:
            for id, nam, las, gen, bir, sta, add, pos, cit, pro, cou, pho, inc, cre, ema \
                in zip(customer_tra['CUST_ID'],
                        customer_tra['CUST_FIRST_NAME'],
                        customer_tra['CUST_LAST_NAME'],
                        customer_tra['CUST_GENDER'],
                        customer_tra['CUST_YEAR_OF_BIRTH'],
                        customer_tra['CUST_MARITAL_STATUS'],
                        customer_tra['CUST_STREET_ADDRESS'],
                        customer_tra['CUST_POSTAL_CODE'],
                        customer_tra['CUST_CITY'],
                        customer_tra['CUST_STATE_PROVINCE'],
                        customer_tra['COUNTRY_ID'],
                        customer_tra['CUST_MAIN_PHONE_NUMBER'],
                        customer_tra['CUST_INCOME_LEVEL'],
                        customer_tra['CUST_CREDIT_LIMIT'],
                        customer_tra['CUST_EMAIL']): 

                        customer_dic["cust_id"].append(id),
                        customer_dic["cust_first_name"].append(nam),
                        customer_dic["cust_last_name"].append(las),
                        customer_dic["cust_gender"].append(gen)
                        customer_dic["cust_year_of_birth"].append(bir)
                        customer_dic["cust_marital_status"].append(sta)
                        customer_dic["cust_street_address"].append(add)
                        customer_dic["cust_postal_code"].append(pos)
                        customer_dic["cust_city"].append(cit)
                        customer_dic["cust_state_province"].append(pro)
                        customer_dic["country_id"].append(cou)
                        customer_dic["cust_main_phone_number"].append(pho)
                        customer_dic["cust_income_level"].append(inc)
                        customer_dic["cust_credit_limit"].append(cre)
                        customer_dic["cust_email"].append(ema),
                        customer_dic["etl_id"].append(etl_id)
        if customer_dic["cust_id"]:
            df_dim_cus = pd.DataFrame(customer_dic)
            df_dim_cus.to_sql('customers',
                                    ses_db_sor, 
                                    if_exists='append',
                                    index=False)

    except:
        traceback.print_exc()
    finally:
        pass