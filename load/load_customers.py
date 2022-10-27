from transform.transfomations import *
from util.functions import *

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
        }

        customer_dic_dim = customer_dic

        customer_tra = pd.read_sql(f"SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_tra WHERE ETL_ID = {etl_id}", ses_db_stg)
        customer_dim = pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers", ses_db_sor)
        
        dic_country = relations('ID', 'COUNTRY_ID', 'countries', ses_db_sor)
        
        if not customer_tra.empty:
            for id, nam, las, gen, bir, sta, add, pos, cit, pro, country_id, pho, inc, cre, ema \
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
                        customer_dic["country_id"].append(dic_country[country_id])
                        customer_dic["cust_main_phone_number"].append(pho)
                        customer_dic["cust_income_level"].append(inc)
                        customer_dic["cust_credit_limit"].append(cre)
                        customer_dic["cust_email"].append(ema),

            if customer_dic_dim["cust_id"]:
                resp = 'Customer : Sucess' if (append(customer_dic_dim, 'customers', ses_db_sor) == 1) else 'Customer : Fail'
        if not customer_dim.empty:
            for id, nam, las, gen, bir, sta, add, pos, cit, pro, cou, pho, inc, cre, ema \
                in zip(customer_dim['CUST_ID'],
                        customer_dim['CUST_FIRST_NAME'],
                        customer_dim['CUST_LAST_NAME'],
                        customer_dim['CUST_GENDER'],
                        customer_dim['CUST_YEAR_OF_BIRTH'],
                        customer_dim['CUST_MARITAL_STATUS'],
                        customer_dim['CUST_STREET_ADDRESS'],
                        customer_dim['CUST_POSTAL_CODE'],
                        customer_dim['CUST_CITY'],
                        customer_dim['CUST_STATE_PROVINCE'],
                        customer_dim['COUNTRY_ID'],
                        customer_dim['CUST_MAIN_PHONE_NUMBER'],
                        customer_dim['CUST_INCOME_LEVEL'],
                        customer_dim['CUST_CREDIT_LIMIT'],
                        customer_dim['CUST_EMAIL']): 

                        customer_dic_dim["cust_id"].append(id),
                        customer_dic_dim["cust_first_name"].append(nam),
                        customer_dic_dim["cust_last_name"].append(las),
                        customer_dic_dim["cust_gender"].append(gen)
                        customer_dic_dim["cust_year_of_birth"].append(bir)
                        customer_dic_dim["cust_marital_status"].append(sta)
                        customer_dic_dim["cust_street_address"].append(add)
                        customer_dic_dim["cust_postal_code"].append(pos)
                        customer_dic_dim["cust_city"].append(cit)
                        customer_dic_dim["cust_state_province"].append(pro)
                        customer_dic_dim["country_id"].append(cou)
                        customer_dic_dim["cust_main_phone_number"].append(pho)
                        customer_dic_dim["cust_income_level"].append(inc)
                        customer_dic_dim["cust_credit_limit"].append(cre)
                        customer_dic_dim["cust_email"].append(ema),

            if customer_dic_dim["cust_id"]:
                resp = 'Customer : Sucess' if (merge_tables(customer_dic, customer_dic_dim, 'customers', ses_db_sor) == 1) else 'Customer : Fail'
            
        print(resp)

    except:
        traceback.print_exc()
    finally:
        pass