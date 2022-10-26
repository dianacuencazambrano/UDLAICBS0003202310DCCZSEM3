import pandas as pd
import traceback

def ext_customers(ses_db_stg):
    try:
         #Diccionario de los valores
        path = "csvs/customers.csv"
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
            "cust_email" : []
        }

        customer_csv = pd.read_csv(path)

        #Procesar los archivos csv

        if not customer_csv.empty:
            for id, nam, las, gen, bir, sta, add, pos, cit, pro, cou, pho, inc, cre, ema \
                in zip(customer_csv['CUST_ID'],
                        customer_csv['CUST_FIRST_NAME'],
                        customer_csv['CUST_LAST_NAME'],
                        customer_csv['CUST_GENDER'],
                        customer_csv['CUST_YEAR_OF_BIRTH'],
                        customer_csv['CUST_MARITAL_STATUS'],
                        customer_csv['CUST_STREET_ADDRESS'],
                        customer_csv['CUST_POSTAL_CODE'],
                        customer_csv['CUST_CITY'],
                        customer_csv['CUST_STATE_PROVINCE'],
                        customer_csv['COUNTRY_ID'],
                        customer_csv['CUST_MAIN_PHONE_NUMBER'],
                        customer_csv['CUST_INCOME_LEVEL'],
                        customer_csv['CUST_CREDIT_LIMIT'],
                        customer_csv['CUST_EMAIL']
                        ): 

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
                        customer_dic["cust_email"].append(ema)
            if customer_dic["cust_id"]:
                ses_db_stg.connect().execute("TRUNCATE TABLE customers_ext")
                df_customers_ext = pd.DataFrame(customer_dic)
                df_customers_ext.to_sql('customers_ext',
                                        ses_db_stg, 
                                        if_exists='append',
                                        index=False)

    except:
        traceback.print_exc()
    finally:
        pass