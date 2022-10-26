from transform.tranform_channels import tra_channels
from transform.tranform_countries import tra_countries
from transform.tranform_customer import tra_customers
from transform.tranform_products import tra_products
from transform.tranform_promotions import tra_promotions
from transform.tranform_sales import tra_sales
from transform.tranform_times import tra_times

import traceback

def transform_all(etl_id, ses_db_stg):
    try:
        tra_channels(etl_id, ses_db_stg)
        tra_countries(etl_id, ses_db_stg)
        tra_customers(etl_id, ses_db_stg)
        tra_products(etl_id, ses_db_stg)
        tra_promotions(etl_id, ses_db_stg)
        tra_sales(etl_id, ses_db_stg)
        tra_times(etl_id, ses_db_stg)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass