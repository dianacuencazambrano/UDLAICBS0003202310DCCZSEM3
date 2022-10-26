from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times

import traceback

def extract_all(ses_db_stg):
    try:
        ext_channels(ses_db_stg)
        ext_countries(ses_db_stg)
        ext_customers(ses_db_stg)
        ext_products(ses_db_stg)
        ext_promotions(ses_db_stg)
        ext_sales(ses_db_stg)
        ext_times(ses_db_stg)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass