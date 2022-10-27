from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times

import traceback

def load_all(etl_id, ses_db_stg, ses_db_sor):
    try:
        load_channels(etl_id, ses_db_stg, ses_db_sor)
        load_countries(etl_id, ses_db_stg, ses_db_sor)
        load_customers(etl_id, ses_db_stg, ses_db_sor)
        load_products(etl_id, ses_db_stg, ses_db_sor)
        load_promotions(etl_id, ses_db_stg, ses_db_sor)
        load_times(etl_id, ses_db_stg, ses_db_sor)
        load_sales(etl_id, ses_db_stg, ses_db_sor)
        
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass