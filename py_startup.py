from extract.extract_customers import ext_customers
from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times
from transform.tranform_customer import tra_customers
from util.db_connection import Db_Connection
from settings import settings

from util.extract_all import extract_all
from util.load_all import load_all
from util.transform_all import transform_all
from util.get_etl_id import get_etl_id

import traceback

try:
    con_db_stg = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_STG)
    con_db_sor = Db_Connection(settings.DB_TYPE, settings.DB_HOST, settings.DB_PORT, settings.DB_USER, settings.DB_PASSWORD, settings.DB_SOR)
    ses_db_stg = con_db_stg.start()
    ses_db_sor = con_db_sor.start()

    if ses_db_stg == -1 or ses_db_sor == -1:
        raise Exception(f"The given database type {type} is not valid")
    elif ses_db_stg == -2 or ses_db_stg == -2:
        raise Exception(f"Error trying to connect to the database")

    #etl process id
    # etl_id = get_etl_id(ses_db_stg)
    # etl_id = etl_id if etl_id !=0 else 'Error etl_id not found'
    # print("ETL Process ID: ", etl_id)

    # #extract all cvs files
    # if extract_all(ses_db_stg) == 1:
    #     print("Extraction complete")
    # else:
    #     print("Extraction failed")
    
    # #transform the data
    # if transform_all(etl_id, ses_db_stg) == 1:
    #     print("Transformation complete")
    # else:
    #     print("Transformation failed")

    # #load the data
    # if load_all(etl_id, ses_db_stg, ses_db_sor) == 1:
    #     print("Load complete")
    # else:
    #     print("Load failed")

    #load_times(24, ses_db_stg, ses_db_sor)
    load_sales(24, ses_db_stg, ses_db_sor)
    
except:
    traceback.print_exc()
finally:
    pass