import traceback

from settings import settings
from util.db_connection import Db_Connection

def get_etl_id(ses_db_stg):
    try:
        ses_db_stg.execute('INSERT INTO ETL_PROCESS VALUES ()')
        etl_id = ses_db_stg.execute('SELECT ID FROM ETL_PROCESS ORDER BY ID DESC LIMIT 1').scalar()
        return etl_id
    except:
        traceback.print_exc()
        return 0
    finally:
        pass