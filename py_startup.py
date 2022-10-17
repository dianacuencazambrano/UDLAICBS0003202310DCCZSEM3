from util.db_connection import Db_Connection
from datetime import datetime

import pandas as pd
import traceback

try:
    type = 'mysql'
    host = 'localhost'
    port = '3306'
    user = 'root'
    pwd = 'admin'
    db = 'dcczdbsor'

    con_db_stg = Db_Connection(type, host, port, user, pwd, db)
    ses_db_stg = con_db_stg.start()

    if ses_db_stg == -1:
        raise Exception(f"The given database type {type} is not valid")
    elif ses_db_stg == -2:
        raise Exception(f"Error trying to connect to the database")

    df = pd.read_sql('SELECT USER()', ses_db_stg)
    print(df)

except:
    traceback.print_exc()
finally:
    pass