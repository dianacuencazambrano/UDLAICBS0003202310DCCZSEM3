from filecmp import dircmp
import traceback

import pandas as pd

def merge_tables(tra_dic, dim_dic, dim_table, ses_db):
    try:
        tra_df = pd.DataFrame(tra_dic)
        dim_df = pd.DataFrame(dim_dic)
        mergedTables = tra_df.merge(dim_df, how='outer', indicator=True).query('queue == "left_only"').drop('queue', axis=1)
        mergedTables.to_sql(dim_table, ses_db, if_exists="append", index=False)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass

def append(dic, table, ses_db):
    try:
        df = pd.DataFrame(dic)
        df.to_sql(table, ses_db, if_exists="append", index=False)
        return 1
    except:
        traceback.print_exc()
        return 0
    finally:
        pass

def relations(surrogate_key, natural_key, table, ses_db):
    try:
        id = surrogate_key.lower()
        table_id = natural_key.lower()
        table_dim = pd.read_sql(f"SELECT {surrogate_key} , {natural_key} FROM {table}", ses_db)
        dic = dict()
        if not table_dim.empty:
            for id, table_id\
                in zip(
                    table_dim[id],
                    table_dim[table_id]):
                dic[table_id] = id
        return dic
    except:
        traceback.print_exc()
        return 0
    finally:
        pass
