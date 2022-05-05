import logging
import os
from sqlalchemy import create_engine
import io
import numpy as np
import pandas as pd
import time
import tempfile
import pyarrow as pa

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


def get_features_conn():
    engine = create_engine(f"""postgresql://{os.environ['DB_FEATURE_USER']}:{os.environ['DB_FEATURE_PWD']}@{os.environ['DB_FEATURE_HOST']}:5432/{os.environ['DB_FEATURE_DB']}""")
    return engine


def check(db, schema='schema', table='t1'):
    if db == 'row':
        #engine = get_raw_conn()
        logging.info(f"working...")
        return
    else:
        engine = get_features_conn()
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}")
        cur.execute(f"select count(*) from {schema}.{table}")
        r = cur.fetchall()
        res = np.array(r)
        logging.info(f"total #: {res}")
    conn.close()


def copy_stringio(connection, arr, schema='lab', table='t1') -> None:
    t = time.perf_counter()
    with connection.cursor() as cursor:
        csv_file_like_object = io.StringIO()    
        for beer in arr:
            csv_file_like_object.write('|'.join(map(str, beer)) + '\n')
        csv_file_like_object.seek(0)            
        logging.info(f"生產temp csv\t{(time.perf_counter()-t):0.4f}")
        cursor.execute(f"SET search_path TO {schema}")            
        t = time.perf_counter()
        cursor.copy_from(csv_file_like_object, f'{table}', sep='|')
    connection.commit()
    logging.info(f"篩入資料\t{(time.perf_counter()-t):0.4f}")

def insert_rows(conn, fn,num=100000, schema='greed_island', table='t1'):
    # if num > 100000:
    #    raise 'too much'
    t = time.perf_counter()
    arr = np.random.randint(40, size=(num, 8))
    logging.info(f"生產資料\t{(time.perf_counter()-t):0.4f}s")
    logging.info(arr.shape)
    #with conn.cursor() as cur:
    #    cur.execute(f"TRUNCATE TABLE {schema}.{table}")
    #conn.commit()
    #copy_stringio(conn, arr, schema=schema, table=table)
    fn(conn, arr, schema=schema, table=table)
    end_t = time.perf_counter()
    total_t = end_t - t
    logging.info(f"insert time:\t{total_t:0.4f}")


if __name__ == '__main__':
    engine = get_features_conn()
    conn = engine.raw_connection()
    for _ in range(10):
        insert_rows(conn, copy_stringio,num=1000000,schema='greed_island', table='t1')
    #insert_rows(conn, copy_pa, schema='greed_island', table='t1')
    conn.close()
    check('feature', 'greed_island', 't1')
