
import logging
from psycopg2 import connect
import psycopg2
import psycopg
import asyncpg
import datetime
import os
import time
from functools import wraps
from memory_profiler import memory_usage
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import io
import tempfile
from memory_tempfile import MemoryTempfile


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        logging.info(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        logging.info(f'Time   {elapsed:0.4}')

        # Measure memory
        mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        logging.info(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner


def profile_lighter(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        logging.info(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        logging.info(f'Time   {elapsed:0.4}')

        # Measure memory
        #mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        #logging.info(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


memfile = MemoryTempfile()


import multiprocessing


def process1_send_function(queue, query):
    db_source = f"""dbname={rawdata_dic['database']} user={rawdata_dic['user']} password={rawdata_dic['password']} host={rawdata_dic['host']}"""
    with psycopg.connect(db_source) as conn1:
        with conn1.cursor().copy(f"""COPY ({query}) TO STDOUT (FORMAT BINARY)""") as copy1:
            data = bytes(copy1.read())
            while data:
                # queue.put(bytes(data))
                queue.send(bytes(data))
                data = bytes(copy1.read())
    # queue.put('end')
    queue.send('end')
    logging.info(f"Event Sent: end")


def process2_recv_function(queue, schema, output_table):
    db_destination = f"""dbname={feature_dic['database']} user={feature_dic['user']} password={feature_dic['password']} host={feature_dic['host']}"""
    with psycopg.connect(db_destination) as conn2:
        with conn2.cursor() as truncate_cur:
            truncate_cur.execute(f"""TRUNCATE TABLE {schema}.{output_table}""")
        with conn2.cursor().copy(f"COPY {schema}.{output_table} FROM STDIN (FORMAT BINARY)") as copy2:
            while True:
                #data = queue.get()
                data = queue.recv()
                #logging.info(f"Event Received: {len(data)}")
                if data == 'end':
                    logging.info(f"Event Received: end")
                    return
                copy2.write(data)


# @profile
def read_and_insert_sql_tmpfile10(query, schema='greed_island', output_table='cdtx0001_6m'):
    t = time.perf_counter()
    logging.info(f"Event Received: start")
    #queue = multiprocessing.Queue()
    conn1, conn2 = multiprocessing.Pipe()
    process_1 = multiprocessing.Process(target=process1_send_function, args=(conn1, query))
    process_2 = multiprocessing.Process(target=process2_recv_function, args=(conn2, schema, output_table))
    process_2.start()
    process_1.start()
    process_2.join()
    process_1.join()
    elapsed = time.perf_counter() - t
    logging.info(f"""{elapsed:0.4}""")


# @profile
def read_and_insert_sql_tmpfile9(query, schema='greed_island', output_table='cdtx0001_6m'):
    t = time.perf_counter()
    logging.info(f"Event Received: start")
    queue = multiprocessing.Queue()
    process_1 = multiprocessing.Process(target=process1_send_function, args=(queue, query))
    process_2 = multiprocessing.Process(target=process2_recv_function, args=(queue, schema, output_table))
    process_2.start()
    process_1.start()
    process_2.join()
    process_1.join()
    elapsed = time.perf_counter() - t
    logging.info(f"""{elapsed:0.4}""")


def try_sql10(from_dt='2019-01-01', to_dt='2019-01-31', target_dt='2021-05-20'):
    sql = f"""    SELECT 
            t2.cust_no, t2.data_ym, t2.tcode, t2.objam, t2.mcc, t2.stonc, t2.scity, t2.adw_category
        FROM (
        SELECT
            id AS cust_no,
            data_dt AS selection_dt
        FROM mlaas_rawdata.user_population 
        WHERE data_dt = '{target_dt}'
        ) AS t1 
        INNER JOIN (
            SELECT 
                bacno AS cust_no, 
                dtadt AS data_ym,
                tcode,
                objam,
                mcc, 
                stonc,
                scity,
                mcc AS adw_category
            FROM cdtx0001_hist 
            WHERE (dtadt BETWEEN '{from_dt}' AND '{to_dt}') 
                AND ((tcode = '05' OR tcode = '25') OR (tcode = '08' OR tcode = '28')) 
                AND (MCC != '6010' AND MCC != '6011')) AS t2
        ON t1.cust_no = t2.cust_no"""
    query2_9(sql)


def check(schema='greed_island', output_table='cdtx0001_6m'):
    feature_engine = create_engine(f"postgresql://{feature_dic['user']}:{feature_dic['password']}@{feature_dic['host']}/{feature_dic['database']}")
    conn = feature_engine.raw_connection()
    with conn.cursor() as cur:
        cur.execute(f'select count(*) from {schema}.{output_table}')
        r = cur.fetchall()
        data = np.array(r)
        print(data)
    conn.close()


def query2_9(sql, schema='greed_island', output_table='cdtx0001_6m'):
    # read and write
    # read_and_insert_sql_tmpfile5(sql,schema='greed_island',output_table='cdtx0001_6m') # direct copy
    # check()
    # await read_and_insert_sql_tmpfile6(sql,schema='greed_island',output_table='cdtx0001_6m') # direct copy
    # check()
    # read_and_insert_sql_tmpfile4(sql,schema='greed_island',output_table='cdtx0001_6m') # memoryfile
    # check()
    # read_and_insert_sql_tmpfile3(sql,schema='greed_island',output_table='cdtx0001_6m') # copy binary
    # check()
    # read_and_insert_sql_tmpfile2(sql,schema='greed_island',output_table='cdtx0001_6m') # copy csv
    # check()
    read_and_insert_sql_tmpfile10(sql, schema='greed_island', output_table='cdtx0001_6m')  # copy csv
    check()
    # read
    # read_fetchall(sql)
    # df_read_sql(sql) # -- bekilled
    # check()
    # read_sql_tmpfile(sql)
    # check()
    # read_sql_inmem_uncompressed(sql)
    # check()


import asyncio


if __name__ == "__main__":
    #to_dt = '2019-03-31'
    to_dt = '2019-06-30'
    #to_dt = '2019-01-01'
    #to_dt = '2019-02-28'
    # try_sql2(to_dt=to_dt)
    #df = try_sql(to_dt=to_dt)
    #arr = try_sql3(to_dt=to_dt)
    # try_sql4(to_dt=to_dt)
    # try_sql5(to_dt=to_dt)
    # try_sql6(to_dt=to_dt)
    # try_sql7(to_dt=to_dt)
    # try_sql8(to_dt=to_dt)
    # try_sql9(to_dt=to_dt)
    # try_sql10(to_dt=to_dt,target_dt='2021-09-03')
    # try_sql10(to_dt=to_dt,target_dt='2021-06-30')
    #loop = asyncio.get_event_loop()
    # loop.run_until_complete(try_sql11(to_dt=to_dt,target_dt='2021-12-01'))
    # loop.run_until_complete(try_sql11(to_dt=to_dt,target_dt='2020-12-01'))
# await try_sql10(to_dt=to_dt,target_dt='2021-12-01')
    # try_sql10(to_dt=to_dt,target_dt='2020-12-01')
    # try_sql10(to_dt=to_dt,target_dt='2021-05-20')
    # try_sql10(to_dt=to_dt,target_dt='2021-12-01')
    # try_sql10(to_dt=to_dt,target_dt='2021-12-01')
    try_sql10(to_dt=to_dt, target_dt='2021-06-30')
