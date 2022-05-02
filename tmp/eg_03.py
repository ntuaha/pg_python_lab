
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


import pickle
from pyarrow import csv
@profile_lighter
def read_sql_tmpfile_arrow_to_pandas(query):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
    #with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)        
        conn.close()
        tmpfile.seek(0)
        #df = csv.read_csv(tmpfile.name).to_pandas()
        t = time.perf_counter()
        df = csv.read_csv(tmpfile.name).to_pandas()
        #df = pd.read_csv(tmpfile,engine="pyarrow")
        logging.info(df.shape)
        elapsed = time.perf_counter() - t
        logging.info(f"""{elapsed:0.4}""")
        logging.info(df.shape)

        return 

@profile_lighter
def read_sql_tmpfile_arrow(query):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
    #with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)        
        conn.close()
        tmpfile.seek(0)
        #df = csv.read_csv(tmpfile.name).to_pandas()
        t = time.perf_counter()
        df = csv.read_csv(tmpfile.name)
        #df = pd.read_csv(tmpfile,engine="pyarrow")
        logging.info(df.shape)
        elapsed = time.perf_counter() - t
        logging.info(f"""{elapsed:0.4}""")
        logging.info(df.shape)

        return 


import dask.dataframe

@profile_lighter
def read_sql_tmpfile_dask(query):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
    #with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)        
        conn.close()
        t = time.perf_counter()
        tmpfile.seek(0)
        df = dask.dataframe.read_csv(tmpfile.name)
        #df = pd.read_csv(tmpfile,engine="pyarrow")
        
        elapsed = time.perf_counter() - t
        logging.info(f"""{elapsed:0.4}""")
        logging.info(df.shape)
#        f.write(tmpfile.read())
#        f.close()
#        tmpfile.seek(0)
        #arr = np.genfromtxt(tmpfile.name, skip_header = 1,delimiter='\t',encoding='utf-8')
#        rows = csv.reader(tmpfile.name, delimiter=',')
#        for row in rows:
#            logging.info(row)      
#@profile_lighter
def read_sql_tmpfile_df_chunk(query):
    #f = open('tt.csv','wb')
    t2 = time.perf_counter()
    with tempfile.TemporaryFile() as tmpfile:
    #with tempfile.TemporaryFile() as tmpfile:    
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)        
        conn.close()
        tmpfile.seek(0)
        t = time.perf_counter()
        #tp = pd.read_csv(tmpfile,iterator=True, chunksize=500000)
        #df = pd.concat(tp,ignore_index=True)
        df = pd.read_csv(tmpfile)
        #df = pd.read_csv(tmpfile,engine="pyarrow")

        elapsed = time.perf_counter() - t
        elapsed2 = time.perf_counter() - t2
        logging.info(f"""{elapsed:0.4}""")
        logging.info(f"""{elapsed2:0.4}""")
        logging.info(df.shape)
        return


@profile_lighter
def read_sql_tmpfile(query):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT  WITH CSV {head}".format(
           query=query, head="header"
        )
        rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        conn = rawdata_engine.raw_connection()
        with conn.cursor(name='aha') as cur:
            cur.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            t = time.perf_counter()
            df = pd.read_csv(tmpfile)
            logging.info(f"""{elapsed:0.4}""")
            logging.info(df.shape)
        conn.close()

        return df



#import csv
@profile_lighter
def read_sql_tmpfile_csv(query):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
    #with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)        
        conn.close()
        with open(tmpfile.name) as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]
        #df = pd.read_csv(tmpfile,engine="pyarrow")
        logging.info(len(data))
        
#        f.write(tmpfile.read())
#        f.close()
#        tmpfile.seek(0)
        #arr = np.genfromtxt(tmpfile.name, skip_header = 1,delimiter='\t',encoding='utf-8')
#        rows = csv.reader(tmpfile.name, delimiter=',')
#        for row in rows:
#            logging.info(row)     
        return 

def try_sql10(from_dt='2019-01-01',to_dt='2019-01-31',target_dt='2021-05-20'):
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
    #data = read_sql_tmpfile_dask(sql)
    #data = read_sql_tmpfile_arrow(sql)
    #data = read_sql_tmpfile_arrow_to_pandas(sql)
    #data = read_sql_tmpfile_df_chunk(sql)
    read_sql_tmpfile(sql)
    #data = read_sql_tmpfile_csv(sql)
    #with open(f'{target_dt}.pkl','wb') as f:
    #    pickle.dump(data,f)



if __name__ == "__main__":
    #to_dt = '2019-03-31'
    to_dt = '2019-06-30'
    #to_dt = '2019-01-01'
    #to_dt = '2019-02-28'
    #try_sql2(to_dt=to_dt)
    #df = try_sql(to_dt=to_dt)
    #arr = try_sql3(to_dt=to_dt)
    #try_sql4(to_dt=to_dt)
    #try_sql5(to_dt=to_dt)
    #try_sql6(to_dt=to_dt)
    #try_sql7(to_dt=to_dt)
    #try_sql8(to_dt=to_dt)
    #try_sql9(to_dt=to_dt)
    #try_sql10(to_dt=to_dt,target_dt='2021-09-03')
    #try_sql10(to_dt=to_dt,target_dt='2021-06-30')
    #loop = asyncio.get_event_loop() 
    #loop.run_until_complete(try_sql11(to_dt=to_dt,target_dt='2021-12-01'))
    #loop.run_until_complete(try_sql11(to_dt=to_dt,target_dt='2020-12-01'))
#await try_sql10(to_dt=to_dt,target_dt='2021-12-01')
    try_sql10(to_dt=to_dt,target_dt='2020-12-01')
    #try_sql10(to_dt=to_dt,target_dt='2021-05-20')
    #try_sql10(to_dt=to_dt,target_dt='2021-12-01')
    #try_sql10(to_dt=to_dt,target_dt='2021-12-01')
    #try_sql10(to_dt=to_dt,target_dt='2021-06-30')
    