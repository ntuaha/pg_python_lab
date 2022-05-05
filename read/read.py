import logging
from psycopg2 import connect
import psycopg2
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
import pyarrow.csv

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


def get_raw_conn():
    engine = create_engine(f"""postgresql://{os.environ['DB_USER']}:{os.environ['DB_PWD']}@localhost:5432/rawdb""")
    return engine


def get_features_conn():
    engine = create_engine(f"""postgresql://{os.environ['DB_USER']}:{os.environ['DB_PWD']}@localhost:5432/features""")
    return engine

# @profile


def read_sql_tmpfile_df_arrow(query="SELECT * from lab.t1"):
    start_t = time.perf_counter()
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = get_raw_conn()
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)
        conn.close()
        tmpfile.seek(0)
        process_start_t = time.perf_counter()
        df = pd.read_csv(tmpfile, engine='pyarrow')
        end_t = time.perf_counter()
        logging.info("read_sql_tmpfile_df_arrow")
        logging.info(f"""process_t:\t{(end_t - process_start_t):0.4}\tall time:\t{(end_t - start_t):0.4}""")
        logging.info(df.shape)
        return df


def read_sql_tmpfile_arrow2df(query="SELECT * from lab.t1"):
    start_t = time.perf_counter()
    with tempfile.NamedTemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        rawdata_engine = get_raw_conn()
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)
        conn.close()
        tmpfile.seek(0)
        process_start_t = time.perf_counter()
        df = pyarrow.csv.read_csv(tmpfile.name).to_pandas()
        end_t = time.perf_counter()
        logging.info("read_sql_tmpfile_arrow2df")
        logging.info(f"""process_t:\t{(end_t - process_start_t):0.4}\tall time:\t{(end_t - start_t):0.4}""")
        logging.info(df.shape)
        return df


if __name__ == "__main__":
    read_sql_tmpfile_df_arrow()
    read_sql_tmpfile_arrow2df()
