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


@profile
def read_sql_tmpfile_df_arrow(query="SELECT * from lab.t1"):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
        # with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        #rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        rawdata_engine = create_engine(f"""postgresql://{os.environ['DB_USER']}:{os.environ['DB_PWD']}@localhost:5432/rawdb""")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)
        conn.close()
        tmpfile.seek(0)
        t = time.perf_counter()
        # tp = pd.read_csv(tmpfile,iterator=True, chunksize=500000)
        # df = pd.concat(tp,ignore_index=True)
        df = pd.read_csv(tmpfile, engine='pyarrow')
        # df = pd.read_csv(tmpfile,engine="pyarrow")

        elapsed = time.perf_counter() - t
        logging.info(f"""{elapsed:0.4}""")
        logging.info(df.shape)
        return


@profile
def read_sql_tmpfile_df(query="SELECT * from lab.t1"):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
        # with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        #rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        rawdata_engine = create_engine(f"""postgresql://{os.environ['DB_USER']}:{os.environ['DB_PWD']}@localhost:5432/rawdb""")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)
        conn.close()
        tmpfile.seek(0)
        t = time.perf_counter()
        # tp = pd.read_csv(tmpfile,iterator=True, chunksize=500000)
        # df = pd.concat(tp,ignore_index=True)
        df = pd.read_csv(tmpfile)
        # df = pd.read_csv(tmpfile,engine="pyarrow")

        elapsed = time.perf_counter() - t
        logging.info(f"""{elapsed:0.4}""")
        logging.info(df.shape)
        return


import pyarrow.csv


@profile
def read_sql_tmpfile_df_arrow2(query="SELECT * from lab.t1"):
    #f = open('tt.csv','wb')
    with tempfile.NamedTemporaryFile() as tmpfile:
        # with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head} ".format(query=query, head="header")
        #rawdata_engine = create_engine(f"postgresql://{rawdata_dic['user']}:{rawdata_dic['password']}@{rawdata_dic['host']}/{rawdata_dic['database']}")
        rawdata_engine = create_engine(f"""postgresql://{os.environ['DB_USER']}:{os.environ['DB_PWD']}@localhost:5432/rawdb""")
        conn = rawdata_engine.raw_connection()
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, tmpfile)
        conn.close()
        tmpfile.seek(0)
        t = time.perf_counter()
        df = pyarrow.csv.read_csv(tmpfile.name).to_pandas()
        # tp = pd.read_csv(tmpfile,iterator=True, chunksize=500000)
        # df = pd.concat(tp,ignore_index=True)
        #df = pd.read_csv(tmpfile)
        # df = pd.read_csv(tmpfile,engine="pyarrow")

        elapsed = time.perf_counter() - t
        logging.info(f"""{elapsed:0.4}""")
        logging.info(df.shape)
        return


def check(db, schema='lab', table='t1'):
    if db == 'row':
        engine = get_raw_conn()
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


def clean_csv_value(value) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

# @profile


def copy_stringio(connection, beers) -> None:
    schema = 'lab'
    table = 't1'
    with connection.cursor() as cursor:
        csv_file_like_object = io.StringIO()
        for beer in beers:
            csv_file_like_object.write('|'.join(map(str, beer)) + '\n')
        csv_file_like_object.seek(0)
        cursor.execute(f"SET search_path TO {schema}")
        cursor.copy_from(csv_file_like_object, f'{table}', sep='|')
    connection.commit()


@profile
def insert_raws(num=10000):
    # if num > 100000:
    #    raise 'too much'
    arr = np.random.randint(40, size=(num, 8))
    df = pd.DataFrame(arr)
    logging.info(arr.shape)
    logging.info(df.shape)
    engine = get_raw_conn()
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE lab.t1")
    conn.commit()
    copy_stringio(conn, arr)
    conn.close()


def main():
    logging.info("HI")


@profile
def copy_from_raw_feature(sql="SELECT * from lab.t1", schema='lab', table='t1') -> None:
    raw_engine = get_raw_conn()
    feature_engine = get_features_conn()
    raw_conn = raw_engine.raw_connection()
    feature_conn = feature_engine.raw_connection()
    try:
        with tempfile.TemporaryFile() as tmp, raw_conn.cursor() as cur1, feature_conn.cursor() as cur2:
            cur2.execute("TRUNCATE TABLE lab.t1")
            feature_conn.commit()
            copy_sql = f"""COPY ({sql}) TO STDOUT WITH CSV HEADER"""
            cur1.copy_expert(copy_sql, tmp)
            tmp.seek(0)
            cur2.copy_expert(f"""COPY {schema}.{table} FROM STDIN DELIMITER ',' CSV HEADER""", tmp)
            feature_conn.commit()
    finally:
        feature_conn.close()
        raw_conn.close()


@profile
def copy_from_raw_feature2(schema='lab', table='t1') -> None:
    raw_engine = get_raw_conn()
    feature_engine = get_features_conn()
    raw_conn = raw_engine.raw_connection()
    feature_conn = feature_engine.raw_connection()
    try:
        with raw_conn.cursor() as cur1, feature_conn.cursor() as cur2:
            cur2.execute("TRUNCATE TABLE lab.t1")
            feature_conn.commit()
            cur1.execute("SELECT * from lab.t1")
            r = cur1.fetchall()
            arr = np.array(r)
            csv_file_like_object = io.StringIO()
            for beer in arr:
                csv_file_like_object.write('|'.join(map(str, beer)) + '\n')
            csv_file_like_object.seek(0)
            cur2.execute(f"SET search_path TO {schema}")
            #cur2.copy_from(csv_file_like_object, f'{schema}.{table}', sep='|')
            cur2.copy_from(csv_file_like_object, f'{table}', sep='|')
            feature_conn.commit()
    finally:
        feature_conn.close()
        raw_conn.close()


memfile = MemoryTempfile()


@profile
def copy_from_raw_feature3(schema='lab', table='t1') -> None:
    raw_engine = get_raw_conn()
    feature_engine = get_features_conn()
    raw_conn = raw_engine.raw_connection()
    feature_conn = feature_engine.raw_connection()
    try:
        with memfile.TemporaryFile() as tmp, raw_conn.cursor() as cur1, feature_conn.cursor() as cur2:
            cur2.execute("TRUNCATE TABLE lab.t1")
            feature_conn.commit()
            sql = "SELECT * from lab.t1"
            copy_sql = f"""COPY ({sql}) TO STDOUT WITH CSV HEADER"""
            cur1.copy_expert(copy_sql, tmp)
            tmp.seek(0)
            cur2.copy_expert(f"""COPY {schema}.{table} FROM STDIN DELIMITER ',' CSV HEADER""", tmp)
            feature_conn.commit()
    finally:
        feature_conn.close()
        raw_conn.close()


@profile
def copy_from_raw_feature4(sql="SELECT * from lab.t1", schema='lab', table='t1') -> None:
    raw_engine = get_raw_conn()
    feature_engine = get_features_conn()
    raw_conn = raw_engine.raw_connection()
    feature_conn = feature_engine.raw_connection()
    try:
        with tempfile.TemporaryFile() as tmp, raw_conn.cursor() as cur1, feature_conn.cursor() as cur2:
            cur2.execute("TRUNCATE TABLE lab.t1")
            feature_conn.commit()
            copy_sql = f"""COPY ({sql}) TO STDOUT WITH BINARY"""
            cur1.copy_expert(copy_sql, tmp)
            tmp.seek(0)
            cur2.copy_expert(f"""COPY {schema}.{table} FROM STDIN (FORMAT BINARY)""", tmp)
            feature_conn.commit()
    finally:
        feature_conn.close()
        raw_conn.close()


@profile
def copy_from_raw_feature5(sql="SELECT * from lab.t1", schema='lab', table='t1') -> None:
    raw_engine = get_raw_conn()
    feature_engine = get_features_conn()
    raw_conn = raw_engine.raw_connection()
    feature_conn = feature_engine.raw_connection()
    try:
        with memfile.TemporaryFile() as tmp, raw_conn.cursor() as cur1, feature_conn.cursor() as cur2:
            cur2.execute("TRUNCATE TABLE lab.t1")
            feature_conn.commit()
            copy_sql = f"""COPY ({sql}) TO STDOUT WITH BINARY"""
            cur1.copy_expert(copy_sql, tmp)
            tmp.seek(0)
            cur2.copy_expert(f"""COPY {schema}.{table} FROM STDIN (FORMAT BINARY)""", tmp)
            feature_conn.commit()
    finally:
        feature_conn.close()
        raw_conn.close()


if __name__ == "__main__":
    '''
    insert_raws(10000000)
    check('row')
    copy_from_raw_feature()
    check('features')
    copy_from_raw_feature2()
    check('features')
    copy_from_raw_feature3()
    check('features')
    copy_from_raw_feature4()
    check('features')
    '''
    # copy_from_raw_feature5()
    check('features')
    read_sql_tmpfile_df_arrow()
    read_sql_tmpfile_df_arrow2()
    read_sql_tmpfile_df()
