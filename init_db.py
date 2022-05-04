import logging
import numpy as np
import time
from sqlalchemy import create_engine
import yaml
import tempfile
import io
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')

with open("gcp_db.yaml",'r') as stream:
    raw_db_dict = yaml.load(stream, Loader=yaml.CLoader)

def check(conn, schema='lab', table='t1'):
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}")
        cur.execute(f"select count(*) from {schema}.{table}")
        r = cur.fetchall()
        res = np.array(r)
        logging.info(f"total #: {res}")


def gen_data(num=1000000):
    t = time.perf_counter()
    arr = np.random.randint(40,size=(num,8))
    logging.info(f'生成時間:\t{(time.perf_counter()-t):0.4}')
    return arr


def gen_iostring_csv(arr):
    t = time.perf_counter()
    csv_file_like_object = io.StringIO()
    for beer in arr:
        csv_file_like_object.write(','.join(map(str, beer)) + '\n')
    csv_file_like_object.seek(0)
    logging.info(f'iostring生成時間:\t{(time.perf_counter()-t):0.4}')
    return csv_file_like_object

def init_db(conn,schema,table):
    arr = gen_data()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
    conn.commit()
    
    with conn.cursor() as cursor:
        obj = gen_iostring_csv(arr)
        t = time.perf_counter()
        cursor.execute(f"SET search_path TO {schema}")
        cursor.copy_from(obj, f'{table}', sep=',')
        logging.info(f'資料庫寫入時間:\t{(time.perf_counter()-t):0.4}')
    conn.commit()
        
    



if __name__ == "__main__":
    schema = 'lab'
    table = 't1'
    engine = create_engine(f"""postgresql://{raw_db_dict['rawdb']['user']}:{raw_db_dict['rawdb']['password']}@{raw_db_dict['rawdb']['host']}:{raw_db_dict['rawdb']['port']}/{raw_db_dict['rawdb']['db']}""")
    conn = engine.raw_connection()
    init_db(conn,schema,table)
    check(conn,schema,table)
    conn.close()