import time
from functools import wraps
from memory_profiler import memory_usage
from mlaas_tools.feature_tool import FeatureBase
import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        with open('log.txt','a+') as f:
            fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
            print(f'\n{fn.__name__}({fn_kwargs_str})')

            # Measure time
            t = time.perf_counter()
            retval = fn(*args, **kwargs)
            elapsed = time.perf_counter() - t
            print(f'Time   {elapsed:0.4}')

            # Measure memory
            mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

            print(f'Memory {max(mem) - min(mem)}')
            f.write(f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]\t{fn.__name__}\t{elapsed:0.4}\t{max(mem) - min(mem)}\n')
            return retval

    return inner


def construct_conn(conn):
    '''
    input:
        conn - 要傳進來的連線(PS:尚未建起來)

    output:
        conn - conn()

    specification:
        確保conn能被建立，若error則立即中止程式
    '''
    try:
        conn = conn()

    except Exception:
        raise

    else:
        return conn

def get_date(given_dt: str, months: int, day_type: str, out_str: bool, out_str_type: str = "%Y-%m-%d"):
    """
    This function returns the relative first/last/same day to the given date

    Args:
        given_dt (str): the given date
        months (int): the number of months relative to the given_dt
        day_type (str): 'first','last' or 'same'
        out_str (boolean): True returns date to string, False returns datetime
        out_str_type (str): like "%Y-%m-%d", "%Y%m" etc.

    Returns:
        date in the type of datetime or string

    Example:
    >>> get_date('2020/02/29', 1, 'same', True, "%Y/%m/%d")
    '2020/03/29'
    """
    given_dt = parse(given_dt)
    num_month = months

    if day_type == "first":
        rel_dt = given_dt + relativedelta(months=num_month) + relativedelta(day=1)
    elif day_type == "last":
        rel_dt = given_dt + relativedelta(months=num_month) + relativedelta(day=31)
    else:
        rel_dt = given_dt + relativedelta(months=num_month)

    if out_str:
        rel_dt = rel_dt.strftime(out_str_type)

    return rel_dt


class TrainETL(FeatureBase):
    def __init__(self, etl_dt=None):
        if etl_dt is None:
            etl_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        super().__init__(etl_dt)        
        self.etl_dt = etl_dt
        self.rawdata_conn = self.dbset.get_rawdata_db_conn
        self.feature_conn = self.dbset.get_feature_db_conn

@profile
def query(db_link,sql):
    try:
        #conn = construct_conn(db_link)
        conn = db_link
        with conn.cursor() as cur:
            cur.execute(sql)            
            #selection_dt_list = cur.fetchall()
            selection_dt_list = [row for row in cur.fetchone()]
            return selection_dt_list
    except Exception as e:
        print(e)
    finally:
        conn.close()
