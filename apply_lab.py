import numpy as np
import time
import logging
import pandas as pd
from functools import wraps
from memory_profiler import memory_usage
import pyarrow as pa

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

@profile
def gen_data(num=10):
    return np.random.randint(40,size=(10000000,8))

@profile
def convert_pandas(arr):
    return pd.DataFrame(arr)

@profile
def convert_from_pd_to_arrow(df):
    return pa.Table.from_pandas(df)


def divide(a, b):
    if b == 0:
        return 0.0
    return float(a)/b


@profile
def test1_np_vector(arr):
    v_func = np.vectorize(divide)
    t = v_func(arr,2)
    logging.info(f"{t[0]}")

@profile
def test2_df_apply(df_arr):
    #t = df_arr.apply(lambda x: divide(x,2),axis=1)
    t2 = df_arr.apply(lambda x: [divide(i,2) for i in x],axis=0)
    logging.info(f"{t2[0]}")



if __name__ == "__main__":
    #logging.info(f"numpy version:\t{np.__version__}")
    arr = gen_data(num=100000)
    #logging.info(f"arr shape:\t{arr.shape}")
    #logging.info(f"pandas version:\t{pd.__version__}")
    df_arr = convert_pandas(arr)
    #logging.info(f"{df_arr.memory_usage(deep=True)}")
    #logging.info(f"df_arr shape:\t{df_arr.shape}")
    #pd_arr = convert_from_pd_to_arrow(df_arr)
    test1_np_vector(arr)
    test2_df_apply(df_arr)