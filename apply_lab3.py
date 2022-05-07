import swifter
import perfplot
import pandas as pd
import dask.dataframe as dd
import warnings
import numpy as np
import logging
import time
import numba
import datetime

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


def main_task(a, b):
    if b == 0:
        return 0.0
    return float(a) / b


def list_comp(df):
    return [main_task(x, y) for x, y in zip(df['A'], df['B'])]


def apply(df):
    return df.apply(lambda row: main_task(row['A'], row['B']), axis=1)


def iterrows(df):
    result = []
    for index, row in df.iterrows():
        result.append(main_task(row['A'], row['B']))
    return result


def np_vec(df):
    f = np.vectorize(lambda x, y: main_task(x, y))
    k = f(df['A'].to_numpy(), df['B'].to_numpy())
    return k


def dask_apply(df):
    # find your own number of partitions
    ddf = dd.from_pandas(df, npartitions=30)
    ddf_update = ddf.apply(lambda row: main_task(row['A'], row['B']), axis=1).compute()
    return ddf_update


def dask_apply2(df):
    # find your own number of partitions
    ddf = dd.from_pandas(df, npartitions=2)
    ddf_update = ddf.apply(lambda row: main_task(row['A'], row['B']), axis=1).compute()
    return ddf_update


def swifter_apply(df):
    return df.swifter.apply(lambda row: main_task(row['A'], row['B']), axis=1)


@numba.vectorize
def numba_vec_core(x, y):
    if y == 0:
        return 0.0
    return float(x)/y


def numba_vec(df):
    return numba_vec_core(df['A'].to_numpy(), df['B'].to_numpy())

# 必須將運算過程用 python 原始運算子寫出，不能呼叫函數
@numba.jit(nogil=True)
def numba_jit_nogil_core(a, b):
    if b == 0:
        return 0.0
    return float(a) / b

def numba_jit_nogil(df):
    n = len(df)
    x = df['A'].to_numpy()
    y = df['B'].to_numpy()
    result = np.empty(n, dtype='float64')
    for i in range(n):
        result[i] = numba_jit_nogil_core(x[i], y[i])
    return result
    
# 必須將運算過程用 python 原始運算子寫出，不能呼叫函數
@numba.jit(parallel=True)
def numba_jit_parallel_core(a, b):
    if b == 0:
        return 0.0
    return float(a) / b


def numba_jit_parallel(df):
    n = len(df)
    x = df['A'].to_numpy()
    y = df['B'].to_numpy()
    result = np.empty(n, dtype='float64')
    for i in range(n):
        result[i] = numba_jit_parallel_core(x[i], y[i])
    return result


def main():
    logging.info('start')
    start_t = time.perf_counter()
    A = np.random.randint(40, size=(10,))
    B = np.random.randint(40, size=(10,))
    logging.info(f"gen_data:\t{(time.perf_counter()-start_t):0.4f}")
    df = pd.DataFrame({'A': A, 'B': B})

    kernels = [
           # vec,
   # vec_numpy,
    list_comp,
    apply,
    iterrows,
    np_vec,
#    dask_apply2,
#    dask_apply,
    numba_vec,
    numba_jit_nogil,
    numba_jit_parallel
#    swifter_apply]
    ]
    logging.info('start...')
    t= time.perf_counter()
    out= perfplot.bench(
        setup=lambda n: pd.concat([df] * n, ignore_index=True),
        kernels=kernels,
        labels=[str(k.__name__) for k in kernels],
        n_range=[10**k for k in range(6)],
        xlabel='N')
    logging.info(f"process time:\t{(time.perf_counter()-t):0.4f}")
    # out.show()
    out.save(f"fig/apply_lab3_{datetime.datetime.now().strftime('%Y%m%dH%H%M%Sz')}.png", transparent=True, bbox_inches="tight")

if __name__ == "__main__":
    main()
