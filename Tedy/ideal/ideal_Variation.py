import sys
sys.path.append('/home/rpan/stock_alpha/')
from operators import *
import pandas as pd
import numpy as np
import os
from joblib import Parallel, delayed
from numpy_ext import rolling_apply
import warnings
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta
import multiprocessing as mp
mp.set_start_method('fork')
import uuid


result_path = '/mnt/Data/rpan/factor_pool/ideal_variation/'
result_path = '/home/tedy/mnt/data.30.4/factor_pool/ideal_variation'

if not os.path.exists(result_path):
    try:
        os.makedirs(result_path)
    except:
        pass

def _ideal_variation(mins,period,column_names,idx,n_jobs):
    barsize = 240//mins
    if mins == 240:
        path = 'daily'
    else:
        path = f'{mins}min'
    try:
        if_jy = pd.read_parquet(f'/data/stock_data/{path}/if_jy.parquet',columns=column_names)
        close = (pd.read_parquet(f'/data/stock_data/{path}/closeprice.parquet',columns=column_names)*if_jy).replace(0,np.nan)
        high = (pd.read_parquet(f'/data/stock_data/{path}/highprice.parquet',columns=column_names)*if_jy).replace(0,np.nan)
        low = (pd.read_parquet(f'/data/stock_data/{path}/lowprice.parquet',columns=column_names)*if_jy).replace(0,np.nan)
    except:
        close = pd.read_parquet(f'/data/stock_data/1min/closeprice.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').last().dropna(how='all')
        if_jy = pd.read_parquet(f'/data/stock_data/daily/if_jy.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').pad().reindex(close.index).fillna(method='ffill')
        close = (close*if_jy).replace(0,np.nan).dropna(how='all')
        high = (pd.read_parquet(f'/data/stock_data/1min/highprice.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').max().dropna(how='all')*if_jy).replace(0,np.nan).dropna(how='all')
        low = (pd.read_parquet(f'/data/stock_data/1min/lowprice.parquet',columns=column_names).resample(f'{mins}T',label='right',closed='right').min().dropna(how='all')*if_jy).replace(0,np.nan).dropna(how='all')

    variation = high/low - 1
    def avgV_high(variation,close):
        tmp = (close>np.nanquantile(close,0.75))
        return np.nansum(variation*tmp)/np.nansum(tmp)
    def avgV_low(variation,close):
        tmp = (close<np.nanquantile(close,0.25))
        return np.nansum(variation*tmp)/np.nansum(tmp)
    def rollingAvgV_diff(variation,close,period):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            V_high = pd.Series(rolling_apply(avgV_high,period,variation.values,close.values),index=variation.index,name=variation.name)
            V_low = pd.Series(rolling_apply(avgV_low,period,variation.values,close.values),index=variation.index,name=variation.name)
        return V_high - V_low
    V = pd.concat(Parallel(n_jobs=n_jobs,verbose=5,pre_dispatch='all')(
                delayed(rollingAvgV_diff)(variation.iloc[:,i],close.iloc[:,i],barsize*period) for i in range(len(close.columns))),axis=1).replace(0,np.nan)
    fn = f'{result_path}/ideal_var_{mins}minute_lookback{period}_{idx}.parquet'
    V.resample('D').last().dropna(how='all').to_parquet(fn)
    return fn


# no rolling_apply
def ideal_variation(mins, period, column_names, idx, n_jobs):
    barsize = 240 // mins
    if mins == 240:
        path = 'daily'
    else:
        path = f'{mins}min'
    try:
        if_jy = pd.read_parquet(f'/data/stock_data/{path}/if_jy.parquet', columns=column_names)
        close = (pd.read_parquet(f'/data/stock_data/{path}/closeprice.parquet',
                                 columns=column_names) * if_jy).replace(0, np.nan)
        high = (pd.read_parquet(f'/data/stock_data/{path}/highprice.parquet',
                                columns=column_names) * if_jy).replace(0, np.nan)
        low = (pd.read_parquet(f'/data/stock_data/{path}/lowprice.parquet', columns=column_names) * if_jy).replace(
            0, np.nan)
    except:
        close = pd.read_parquet(f'/data/stock_data/1min/closeprice.parquet', columns=column_names).resample(
            f'{mins}T', label='right', closed='right').last().dropna(how='all')
        if_jy = pd.read_parquet(f'/data/stock_data/daily/if_jy.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                        label='right',
                                                                                                        closed='right').pad().reindex(
            close.index).fillna(method='ffill')
        close = (close * if_jy).replace(0, np.nan).dropna(how='all')
        high = (pd.read_parquet(f'/data/stock_data/1min/highprice.parquet', columns=column_names).resample(
            f'{mins}T', label='right', closed='right').max().dropna(how='all') * if_jy).replace(0, np.nan).dropna(
            how='all')
        low = (pd.read_parquet(f'/data/stock_data/1min/lowprice.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                         label='right',
                                                                                                         closed='right').min().dropna(
            how='all') * if_jy).replace(0, np.nan).dropna(how='all')

    variation = high / low - 1

    def avgV_diff(variation,close):
        tmphigh = (close>np.nanquantile(close,0.75))
        tmplow = (close<np.nanquantile(close,0.25))
        return (np.nansum(variation*tmphigh)/np.nansum(tmphigh)) - (np.nansum(variation*tmplow)/np.nansum(tmplow))

    def rollingAvgV_diff(variation,close,period):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dates = pd.to_datetime(variation.index.to_series().dt.strftime('%Y-%m-%d').unique())
            V_diff = pd.Series(np.array([avgV_diff(variation.loc[(variation.index>=dates[i])&(variation.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                close.loc[(close.index>=dates[i])&(close.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                ) for i in range(len(dates)-period+1)])
                                ,index=dates[(period-1):],name=variation.name).reindex(dates)
        return V_diff


    V = pd.concat(Parallel(n_jobs=n_jobs, verbose=5, pre_dispatch='all')(
        delayed(rollingAvgV_diff)(variation.iloc[:, i], close.iloc[:, i], barsize * period) for i in
        range(len(close.columns))), axis=1).replace(0, np.nan)
    fn = f'{result_path}/ideal_var_{mins}minute_lookback{period}_{idx}.parquet'
    V.resample('D').last().dropna(how='all').to_parquet(fn)
    return fn


def work_process(mins, period, column_names, q):
    barsize = 240 // mins
    if mins == 240:
        path = 'daily'
    else:
        path = f'{mins}min'
    try:
        if_jy = pd.read_parquet(f'/data/stock_data/{path}/if_jy.parquet', columns=column_names)
        close = (pd.read_parquet(f'/data/stock_data/{path}/closeprice.parquet',
                                 columns=column_names) * if_jy).replace(0, np.nan)
        high = (pd.read_parquet(f'/data/stock_data/{path}/highprice.parquet',
                                columns=column_names) * if_jy).replace(0, np.nan)
        low = (pd.read_parquet(f'/data/stock_data/{path}/lowprice.parquet', columns=column_names) * if_jy).replace(
            0, np.nan)
    except:
        close = pd.read_parquet(f'/data/stock_data/1min/closeprice.parquet', columns=column_names).resample(
            f'{mins}T', label='right', closed='right').last().dropna(how='all')
        if_jy = pd.read_parquet(f'/data/stock_data/daily/if_jy.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                        label='right',
                                                                                                        closed='right').pad().reindex(
            close.index).fillna(method='ffill')
        close = (close * if_jy).replace(0, np.nan).dropna(how='all')
        high = (pd.read_parquet(f'/data/stock_data/1min/highprice.parquet', columns=column_names).resample(
            f'{mins}T', label='right', closed='right').max().dropna(how='all') * if_jy).replace(0, np.nan).dropna(
            how='all')
        low = (pd.read_parquet(f'/data/stock_data/1min/lowprice.parquet', columns=column_names).resample(f'{mins}T',
                                                                                                         label='right',
                                                                                                         closed='right').min().dropna(
            how='all') * if_jy).replace(0, np.nan).dropna(how='all')

    variation = high / low - 1

    def avgV_diff(variation,close):
        tmphigh = (close>np.nanquantile(close,0.75))
        tmplow = (close<np.nanquantile(close,0.25))
        return (np.nansum(variation*tmphigh)/np.nansum(tmphigh)) - (np.nansum(variation*tmplow)/np.nansum(tmplow))

    def rollingAvgV_diff(variation,close,period):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dates = pd.to_datetime(variation.index.to_series().dt.strftime('%Y-%m-%d').unique())
            V_diff = pd.Series(np.array([avgV_diff(variation.loc[(variation.index>=dates[i])&(variation.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                close.loc[(close.index>=dates[i])&(close.index<(dates[i+period-1]+relativedelta(days=+1)))].copy().values,
                                ) for i in range(len(dates)-period+1)])
                                ,index=dates[(period-1):],name=variation.name).reindex(dates)
        return V_diff

    for i in range(len(close.columns)):
        res = rollingAvgV_diff( variation.iloc[:, i], close.iloc[:, i], barsize * period)
        fn = f"/dev/shm/tedyworker/{uuid.uuid4().hex}.parquet"
        df = pd.DataFrame(res)
        df.to_parquet(fn)
        q.put(fn)

def ideal_variation_multiprocessing(mins, period, column_names, idx, n_jobs):
    print(f"worker start: [{(mins, period, n_jobs)}]")
    column_names = np.array_split(column_names, n_jobs)
    q = mp.Queue()
    plist = []
    for cols in column_names:
        p = mp.Process(target=work_process, args=(mins, period, cols, q))
        plist.append(p)
        p.start()

    for n, p in enumerate(plist):
        # print('worker end join: --------------- ',n)
        p.join()
        # print(f"worker process end: {n}/len(plist)")
    datas = []
    while q.qsize():
        fn = q.get()
        df = pd.read_parquet(fn)
        datas.append(df)

    V = pd.concat(datas, axis=1).replace(0, np.nan)
    fn = f'{result_path}/ideal_var_{mins}minute_lookback{period}_{idx}.parquet'
    V.resample('D').last().dropna(how='all').to_parquet(fn)
    return fn


def project_finished():
    pass

def task_finished(task,result):
    pass



def data_div() -> list:
  """
  @return:
    [   ['00001.XHG , 00002.XHG ] ,
        ['00003.XHG , 00004.XHG ] ,
        ]

  """

  name = '../volume.parquet'
  pq_file = pq.ParquetFile(name)
  WORKERS = 10

  column_indices = range(len(pq_file.schema))
  # column_indices = range(100)   # 仅仅启用100列数据快速测试运行

  column_names = [pq_file.schema[i].name for i in column_indices]
  last = column_names[-1]
  if last =='time':
      column_names = column_names[:-1]
  column_names = np.array_split(column_names,WORKERS)
  result = []
  for n,cols in enumerate(column_names):
      result.append( [list(cols),n] )
  return  result



if __name__ == '__main__':
    mins_bin = [1,5,15,30,240]
    period_bin = [3,5,10,20,30]
    for mins in mins_bin:
        for period in period_bin:
            ideal_variation(mins,period,-1)